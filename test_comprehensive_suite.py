"""
Comprehensive Verification and Regression Suite for HROne Employee Attendance & Analytics API
Tests all endpoints, R1-R10 business rules, concurrency, boundary conditions, and MongoDB explain plans.
"""
import calendar
import concurrent.futures
import json
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP

# Set path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fastapi.testclient import TestClient
import app.main as main_module
from app.main import (
    app,
    db,
    client,
    IST,
    truncate_ms,
    epoch_ms_to_datetime,
    datetime_to_epoch_ms,
    round_half_up_2,
    round_half_up_4,
    ensure_indexes,
)

test_client = TestClient(app)

passed_checks = []
failed_checks = []

def record_result(name: str, passed: bool, msg: str = ""):
    if passed:
        passed_checks.append(name)
        print(f"  [PASS] {name}")
    else:
        failed_checks.append((name, msg))
        print(f"  [FAIL] {name}: {msg}")

def run_all_tests():
    print("=" * 70)
    print("STARTING HROne API COMPREHENSIVE VERIFICATION")
    print("=" * 70)

    ensure_indexes()

    # 1. Health endpoint
    print("\n1. Testing GET /health ...")
    res = test_client.get("/health")
    record_result("health_200", res.status_code == 200 and res.json() == {"status": "ok"}, res.text)

    # 2. Employees CRUD & Validation
    print("\n2. Testing /employees CRUD, Constraints & Concurrency ...")
    db.employees.delete_many({"emp_code": {"$regex": "^EMP9"}})
    db.attendance_logs.delete_many({"emp_code": {"$regex": "^EMP9"}})

    # Valid create
    emp1 = {
        "emp_code": "EMP9001",
        "name": "Alice Smith",
        "email": "alice@company.com",
        "department": "Engineering",
        "shift_start": "09:30",
        "shift_end": "18:30",
        "joined_on": "2026-07-01",
    }
    res = test_client.post("/employees", json=emp1)
    record_result("employee_create_201", res.status_code == 201, res.text)
    data = res.json()
    record_result("employee_no_id", "_id" not in data and "id" not in data, f"Exposed _id/id: {data}")
    record_result("employee_created_at_epoch_ms", isinstance(data.get("created_at"), int) and data["created_at"] > 100000000000)

    # Shift start == Shift end rejected with 422
    emp_bad_shift = dict(emp1, emp_code="EMP9002", email="bad@co.com", shift_end="09:30")
    res = test_client.post("/employees", json=emp_bad_shift)
    record_result("employee_shift_start_eq_end_422", res.status_code == 422, res.text)

    # Invalid emp_code format (e.g. EMP12) -> 422
    emp_bad_code = dict(emp1, emp_code="EMP12", email="bad2@co.com")
    res = test_client.post("/employees", json=emp_bad_code)
    record_result("employee_bad_code_422", res.status_code == 422, res.text)

    # Duplicate emp_code -> 409
    res = test_client.post("/employees", json=emp1)
    record_result("employee_duplicate_409", res.status_code == 409, res.text)

    # Concurrent employee creation: exactly one 201, others 409
    def create_emp_concurrent(code):
        c = TestClient(app)
        return c.post("/employees", json=dict(emp1, emp_code=code, email=f"{code}@co.com")).status_code

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(create_emp_concurrent, "EMP9003") for _ in range(5)]
        codes = [f.result() for f in futures]
    record_result("concurrent_employee_create_isolation", codes.count(201) == 1 and codes.count(409) == 4, f"Status codes: {codes}")

    # List employees with pagination and department filter
    # Create extra employees in Engineering and Sales
    test_client.post("/employees", json=dict(emp1, emp_code="EMP9004", department="Sales", email="sales1@co.com"))
    test_client.post("/employees", json=dict(emp1, emp_code="EMP9005", department="Engineering", email="eng2@co.com"))

    res = test_client.get("/employees?department=Engineering&page=1&page_size=10")
    record_result("list_employees_filtered", res.status_code == 200)
    data = res.json()
    eng_emp_codes = [e["emp_code"] for e in data["items"] if e["emp_code"].startswith("EMP9")]
    record_result("list_employees_sort_and_filter", eng_emp_codes == sorted(eng_emp_codes) and all(e["department"] == "Engineering" for e in data["items"]), str(data))

    # 3. Attendance Punch-In (R1, R2, Concurrency)
    print("\n3. Testing POST /attendance/punch-in & Business Rules R1 & R2 ...")
    # 404 for non-existent employee
    res = test_client.post("/attendance/punch-in", json={"emp_code": "EMP9999"})
    record_result("punch_in_unknown_emp_404", res.status_code == 404, res.text)

    # R1: Whole-second truncation (09:40:00.900 IST -> 09:40:00 IST -> late_minutes = 0)
    # 2026-07-06 09:40:00.900 IST is 2026-07-06 04:10:00.900 UTC
    dt_trunc = datetime(2026, 7, 6, 4, 10, 0, 900000, tzinfo=timezone.utc)
    res = test_client.post("/attendance/punch-in", json={"emp_code": "EMP9001", "punched_at": int(dt_trunc.timestamp() * 1000)})
    record_result("punch_in_201", res.status_code == 201, res.text)
    data = res.json()
    record_result("punch_in_date_ist", data["date"] == "2026-07-06", str(data))
    record_result("punch_in_r1_truncation", data["punch_in"] % 1000 == 0, str(data))
    record_result("punch_in_r2_grace_0_late", data["late_minutes"] == 0, str(data))
    record_result("punch_in_no_id", "_id" not in data and "id" not in data, str(data))

    # Duplicate punch-in for same employee and date -> 409
    res = test_client.post("/attendance/punch-in", json={"emp_code": "EMP9001", "punched_at": int(dt_trunc.timestamp() * 1000)})
    record_result("punch_in_duplicate_date_409", res.status_code == 409, res.text)

    # Concurrent punch-in for same employee and date: exactly one 201, others 409
    def punch_in_concurrent(ts):
        c = TestClient(app)
        return c.post("/attendance/punch-in", json={"emp_code": "EMP9005", "punched_at": ts}).status_code

    ts_conc = int(datetime(2026, 7, 6, 4, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(punch_in_concurrent, ts_conc) for _ in range(5)]
        statuses = [f.result() for f in futures]
    record_result("concurrent_punch_in_isolation", statuses.count(201) == 1 and statuses.count(409) == 4, f"Statuses: {statuses}")

    # Overnight shift employee setup

    # R1 Overnight shift employee: 22:00 to 06:00
    emp_night = {
        "emp_code": "EMP9006",
        "name": "Night Guard",
        "email": "night@co.com",
        "department": "Security",
        "shift_start": "22:00",
        "shift_end": "06:00",
        "joined_on": "2026-07-01",
    }
    test_client.post("/employees", json=emp_night)

    # Punch in at 05:45 IST on 2026-07-08 (< shift_end 06:00) -> belongs to 2026-07-07!
    # 05:45 IST is 00:15 UTC
    dt_night_in = datetime(2026, 7, 8, 0, 15, 0, tzinfo=timezone.utc)
    res = test_client.post("/attendance/punch-in", json={"emp_code": "EMP9006", "punched_at": int(dt_night_in.timestamp() * 1000)})
    data = res.json()
    record_result("overnight_punch_in_date_mapping", data["date"] == "2026-07-07", str(data))

    # 4. Attendance Punch-Out (R3, R4, R5, Concurrency)
    print("\n4. Testing POST /attendance/punch-out & Business Rules R3, R4, R5 ...")
    # EMP9001 has one record currently: 2026-07-06 (punched in at 09:40:00 IST = 04:10:00 UTC)
    # Punch out before punch in -> 422
    dt_before = datetime(2026, 7, 6, 4, 5, 0, tzinfo=timezone.utc)
    res = test_client.post("/attendance/punch-out", json={"emp_code": "EMP9001", "punched_at": int(dt_before.timestamp() * 1000)})
    record_result("punch_out_before_punch_in_422", res.status_code == 422, res.text)

    # Punch out > 24 hours after punch in -> 422 (e.g. 2026-07-07 05:00:00 UTC = 24h 50m later)
    dt_25h = datetime(2026, 7, 7, 5, 0, 0, tzinfo=timezone.utc)
    res = test_client.post("/attendance/punch-out", json={"emp_code": "EMP9001", "punched_at": int(dt_25h.timestamp() * 1000)})
    record_result("punch_out_gt_24h_422", res.status_code == 422, res.text)

    # Test concurrent punch-out on EMP9004 (2026-07-06): exactly one 200, others 409
    test_client.post("/attendance/punch-in", json={"emp_code": "EMP9004", "punched_at": int(datetime(2026, 7, 6, 4, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)})
    ts_out_conc = int(datetime(2026, 7, 6, 13, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)

    def punch_out_concurrent(ts):
        c = TestClient(app)
        return c.post("/attendance/punch-out", json={"emp_code": "EMP9004", "punched_at": ts}).status_code

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(punch_out_concurrent, ts_out_conc) for _ in range(5)]
        out_statuses = [f.result() for f in futures]
    record_result("concurrent_punch_out_isolation", out_statuses.count(200) == 1 and out_statuses.count(409) == 4, f"Out statuses: {out_statuses}")

    # R3 Overtime boundary on EMP9001 (2026-07-06):
    # EMP9001 punched in at 09:40:00 IST (04:10:00 UTC). Shift end is 18:30 IST (13:00 UTC).
    # Out at 18:59:59 IST (13:29:59 UTC) -> 29 minutes overtime -> R3: < 30 min is 0!
    # Work hours: 04:10:00 to 13:29:59 = 33599 seconds / 3600 = 9.333... -> 9.33. Half day: False.
    dt_185959 = datetime(2026, 7, 6, 13, 29, 59, tzinfo=timezone.utc)
    res = test_client.post("/attendance/punch-out", json={"emp_code": "EMP9001", "punched_at": int(dt_185959.timestamp() * 1000)})
    record_result("punch_out_200", res.status_code == 200, res.text)
    data = res.json()
    record_result("punch_out_r3_29min_zero_ot", data["overtime_minutes"] == 0, str(data))
    record_result("punch_out_r4_work_hours_933", data["work_hours"] == 9.33, str(data))
    record_result("punch_out_r5_half_day_false", data["half_day"] is False, str(data))

    # Sequential duplicate punch out -> 409
    res = test_client.post("/attendance/punch-out", json={"emp_code": "EMP9001", "punched_at": int(dt_185959.timestamp() * 1000)})
    record_result("punch_out_already_closed_409", res.status_code == 409, res.text)

    # Now punch in EMP9001 on July 7 to test R2 boundary & R5 Half-Day (< 4.50 hours):
    # R2 boundary: strictly > 10:00 (09:40:01 IST = 04:10:01 UTC -> 10 minutes late)
    dt_late10 = datetime(2026, 7, 7, 4, 10, 1, tzinfo=timezone.utc)
    res = test_client.post("/attendance/punch-in", json={"emp_code": "EMP9001", "punched_at": int(dt_late10.timestamp() * 1000)})
    data = res.json()
    record_result("punch_in_strictly_gt_10min_late", data["late_minutes"] == 10, str(data))

    # Punch out 4.0 hours later on July 7: 13:40:01 IST (08:10:01 UTC)
    # Duration: 4.0 hours (< 4.50) -> half_day = True!
    dt_half = datetime(2026, 7, 7, 8, 10, 1, tzinfo=timezone.utc)
    res = test_client.post("/attendance/punch-out", json={"emp_code": "EMP9001", "punched_at": int(dt_half.timestamp() * 1000)})
    data = res.json()
    record_result("punch_out_half_day_true", data["half_day"] is True and data["work_hours"] == 4.0, str(data))

    # Test Overnight Shift Punch-Out (across midnight) for EMP9006:
    # Punched in on 2026-07-08 05:45 IST (date: 2026-07-07, shift 22:00 to 06:00).
    # Punched out on 2026-07-08 06:45 IST (01:15 UTC): 45 minutes after shift_end 06:00 -> 45 min overtime!
    dt_night_out = datetime(2026, 7, 8, 1, 15, 0, tzinfo=timezone.utc)
    res = test_client.post("/attendance/punch-out", json={"emp_code": "EMP9006", "punched_at": int(dt_night_out.timestamp() * 1000)})
    data = res.json()
    record_result("overnight_punch_out_across_midnight", data["overtime_minutes"] == 45, str(data))

    # 5. List Attendance (/attendance)
    print("\n5. Testing GET /attendance Filtering & Pagination ...")
    # Date from > date to -> 422
    res = test_client.get("/attendance?date_from=2026-07-10&date_to=2026-07-01")
    record_result("list_attendance_inverted_dates_422", res.status_code == 422, res.text)

    res = test_client.get("/attendance?emp_code=EMP9001&page=1&page_size=20")
    record_result("list_attendance_200", res.status_code == 200, res.text)
    data = res.json()
    record_result("list_attendance_sorted_desc", [d["date"] for d in data["items"]] == sorted([d["date"] for d in data["items"]], reverse=True), str(data))

    # 6. Regularization (/attendance/{emp_code}/{date})
    print("\n6. Testing PATCH /attendance/{emp_code}/{date} Regularization & Audit Trail ...")
    # 404 for unknown record
    res = test_client.patch("/attendance/EMP9001/2026-01-01", json={"reason": "test correction", "regularized_by": "admin", "status": "WFH"})
    record_result("regularize_404_not_found", res.status_code == 404, res.text)

    # 422 if supplying punch_in with ABSENT
    res = test_client.patch("/attendance/EMP9001/2026-07-06", json={"reason": "mark absent", "regularized_by": "admin", "status": "ABSENT", "punch_in": 1783312500000})
    record_result("regularize_absent_with_punch_in_422", res.status_code == 422, res.text)

    # 422 if changing nothing
    res = test_client.patch("/attendance/EMP9001/2026-07-06", json={"reason": "no change here", "regularized_by": "admin", "status": "PRESENT"})
    record_result("regularize_no_changes_422", res.status_code == 422, res.text)

    # Valid change: Change status to WFH
    res = test_client.patch("/attendance/EMP9001/2026-07-06", json={"reason": "Worked from home approved", "regularized_by": "hr_manager", "status": "WFH"})
    record_result("regularize_status_wfh_200", res.status_code == 200, res.text)
    data = res.json()
    record_result("regularize_history_appended", len(data["history"]) == 1, str(data))
    h0 = data["history"][0]
    record_result("regularize_history_fields", h0["by"] == "hr_manager" and h0["reason"] == "Worked from home approved" and "status" in h0["changes"], str(h0))
    record_result("regularize_history_epoch_ms", isinstance(h0["at"], int) and h0["at"] > 100000000000, str(h0))

    # Concurrent regularization: exactly one succeeds, loser gets 409 (no lost history)
    def regularize_concurrent(st):
        c = TestClient(app)
        return c.patch("/attendance/EMP9001/2026-07-06", json={"reason": f"Update to {st}", "regularized_by": "admin", "status": st}).status_code

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(regularize_concurrent, s) for s in ["ON_DUTY", "PRESENT"]]
        reg_codes = [f.result() for f in futures]
    record_result("regularize_concurrency_safe", 200 in reg_codes and (len(reg_codes) == 1 or 409 in reg_codes), f"Reg codes: {reg_codes}")

    # 7. Analytics: Employee Monthly
    print("\n7. Testing GET /analytics/employees/{emp_code}/monthly ...")
    # Setup test employee joined mid-month: joined 2026-07-15 (Wednesday)
    # July 2026 has 23 working days total. From July 15 to July 31 there are 13 working days.
    emp_mid = {
        "emp_code": "EMP9010",
        "name": "Mid Month Joiner",
        "email": "mid@co.com",
        "department": "Engineering",
        "shift_start": "09:30",
        "shift_end": "18:30",
        "joined_on": "2026-07-15",
    }
    test_client.post("/employees", json=emp_mid)

    res = test_client.get("/analytics/employees/EMP9010/monthly?month=2026-07")
    record_result("monthly_200", res.status_code == 200, res.text)
    data = res.json()
    record_result("monthly_working_days_mid_month", data["working_days"] == 13, f"Expected 13, got {data['working_days']}")
    record_result("monthly_present_days_zero", data["present_days"] == 0.0, str(data))
    record_result("monthly_attendance_pct_zero", data["attendance_pct"] == 0.0, str(data))

    # Add a half day (0.5 present) and a weekend record (Saturday July 18)
    # Weekday record: July 16 (Thursday) half day
    dt_j16_in = datetime(2026, 7, 16, 4, 0, 0, tzinfo=timezone.utc)
    dt_j16_out = datetime(2026, 7, 16, 8, 0, 0, tzinfo=timezone.utc)
    db.attendance_logs.insert_one({
        "emp_code": "EMP9010",
        "date": "2026-07-16",
        "status": "PRESENT",
        "punch_in": dt_j16_in,
        "punch_out": dt_j16_out,
        "work_hours": 4.0,
        "late_minutes": 15,
        "overtime_minutes": 0,
        "half_day": True,
        "history": [],
    })
    # Weekend record: Saturday July 18 (R7: does not add to present_days, but adds to late/overtime)
    dt_j18_in = datetime(2026, 7, 18, 4, 0, 0, tzinfo=timezone.utc)
    dt_j18_out = datetime(2026, 7, 18, 13, 40, 0, tzinfo=timezone.utc)
    db.attendance_logs.insert_one({
        "emp_code": "EMP9010",
        "date": "2026-07-18",
        "status": "PRESENT",
        "punch_in": dt_j18_in,
        "punch_out": dt_j18_out,
        "work_hours": 9.0,
        "late_minutes": 20,
        "overtime_minutes": 40,
        "half_day": False,
        "history": [],
    })
    # Leave record: July 17
    db.attendance_logs.insert_one({
        "emp_code": "EMP9010",
        "date": "2026-07-17",
        "status": "LEAVE",
        "punch_in": None,
        "punch_out": None,
        "work_hours": None,
        "late_minutes": 0,
        "overtime_minutes": 0,
        "half_day": False,
        "history": [],
    })

    res = test_client.get("/analytics/employees/EMP9010/monthly?month=2026-07")
    data = res.json()
    # present_days must be 0.5 (half day on weekday; weekend record does NOT count)
    record_result("monthly_present_days_half_day_only_weekday", data["present_days"] == 0.5, str(data))
    record_result("monthly_leave_days", data["leave_days"] == 1, str(data))
    # total_late_minutes: 15 (weekday) + 20 (weekend) = 35
    record_result("monthly_weekend_late_minutes_counted", data["total_late_minutes"] == 35, str(data))
    record_result("monthly_weekend_overtime_counted", data["total_overtime_minutes"] == 40, str(data))
    # attendance_pct = 0.5 / 13 * 100 = 3.84615... -> half-up 3.85
    expected_pct = float((Decimal("0.5") / Decimal("13") * Decimal("100")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
    record_result("monthly_attendance_pct_rounding", data["attendance_pct"] == expected_pct, f"Expected {expected_pct}, got {data['attendance_pct']}")

    # Employee joined in future month (zero working days)
    emp_future = {
        "emp_code": "EMP9011",
        "name": "Future Joiner",
        "email": "future@co.com",
        "department": "Engineering",
        "shift_start": "09:30",
        "shift_end": "18:30",
        "joined_on": "2026-08-01",
    }
    test_client.post("/employees", json=emp_future)
    res = test_client.get("/analytics/employees/EMP9011/monthly?month=2026-07")
    data = res.json()
    record_result("monthly_zero_working_days_pct_null", data["working_days"] == 0 and data["attendance_pct"] is None, str(data))

    # 8. Analytics: Department Summary
    print("\n8. Testing GET /analytics/departments/summary ...")
    res = test_client.get("/analytics/departments/summary?month=2026-07")
    record_result("department_summary_200", res.status_code == 200, res.text)
    data = res.json()
    # Check that Engineering has headcount including those with no logs
    eng_summary = next((d for d in data["items"] if d["department"] == "Engineering"), None)
    record_result("dept_summary_eng_headcount", eng_summary is not None and eng_summary["headcount"] >= 3, str(eng_summary))
    # Department filter query param
    res_filtered = test_client.get("/analytics/departments/summary?month=2026-07&department=Sales")
    data_filtered = res_filtered.json()
    record_result("dept_summary_filtered", len(data_filtered["items"]) == 1 and data_filtered["items"][0]["department"] == "Sales", str(data_filtered))

    # 9. Analytics: Late Leaderboard
    print("\n9. Testing GET /analytics/leaderboard/late Ties & Ranking ...")
    # Setup 4 employees with late records:
    # EMP9021: 60 min late
    # EMP9022: 45 min late (tie)
    # EMP9023: 45 min late (tie)
    # EMP9024: 30 min late
    # Ranks should be: 1, 2, 2, 4 (standard competition ranking)
    db.employees.insert_many([
        {"emp_code": "EMP9021", "name": "Late A", "email": "la@co.com", "department": "TieDept", "shift_start": "09:30", "shift_end": "18:30", "joined_on": "2026-07-01"},
        {"emp_code": "EMP9022", "name": "Late B", "email": "lb@co.com", "department": "TieDept", "shift_start": "09:30", "shift_end": "18:30", "joined_on": "2026-07-01"},
        {"emp_code": "EMP9023", "name": "Late C", "email": "lc@co.com", "department": "TieDept", "shift_start": "09:30", "shift_end": "18:30", "joined_on": "2026-07-01"},
        {"emp_code": "EMP9024", "name": "Late D", "email": "ld@co.com", "department": "TieDept", "shift_start": "09:30", "shift_end": "18:30", "joined_on": "2026-07-01"},
    ])
    db.attendance_logs.insert_many([
        {"emp_code": "EMP9021", "date": "2026-07-01", "status": "PRESENT", "late_minutes": 60, "punch_in": None, "punch_out": None},
        {"emp_code": "EMP9022", "date": "2026-07-01", "status": "PRESENT", "late_minutes": 45, "punch_in": None, "punch_out": None},
        {"emp_code": "EMP9023", "date": "2026-07-01", "status": "PRESENT", "late_minutes": 45, "punch_in": None, "punch_out": None},
        {"emp_code": "EMP9024", "date": "2026-07-01", "status": "PRESENT", "late_minutes": 30, "punch_in": None, "punch_out": None},
    ])

    res = test_client.get("/analytics/leaderboard/late?month=2026-07&department=TieDept&limit=10")
    record_result("leaderboard_200", res.status_code == 200, res.text)
    lb_items = res.json()["items"]
    ranks = [it["rank"] for it in lb_items]
    record_result("leaderboard_competition_ranking_1_2_2_4", ranks == [1, 2, 2, 4], f"Got ranks: {ranks}")

    # Test cutoff behavior: with limit=2, since both EMP9022 and EMP9023 are rank 2 (rank <= 2), BOTH are returned (3 rows total)
    res_lim2 = test_client.get("/analytics/leaderboard/late?month=2026-07&department=TieDept&limit=2")
    lb_lim2 = res_lim2.json()["items"]
    record_result("leaderboard_tie_at_cutoff_returned", len(lb_lim2) == 3 and [it["rank"] for it in lb_lim2] == [1, 2, 2], f"Got: {lb_lim2}")

    # 10. Analytics: Department Trend
    print("\n10. Testing GET /analytics/departments/{department}/trend Gap Filling & Moving Avg ...")
    # Date range exceeding 92 days -> 422
    res_range_exceed = test_client.get("/analytics/departments/TieDept/trend?from=2026-01-01&to=2026-05-01")
    record_result("trend_exceeds_92_days_422", res_range_exceed.status_code == 422, res_range_exceed.text)

    # Inverted date range -> 422
    res_inv = test_client.get("/analytics/departments/TieDept/trend?from=2026-07-10&to=2026-07-01")
    record_result("trend_inverted_date_422", res_inv.status_code == 422, res_inv.text)

    # Unknown department -> 404
    res_unknown_dept = test_client.get("/analytics/departments/NonExistentDept/trend?from=2026-07-01&to=2026-07-05")
    record_result("trend_unknown_dept_404", res_unknown_dept.status_code == 404, res_unknown_dept.text)

    # Valid trend for 10 days: 2026-07-01 (Wed) to 2026-07-10 (Fri)
    res_trend = test_client.get("/analytics/departments/TieDept/trend?from=2026-07-01&to=2026-07-10")
    record_result("trend_valid_200", res_trend.status_code == 200, res_trend.text)
    trend_data = res_trend.json()
    record_result("trend_densify_all_10_days", len(trend_data["items"]) == 10, f"Got count {len(trend_data['items'])}")

    # Check weekend rows have attendance_rate = None
    weekend_items = [it for it in trend_data["items"] if it["date"] in ("2026-07-04", "2026-07-05")]
    record_result("trend_weekend_rate_null", all(it["is_working_day"] is False and it["attendance_rate"] is None for it in weekend_items), str(weekend_items))

    # 11. Admin Explain Endpoint
    print("\n11. Testing GET /admin/explain/{endpoint} for all endpoints ...")
    explain_endpoints = [
        ("attendance_list", "/admin/explain/attendance_list"),
        ("employee_monthly", "/admin/explain/employee_monthly?emp_code=EMP9001&month=2026-07"),
        ("department_summary", "/admin/explain/department_summary?month=2026-07&department=Engineering"),
        ("late_leaderboard", "/admin/explain/late_leaderboard?month=2026-07&limit=10"),
        ("department_trend", "/admin/explain/department_trend?department=TieDept&from=2026-07-01&to=2026-07-10"),
    ]

    for ep_name, ep_url in explain_endpoints:
        res = test_client.get(ep_url)
        record_result(f"explain_{ep_name}_200", res.status_code == 200, res.text)
        if res.status_code == 200:
            exp_data = res.json()["explain"]
            # Verify IXSCAN and no COLLSCAN
            exp_str = json.dumps(exp_data)
            has_ixscan = "IXSCAN" in exp_str
            has_collscan = "COLLSCAN" in exp_str
            record_result(f"explain_{ep_name}_ixscan", has_ixscan, "IXSCAN missing")
            record_result(f"explain_{ep_name}_no_collscan", not has_collscan, "COLLSCAN present")

    # Clean up test data
    db.employees.delete_many({"emp_code": {"$regex": "^EMP9"}})
    db.attendance_logs.delete_many({"emp_code": {"$regex": "^EMP9"}})

    print("\n" + "=" * 70)
    print(f"VERIFICATION COMPLETE: {len(passed_checks)} PASSED, {len(failed_checks)} FAILED")
    print("=" * 70)
    if failed_checks:
        print("\nFAILED CHECKS:")
        for name, msg in failed_checks:
            print(f"  - {name}: {msg}")
    return len(failed_checks) == 0

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
