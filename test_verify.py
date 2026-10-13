"""Comprehensive verification suite for HROne Employee Attendance & Analytics API."""
import calendar
import concurrent.futures
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP

current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

from fastapi.testclient import TestClient
from pymongo import MongoClient
import app.main as main_module
from app.main import app, db, client, IST, truncate_ms, epoch_ms_to_datetime, datetime_to_epoch_ms

test_client = TestClient(app)

def test_r1_r2_r3_r4_r5_boundaries():
    print("\n--- Testing Rules R1 to R5 Boundary Cases ---")
    
    # Setup test employee with normal shift 09:30 to 18:30
    emp_norm = {
        "emp_code": "EMP8001",
        "name": "Norm Tester",
        "email": "norm@example.com",
        "department": "Engineering",
        "shift_start": "09:30",
        "shift_end": "18:30",
        "joined_on": "2026-07-01"
    }
    db.employees.delete_one({"emp_code": "EMP8001"})
    db.attendance_logs.delete_many({"emp_code": "EMP8001"})
    res = test_client.post("/employees", json=emp_norm)
    assert res.status_code == 201

    # Setup overnight shift employee 22:00 to 06:00
    emp_over = {
        "emp_code": "EMP8002",
        "name": "Night Tester",
        "email": "night@example.com",
        "department": "Support",
        "shift_start": "22:00",
        "shift_end": "06:00",
        "joined_on": "2026-07-01"
    }
    db.employees.delete_one({"emp_code": "EMP8002"})
    db.attendance_logs.delete_many({"emp_code": "EMP8002"})
    res = test_client.post("/employees", json=emp_over)
    assert res.status_code == 201

    # R1 & R2: Punch in at 09:40:00.900 (should truncate to 09:40:00 -> exactly 10 min -> 0 late)
    # 2026-07-06 09:40:00.900 IST is 04:10:00.900 UTC
    dt_094000_900 = datetime(2026, 7, 6, 4, 10, 0, 900000, tzinfo=timezone.utc)
    ts_094000_900 = int(dt_094000_900.timestamp() * 1000)
    res = test_client.post("/attendance/punch-in", json={"emp_code": "EMP8001", "punched_at": ts_094000_900})
    assert res.status_code == 201, res.text
    doc = res.json()
    assert doc["date"] == "2026-07-06"
    assert doc["late_minutes"] == 0, f"Expected 0 late, got {doc['late_minutes']}"
    # Verify whole-second truncation in response
    dt_truncated = datetime(2026, 7, 6, 4, 10, 0, 0, tzinfo=timezone.utc)
    assert doc["punch_in"] == int(dt_truncated.timestamp() * 1000), "Punch-in not truncated to whole seconds"

    # Clean up and test strictly > 10 min: 09:40:01 IST -> 10 minutes late
    db.attendance_logs.delete_many({"emp_code": "EMP8001"})
    dt_094001 = datetime(2026, 7, 6, 4, 10, 1, tzinfo=timezone.utc)
    res = test_client.post("/attendance/punch-in", json={"emp_code": "EMP8001", "punched_at": int(dt_094001.timestamp() * 1000)})
    assert res.status_code == 201
    doc = res.json()
    assert doc["late_minutes"] == 10, f"09:40:01 should give 10 late minutes, got {doc['late_minutes']}"

    # Clean up and test 10:15:59 IST -> 45 minutes late
    db.attendance_logs.delete_many({"emp_code": "EMP8001"})
    dt_101559 = datetime(2026, 7, 6, 4, 45, 59, tzinfo=timezone.utc)
    res = test_client.post("/attendance/punch-in", json={"emp_code": "EMP8001", "punched_at": int(dt_101559.timestamp() * 1000)})
    assert res.status_code == 201
    doc = res.json()
    assert doc["late_minutes"] == 45, f"10:15:59 should give 45 late minutes, got {doc['late_minutes']}"

    # Test Overnight Shift R1:
    # Punch in at 05:55 IST on 2026-07-07 (earlier than shift_end 06:00) -> belongs to 2026-07-06!
    # 05:55 IST = 00:25 UTC on 2026-07-07
    dt_night = datetime(2026, 7, 7, 0, 25, 0, tzinfo=timezone.utc)
    res = test_client.post("/attendance/punch-in", json={"emp_code": "EMP8002", "punched_at": int(dt_night.timestamp() * 1000)})
    assert res.status_code == 201
    doc = res.json()
    assert doc["date"] == "2026-07-06", f"Overnight punch-in at 05:55 IST should belong to 2026-07-06, got {doc['date']}"
    print("[PASS] R1 & R2 boundaries (grace period, truncation, overnight shift date) verified")

    # R3, R4, R5 Punch Out:
    # Punch out for EMP8001 (punched in at 10:15:59 IST = 04:45:59 UTC on 2026-07-06)
    # Test Overtime: shift end is 18:30 IST.
    # Case A: Punch out at 18:59 IST (29 min overtime -> should give 0 overtime)
    # 18:59 IST = 13:29 UTC
    dt_1859 = datetime(2026, 7, 6, 13, 29, 0, tzinfo=timezone.utc)
    res = test_client.post("/attendance/punch-out", json={"emp_code": "EMP8001", "punched_at": int(dt_1859.timestamp() * 1000)})
    assert res.status_code == 200, res.text
    doc = res.json()
    assert doc["overtime_minutes"] == 0, f"29 min overtime must be 0, got {doc['overtime_minutes']}"
    # Work hours: from 04:45:59 to 13:29:00 = 8h 43m 1s = 31381 seconds = 8.71694... -> half-up 8.72
    expected_work_hours = float((Decimal("31381") / Decimal("3600")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
    assert doc["work_hours"] == expected_work_hours, f"Expected {expected_work_hours}, got {doc['work_hours']}"
    assert doc["half_day"] is False
    print("[PASS] R3 (<30 min overtime = 0) and R4 work hours verified")

    # Clean up
    db.employees.delete_many({"emp_code": {"$in": ["EMP8001", "EMP8002"]}})
    db.attendance_logs.delete_many({"emp_code": {"$in": ["EMP8001", "EMP8002"]}})

def test_legacy_record_regularization():
    print("\n--- Testing Legacy Record Regularization ---")
    emp = {
        "emp_code": "EMP8003",
        "name": "Legacy Tester",
        "email": "legacy@example.com",
        "department": "HR",
        "shift_start": "09:30",
        "shift_end": "18:30",
        "joined_on": "2026-07-01"
    }
    db.employees.delete_one({"emp_code": "EMP8003"})
    db.attendance_logs.delete_many({"emp_code": "EMP8003"})
    test_client.post("/employees", json=emp)

    # Insert legacy record directly into DB (missing history, missing half_day)
    dt_in = datetime(2026, 7, 14, 4, 0, 0, tzinfo=timezone.utc)
    dt_out = datetime(2026, 7, 14, 13, 0, 0, tzinfo=timezone.utc)
    legacy_doc = {
        "emp_code": "EMP8003",
        "date": "2026-07-14",
        "status": "PRESENT",
        "punch_in": dt_in,
        "punch_out": dt_out,
        "work_hours": 9.0,
        "late_minutes": 0,
        "overtime_minutes": 0
        # NO history, NO half_day
    }
    db.attendance_logs.insert_one(legacy_doc)

    # Now attempt to regularize it
    patch_payload = {
        "reason": "Correcting glitch on legacy record",
        "regularized_by": "admin",
        "status": "WFH"
    }
    res = test_client.patch("/attendance/EMP8003/2026-07-14", json=patch_payload)
    print(f"Legacy regularize status: {res.status_code}, body: {res.text}")
    assert res.status_code == 200, f"Regularizing legacy record failed: {res.status_code} {res.text}"
    updated = res.json()
    assert updated["status"] == "WFH"
    assert len(updated["history"]) == 1
    assert "status" in updated["history"][0]["changes"]
    print("[PASS] Legacy record successfully regularized without failure")

    db.employees.delete_one({"emp_code": "EMP8003"})
    db.attendance_logs.delete_many({"emp_code": "EMP8003"})

def test_department_trend_zero_logs():
    print("\n--- Testing Department Trend with Zero Attendance Logs ---")
    emp = {
        "emp_code": "EMP8004",
        "name": "Trend Tester",
        "email": "trend@example.com",
        "department": "EmptyDept",
        "shift_start": "09:30",
        "shift_end": "18:30",
        "joined_on": "2026-07-01"
    }
    db.employees.delete_one({"emp_code": "EMP8004"})
    db.attendance_logs.delete_many({"emp_code": "EMP8004"})
    test_client.post("/employees", json=emp)

    # EmptyDept has 1 employee, but 0 attendance records for range 2026-07-06 to 2026-07-10 (5 days)
    res = test_client.get("/analytics/departments/EmptyDept/trend?from=2026-07-06&to=2026-07-10")
    assert res.status_code == 200, res.text
    data = res.json()
    assert data["department"] == "EmptyDept"
    assert len(data["items"]) == 5, f"Expected 5 items, got {len(data['items'])}"
    
    # Check that Monday 2026-07-06 is working day, headcount 1, present_count 0, attendance_rate 0.0
    item0 = data["items"][0]
    assert item0["date"] == "2026-07-06"
    assert item0["is_working_day"] is True
    assert item0["headcount"] == 1
    assert item0["present_count"] == 0.0
    assert item0["late_count"] == 0
    assert item0["attendance_rate"] == 0.0
    assert item0["moving_avg_7d"] == 0.0
    print("[PASS] Department trend with zero logs returned correct 5 days")

    db.employees.delete_one({"emp_code": "EMP8004"})

if __name__ == "__main__":
    test_r1_r2_r3_r4_r5_boundaries()
    test_legacy_record_regularization()
    test_department_trend_zero_logs()
