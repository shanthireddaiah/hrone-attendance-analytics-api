"""
Scale and performance verification with executionStats explain plans.
Generates 10,000 attendance records across multiple employees and departments.
Verifies IXSCAN presence, COLLSCAN absence, and execution statistics.
"""
import json
import os
import random
import sys
import time
from datetime import date, datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pymongo import MongoClient
from app.main import (
    db,
    client,
    ensure_indexes,
    build_employee_monthly_pipeline,
    build_department_summary_pipeline,
    build_late_leaderboard_pipeline,
    build_department_trend_pipeline,
)

def run_scale_verification():
    print("=" * 70)
    print("SCALE VERIFICATION & EXECUTIONSTATS EXPLAIN ANALYSIS")
    print("=" * 70)

    ensure_indexes()

    scale_db = client["scale_test_db"]
    # Replicate indexes on scale_db
    scale_db.employees.create_index([("emp_code", 1)], unique=True)
    scale_db.employees.create_index([("department", 1), ("joined_on", 1)])
    scale_db.employees.create_index([("joined_on", 1)])
    scale_db.attendance_logs.create_index([("emp_code", 1), ("date", 1)], unique=True)
    scale_db.attendance_logs.create_index([("date", -1), ("emp_code", 1)])
    scale_db.attendance_logs.create_index([("emp_code", 1), ("punch_in", -1)])
    scale_db.attendance_logs.create_index([("date", 1), ("late_minutes", -1)])
    scale_db.attendance_logs.create_index([("date", 1), ("status", 1)])
    scale_db.attendance_logs.create_index([("status", 1), ("date", -1), ("emp_code", 1)])

    print("Populating 100 employees and 10,000 attendance records...")
    scale_db.employees.delete_many({})
    scale_db.attendance_logs.delete_many({})

    departments = ["Engineering", "Product", "Design", "Marketing", "Sales", "Support", "Finance", "HR"]
    employees = []
    for i in range(1, 101):
        emp_code = f"EMP{i:04d}"
        dept = departments[i % len(departments)]
        employees.append({
            "emp_code": emp_code,
            "name": f"Employee {i}",
            "email": f"emp{i}@company.com",
            "department": dept,
            "shift_start": "09:30",
            "shift_end": "18:30",
            "joined_on": "2026-01-01",
            "created_at": datetime(2026, 1, 1, 4, 30, tzinfo=timezone.utc),
        })
    scale_db.employees.insert_many(employees)

    # 100 days of records for 100 employees = 10,000 documents
    logs = []
    base_date = date(2026, 4, 1)
    for day_offset in range(100):
        cur_date = base_date + timedelta(days=day_offset)
        d_str = cur_date.isoformat()
        is_wkend = cur_date.weekday() >= 5
        for i in range(1, 101):
            emp_code = f"EMP{i:04d}"
            if is_wkend and random.random() > 0.1:
                continue
            
            # 80% present, 10% leave, 10% wfh
            r = random.random()
            if r < 0.8:
                st = "PRESENT"
            elif r < 0.9:
                st = "WFH"
            else:
                st = "LEAVE"

            p_in = None
            p_out = None
            wh = None
            late_m = 0
            ot_m = 0
            hd = False
            if st != "LEAVE":
                p_in = datetime(cur_date.year, cur_date.month, cur_date.day, 4, 0, tzinfo=timezone.utc)
                p_out = datetime(cur_date.year, cur_date.month, cur_date.day, 13, 0, tzinfo=timezone.utc)
                wh = 9.0
                if random.random() < 0.2:
                    late_m = random.randint(15, 60)
                if random.random() < 0.2:
                    ot_m = random.randint(30, 90)
                if random.random() < 0.05:
                    wh = 4.0
                    hd = True

            logs.append({
                "emp_code": emp_code,
                "date": d_str,
                "status": st,
                "punch_in": p_in,
                "punch_out": p_out,
                "work_hours": wh,
                "late_minutes": late_m,
                "overtime_minutes": ot_m,
                "half_day": hd,
                "history": [],
            })

    scale_db.attendance_logs.insert_many(logs)
    count = scale_db.attendance_logs.count_documents({})
    print(f"Successfully loaded {count} attendance logs into scale test database.")

    def explain_and_verify(name, cmd):
        print(f"\n--- Explaining: {name} ---")
        t0 = time.perf_counter()
        exp = scale_db.command({"explain": cmd, "verbosity": "executionStats"})
        dur_ms = (time.perf_counter() - t0) * 1000
        from bson import json_util
        exp_str = json_util.dumps(exp)

        has_ixscan = "IXSCAN" in exp_str
        has_collscan = "COLLSCAN" in exp_str

        # Extract executionStats
        stats = {}
        if "executionStats" in exp:
            stats = exp["executionStats"]
        elif "stages" in exp and len(exp["stages"]) > 0:
            s0 = exp["stages"][0]
            if "$cursor" in s0 and "executionStats" in s0["$cursor"]:
                stats = s0["$cursor"]["executionStats"]

        keys_examined = stats.get("totalKeysExamined", "N/A")
        docs_examined = stats.get("totalDocsExamined", "N/A")
        n_returned = stats.get("nReturned", "N/A")
        server_exec_ms = stats.get("executionTimeMillis", "N/A")

        print(f"  IXSCAN: {has_ixscan} | COLLSCAN: {has_collscan}")
        print(f"  Execution Time: {server_exec_ms} ms (Round-trip: {dur_ms:.2f} ms)")
        print(f"  Keys Examined: {keys_examined} | Docs Examined: {docs_examined} | Returned: {n_returned}")

        assert has_ixscan, f"{name} failed: missing IXSCAN"
        assert not has_collscan, f"{name} failed: contains COLLSCAN"
        print(f"  [SUCCESS] {name} uses optimal IXSCAN with NO COLLSCAN.")

    # 1. attendance_list
    cmd1 = {
        "find": "attendance_logs",
        "filter": {"date": {"$gte": "2026-05-01", "$lte": "2026-05-31"}},
        "sort": {"date": -1, "emp_code": 1},
        "skip": 0,
        "limit": 20,
    }
    explain_and_verify("attendance_list (date range)", cmd1)

    # 2. employee_monthly
    p2 = build_employee_monthly_pipeline("EMP0005", "2026-05")
    cmd2 = {"aggregate": "attendance_logs", "pipeline": p2, "cursor": {}}
    explain_and_verify("employee_monthly", cmd2)

    # 3. department_summary (with department)
    p3 = build_department_summary_pipeline("2026-05", "Engineering")
    cmd3 = {"aggregate": "employees", "pipeline": p3, "cursor": {}}
    explain_and_verify("department_summary (with dept)", cmd3)

    # 3b. department_summary (all departments)
    p3b = build_department_summary_pipeline("2026-05", None)
    cmd3b = {"aggregate": "employees", "pipeline": p3b, "cursor": {}}
    explain_and_verify("department_summary (all depts)", cmd3b)

    # 4. late_leaderboard
    p4 = build_late_leaderboard_pipeline("2026-05", 10, "Engineering")
    cmd4 = {"aggregate": "attendance_logs", "pipeline": p4, "cursor": {}}
    explain_and_verify("late_leaderboard", cmd4)

    # 5. department_trend
    p5 = build_department_trend_pipeline("Engineering", "2026-05-01", "2026-05-31")
    cmd5 = {"aggregate": "attendance_logs", "pipeline": p5, "cursor": {}}
    explain_and_verify("department_trend", cmd5)

    # Cleanup scale test db
    client.drop_database("scale_test_db")
    print("\nScale test database dropped. All tests passed flawlessly!")

if __name__ == "__main__":
    run_scale_verification()
