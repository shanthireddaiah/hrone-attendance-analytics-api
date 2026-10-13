import json
import sys
import os

from pymongo import MongoClient
import app.main as main_module
from app.main import (
    db,
    build_employee_monthly_pipeline,
    build_department_summary_pipeline,
    build_late_leaderboard_pipeline,
    build_department_trend_pipeline,
    ensure_indexes,
)

ensure_indexes()

def check_plan(name, exp):
    print(f"\n=== Checking Explain for: {name} ===")
    qp = None
    if "queryPlanner" in exp:
        qp = exp["queryPlanner"]
    elif "stages" in exp and len(exp["stages"]) > 0:
        s0 = exp["stages"][0]
        if "$cursor" in s0 and "queryPlanner" in s0["$cursor"]:
            qp = s0["$cursor"]["queryPlanner"]
    
    if not qp:
        print(f"Could not locate queryPlanner in top keys: {list(exp.keys())}")
        return
    
    winning_plan_str = json.dumps(qp.get("winningPlan", {}))
    has_ixscan = "IXSCAN" in winning_plan_str
    has_collscan = "COLLSCAN" in winning_plan_str
    print(f"  IXSCAN present: {has_ixscan}")
    print(f"  COLLSCAN present: {has_collscan}")
    
    # Extract indexName if available
    indexes_used = []
    def find_indexes(d):
        if isinstance(d, dict):
            if "indexName" in d and d["indexName"]:
                indexes_used.append(d["indexName"])
            for v in d.values():
                find_indexes(v)
        elif isinstance(d, list):
            for item in d:
                find_indexes(item)
    find_indexes(qp.get("winningPlan", {}))
    print(f"  Indexes used: {set(indexes_used)}")

# 1. attendance_list
cmd1 = {"find": "attendance_logs", "filter": {}, "sort": {"date": -1, "emp_code": 1}, "skip": 0, "limit": 20}
exp1 = db.command({"explain": cmd1, "verbosity": "executionStats"})
check_plan("attendance_list", exp1)

# 2. employee_monthly
p2 = build_employee_monthly_pipeline("EMP0001", "2026-07")
exp2 = db.command({"explain": {"aggregate": "attendance_logs", "pipeline": p2, "cursor": {}}, "verbosity": "executionStats"})
check_plan("employee_monthly", exp2)

# 3. department_summary
p3 = build_department_summary_pipeline("2026-07", "Engineering")
exp3 = db.command({"explain": {"aggregate": "employees", "pipeline": p3, "cursor": {}}, "verbosity": "executionStats"})
check_plan("department_summary (with dept)", exp3)

p3_all = build_department_summary_pipeline("2026-07", None)
exp3_all = db.command({"explain": {"aggregate": "employees", "pipeline": p3_all, "cursor": {}}, "verbosity": "executionStats"})
check_plan("department_summary (without dept)", exp3_all)

# 4. late_leaderboard
p4 = build_late_leaderboard_pipeline("2026-07", 10, "Engineering")
exp4 = db.command({"explain": {"aggregate": "attendance_logs", "pipeline": p4, "cursor": {}}, "verbosity": "executionStats"})
check_plan("late_leaderboard", exp4)

# 5. department_trend
p5 = build_department_trend_pipeline("Engineering", "2026-07-01", "2026-07-10")
exp5 = db.command({"explain": {"aggregate": "attendance_logs", "pipeline": p5, "cursor": {}}, "verbosity": "executionStats"})
check_plan("department_trend", exp5)
