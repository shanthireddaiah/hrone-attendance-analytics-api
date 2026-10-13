# Employee Attendance & Analytics API

FastAPI and MongoDB backend for employee attendance tracking, regularization audit trails, and aggregation analytics.

## Requirements
* Python 3.11+
* MongoDB 6.0+ (MongoDB 7.0 supported)

## Setup & Running

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure Environment**:
   Ensure `MONGO_URI` and `MONGO_DB` are available in your environment or defined in `.env`:
   ```bash
   export MONGO_URI="mongodb://localhost:27017"
   export MONGO_DB="attendance_db"
   ```

3. **Load Sample Data** (Optional):
   ```bash
   python sample_seed.py
   ```
   *Note: This replaces existing documents in `employees` and `attendance_logs` with sample data. Use a dedicated test database.*

4. **Run the API**:
   ```bash
   uvicorn app.main:app --port 8000
   ```
   The service will start on `http://localhost:8000`. Interactive OpenAPI documentation is accessible at `http://localhost:8000/docs`.

5. **Test the API**:
   ```bash
   # Run the comprehensive test suite
   python test_comprehensive_suite.py

   # Verify explain execution plans and index usage
   python test_explain_plans.py

   # Run scale verification (10,000 documents)
   python test_scale_explain.py
   ```

## Architecture & Implementation Notes

* **Single-File Structure**: All models, business rule helpers (R1–R10), endpoints, and startup index definitions reside in `app/main.py`.
* **Database Indexes**: Created idempotently on application startup via the FastAPI lifespan context manager (`emp_code`, `(emp_code, date)`, `(date, -1, emp_code, 1)`, etc.).
* **Concurrency Guarantees**:
  * Concurrent punch-ins for the same employee and date are resolved atomically by the unique compound index on `(emp_code, date)`, ensuring exactly one HTTP 201 and all others HTTP 409.
  * Concurrent punch-outs use atomic `find_one_and_update` with `{"punch_out": None}`, yielding exactly one HTTP 200 and all others HTTP 409.
  * Regularizations use atomic updates with audit history version tracking to prevent lost updates.
* **Analytics Pipelines**: Implemented purely using MongoDB aggregation pipelines (`$group`, `$lookup`, `$densify`, `$setWindowFields`) without Python loops over raw collection documents.
* **Precision Rounding**: Exact round-half-up (R4, R8) implemented using Python `Decimal` with `ROUND_HALF_UP` to prevent floating-point Banker's rounding discrepancies.
* **Explain Endpoint**: `GET /admin/explain/{endpoint}` executes raw `explain` commands with `executionStats` verbosity on the exact query or aggregation pipeline, verifying index scan (`IXSCAN`) coverage.

## Evaluation Files
* `REVIEW.md`: Detailed defect catalog from the starter code review with concrete failing cases and fixes.
* `DECISIONS.md`: Design rationale covering index strategy, concurrency handling, tie-breaking, zero-log headcount, and scalability.
