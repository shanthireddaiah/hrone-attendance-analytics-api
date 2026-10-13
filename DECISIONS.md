# DECISIONS.md (about 150-200 words in total - plain language, your own words)

1. **Indexes.** On `employees`, unique `{emp_code: 1}` guarantees business code uniqueness; compound `{department: 1, joined_on: 1}` and `{joined_on: 1}` accelerate department aggregations. On `attendance_logs`, unique `{emp_code: 1, date: 1}` enforces one record per employee-day; `{date: -1, emp_code: 1}` and `{status: 1, date: -1, emp_code: 1}` support paginated listing; `{emp_code: 1, punch_in: -1}` speeds up punch-out lookups; `{date: 1, late_minutes: -1}` and `{date: 1, status: 1}` support analytics. We considered and rejected an index on `work_hours` because analytics only aggregate it after filtering by date and status, never as an entry predicate.

2. **Punch-in race.** Both requests pass initial validation and attempt `insert_one`. The unique index on `(emp_code, date)` guarantees atomic isolation in WiredTiger: one insert commits (returning 201), while the concurrent insert fails with a `DuplicateKeyError`. We catch this exception and return 409 Conflict.

3. **Ties.** We rank employees using MongoDB's `$setWindowFields` with `$rank`, which implements standard competition ranking (1, 2, 2, 4). We apply `$match: {rank: {$lte: limit}}` after ranking. Thus, all employees tied at or below the cutoff rank are returned, even if the total row count exceeds `limit`.

4. **Headcount.** The aggregation pipeline starts from `employees`, filtering `joined_on <= month_end` and grouping by department to establish baseline headcount before executing a `$lookup` to `attendance_logs`. Employees without logs are preserved with zeroed metrics.

5. **One thing you would change** if this had to serve 100x the data: We would pre-aggregate daily attendance into monthly summary documents at punch-out/regularization time, and shard `attendance_logs` on `emp_code`.
