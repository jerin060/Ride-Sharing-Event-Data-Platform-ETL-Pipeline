# Data Quality Assessment Report

**Project:** Ride-Sharing Event Data Platform & ETL Pipeline
**Dataset:** Pathao-style synthetic ride-sharing event logs
**Assessment Date:** July 2026
**Prepared By:** Data Engineering Team
**Dataset Scope Assessed:** Sample-scale generation run (3,000 customers / 1,200 drivers / 1,300 vehicles / 12,202 ride requests / ~964K total records across 14 files)

---

## TL;DR — Executive Summary

- **14 files, ~964,351 records** profiled across **6 data quality dimensions** (uniqueness, completeness, referential integrity, validity, consistency, timeliness).
- **0 referential integrity violations** — every foreign key across customers, drivers, vehicles, rides, payments, and promotions resolves correctly.
- **0 validity violations** — all coordinates, ratings, ages, fares, and GPS attributes fall within their expected domains.
- **100% consistency** — `driver_payout + platform_commission = fare` holds exactly across all 8,398 completed rides.
- **100% clean ride-lifecycle ordering** — every ride's 5-stage funnel (requested → assigned → accepted → arrived → started → completed) arrives in correct chronological order.
- **Only findings:** intentionally injected, production-realistic noise — **~1.5% duplicate events** (at-least-once delivery), **~4.6% missing GPS pings**, and **~1% out-of-order GPS timestamps** — all with concrete ETL handling recommendations below.
- **Overall verdict:** dataset is **ETL-ready**; no unexpected defects found. Full findings, issue log, and recommendations follow.

---

## 1. Purpose

This report documents the data quality assessment performed on the raw JSONL event files produced by `generate.py`, prior to ETL ingestion into the PostgreSQL warehouse. The goal is to establish a quantitative baseline of data quality across all source files, using industry-standard dimensions, so that:

- The ETL pipeline can be designed with the right validation, cleansing, and deduplication logic.
- Known/expected issues are distinguished from unexpected defects.
- Stakeholders have a documented, auditable record of source data reliability before it reaches the warehouse.

This assessment was run directly against the generated data files (not against the generator's design intent), so all figures below are measured, not assumed.

---

## 2. Methodology

Each of the 14 source files was profiled programmatically across six data quality dimensions:

| Dimension | What was measured |
|---|---|
| **Uniqueness** | Duplicate record rate per file, based on primary key repetition |
| **Completeness** | Null/missing rate on fields that are legitimately optional |
| **Referential Integrity** | Orphan foreign keys — records referencing a parent ID that doesn't exist |
| **Validity** | Values outside their defined domain (coordinate bounds, rating range, age range, negative fares) |
| **Consistency** | Cross-field arithmetic and logical agreement (e.g., `driver_payout + platform_commission == fare`) |
| **Timeliness** | Chronological ordering of lifecycle timestamps and GPS ping sequences |

All checks were run against the full generated dataset (no sampling), except where noted.

---

## 3. Summary Scorecard

| Dimension | Result | Status |
|---|---|---|
| Uniqueness (event streams) | 1.4%–1.7% duplicate rate | ⚠️ Expected — requires dedup in ETL |
| Completeness (ratings, tickets) | 8.0%–41.1% null on optional fields | ✅ Expected — must be nullable in schema |
| Referential Integrity | 0 orphan records across all checked relationships | ✅ Pass |
| Validity (coordinates, ranges) | 0 out-of-range values found | ✅ Pass |
| Consistency (fare math) | 0 mismatches out of 8,398 completed rides | ✅ Pass |
| Timeliness (ride lifecycle) | 0 out-of-order lifecycle events out of 8,274 rides | ✅ Pass |
| Timeliness (GPS pings) | 1.07% of individual pings out of order; 61.6% of rides have ≥1 affected ping | ⚠️ Expected — requires sort-before-load |
| GPS completeness | ~4.6% of expected pings missing | ⚠️ Expected — requires gap-tolerant analytics |

**Overall assessment: the dataset is production-ETL-ready.** All anomalies found are intentionally injected data-quality conditions that mirror real production event streams (duplicate delivery, dropped GPS pings, out-of-order arrival, nullable ratings) rather than generation defects. No unexpected integrity violations, orphan keys, or invalid-domain values were found anywhere in the dataset.

---

## 4. Detailed Findings by Dimension

### 4.1 Uniqueness — Duplicate Records

| File | Duplicate Records | Total Records | Duplicate Rate |
|---|---|---|---|
| ride_requested.json | 202 | 12,202 | 1.66% |
| driver_assigned.json | 193 | 11,139 | 1.73% |
| driver_accepted.json | 127 | 8,844 | 1.44% |
| payment.json | 124 | 8,398 | 1.48% |

**Finding:** All four event streams show a duplicate rate in the 1.4%–1.7% range. This is by design — the generator injects duplicate events at a target rate of 1.5% to simulate at-least-once delivery semantics typical of production event buses (Kafka, SQS, etc.), where the same event can be re-delivered after a consumer acknowledgment failure.

**ETL implication:** The ingestion layer must deduplicate on primary key (`event_id` / `payment_id`) before loading into fact tables, keeping the first-seen or last-seen record per key depending on business rule (recommend: keep first occurrence, since duplicates are exact copies in this dataset).

---

### 4.2 Completeness — Null / Missing Values

| Field | File | Nulls | Total | Null Rate |
|---|---|---|---|---|
| ride_rating_customer | ride_completed.json | 673 | 8,398 | 8.01% |
| ride_rating_driver | ride_completed.json | 686 | 8,398 | 8.17% |
| resolved_at | customer_support.json | 547 | 1,332 | 41.07% |
| online_since (while online_status=true) | drivers.json | 0 | 1,200 | 0.00% |

**Finding:** Ratings are legitimately optional (~8% of riders/drivers don't leave a rating after a trip, which is realistic). `resolved_at` is null for any ticket not yet in `Resolved` status (Pending/Escalated), which is expected given the resolution-status distribution (60% Resolved / 25% Pending / 15% Escalated). No unexpected nulls were found — `online_since` is always populated when `online_status` is true, confirming no inconsistent state.

**ETL implication:** These fields must be defined as `NULL`-able in the warehouse schema (`ride_rating_customer`, `ride_rating_driver`, `resolved_at`). Do not default-fill with 0 or a placeholder date — that would corrupt average-rating and resolution-time KPIs.

---

### 4.3 Referential Integrity — Orphan Records

| Relationship Checked | Orphans Found | Total Checked |
|---|---|---|
| drivers.vehicle_id → vehicles.vehicle_id | 0 | 1,200 |
| ride_completed.customer_id → customers.customer_id | 0 | 8,398 |
| ride_completed.driver_id → drivers.driver_id | 0 | 8,398 |
| payment.ride_id → ride_requested.ride_id | 0 | 8,398 |
| customer_support.ride_id → ride_requested.ride_id | 0 | 1,332 |
| customer_support.customer_id → customers.customer_id | 0 | 1,332 |
| ride_requested.metadata.promotion_id → promotion.promotion_id | 0 | 2,729 (promo attempts) |

**Finding:** Zero orphan records across every relationship checked. Referential integrity holds at 100% between all master data and event streams.

**ETL implication:** Foreign key constraints can be enforced strictly at the warehouse layer (`ON DELETE RESTRICT` / `NOT VALID` checks) without needing a "quarantine" table for unmatched keys — the source data doesn't require that safety net for this generation run. This should still be re-validated on every future generation run, since it's a property of this run, not a structural guarantee.

---

### 4.4 Validity — Domain / Range Conformance

| Check | Violations | Total Checked |
|---|---|---|
| Pickup/destination latitude within Dhaka bounds (23.6–24.0) | 0 | 12,202 |
| Pickup/destination longitude within Dhaka bounds (90.2–90.6) | 0 | 12,202 |
| Driver rating within 3.8–5.0 | 0 | 1,200 |
| Customer age within 18–60 | 0 | 3,000 |
| Negative fare values | 0 | 8,398 |
| GPS speed_kmh negative | 0 | 887,961 |
| GPS accuracy_m outside 0–30m | 0 | 887,961 |
| GPS heading outside 0–359° | 0 | 887,961 |

**Finding:** No validity violations found in any file. All geographic coordinates fall within Dhaka's bounding box, all ratings/ages/fares/GPS attributes are within their defined domains.

**ETL implication:** Standard range-check constraints (`CHECK` clauses) can be applied at load time as a defensive measure for future data drift, but no cleansing/correction logic is required for this dataset.

---

### 4.5 Consistency — Cross-Field Logic

| Check | Mismatches | Total Checked | Rate |
|---|---|---|---|
| `driver_payout + platform_commission == fare` (±0.05 tolerance) | 0 | 8,398 | 0.00% |

**Finding:** Fare, commission, and payout figures are internally consistent across all completed rides — the 80/20 driver/platform split holds exactly.

**ETL implication:** This consistency can be used as a built-in reconciliation check in the ETL pipeline (a "fare reconciliation" validation step) to catch any future upstream calculation errors before they reach `Fact_Payments`.

---

### 4.6 Timeliness — Event Ordering

**Ride lifecycle ordering** (requested → assigned → accepted → arrived → started → completed):

| Check | Result |
|---|---|
| Rides checked | 8,274 |
| Rides with out-of-order lifecycle timestamps | 0 (0.00%) |

**GPS ping ordering** (within each ride's ping sequence, as delivered):

| Check | Result |
|---|---|
| Total GPS pings | 887,961 |
| Rides with GPS data | 8,274 |
| Individual pings arriving out of chronological order | 9,499 (1.07% of all pings) |
| Rides affected by at least one out-of-order ping | 5,096 (61.59% of rides) |

**GPS completeness (missing pings):**

| Check | Result |
|---|---|
| Estimated expected pings (based on ride duration, ~15s interval) | ~931,131 |
| Actual pings recorded | 887,961 |
| Estimated missing-ping rate | ~4.64% |

**Finding:** Ride-lifecycle event ordering is perfectly clean — every ride's five-stage funnel arrives in correct chronological order with zero exceptions. GPS pings, by contrast, show the injected ~1–2% out-of-order rate and ~5% drop rate at the individual-ping level, consistent with real mobile GPS behavior under poor connectivity or backgrounded apps. Because a single out-of-order ping is enough to flag a ride, the 61.6% ride-level figure looks alarming at first glance but is not — it reflects that most 20–90 minute rides contain 80–350+ pings, so even a low per-ping disorder rate touches most rides at least once.

**ETL implication:** GPS data must be explicitly sorted by `timestamp` per `ride_id` before any sequential analysis (e.g., route reconstruction, speed-over-time charts). Route/distance calculations should tolerate gaps of up to ~5% missing pings without breaking (e.g., use interpolation or gap-aware distance summation rather than assuming fixed-interval data).

---

### 4.7 Business Logic Distribution Sanity Checks

| Metric | Result |
|---|---|
| Total ride requests | 12,202 |
| Completed rides | 8,398 (68.8%) |
| Cancelled/failed rides | 3,779 (31.0%) — remainder in-flight/edge cases |
| Cancellation by actor | System: 1,974 · Customer: 1,355 · Driver: 450 |
| Top cancellation reasons | No Driver Found (1,068) · Driver Taking Too Long (906) · Wrong Pickup (474) · Emergency (466) |
| Payment status distribution | Completed: 89.85% · Failed: 6.47% · Pending: 3.68% |

**Finding:** Outcome distributions align with the business rules the generator was designed around (peak-hour cancellation increases, ~90% payment success rate) and show no skew or anomaly that would distort downstream KPI baselines.

---

## 5. Issue Log

| ID | Severity | File(s) | Issue | Count / Rate | Recommendation |
|---|---|---|---|---|---|
| DQ-01 | Low | ride_requested, driver_assigned, driver_accepted, payment | Duplicate events present | 1.4%–1.7% per file | Deduplicate on primary key during staging load |
| DQ-02 | Low | ride_completed, customer_support | Nullable fields (ratings, resolved_at) | 8%–41% | Model as nullable columns; exclude nulls from averages, don't zero-fill |
| DQ-03 | Low | gps_logs | Out-of-order ping arrival | 1.07% of pings / 61.6% of rides affected | Sort by `(ride_id, timestamp)` before any sequential/route analysis |
| DQ-04 | Low | gps_logs | Missing/dropped pings vs. expected interval | ~4.6% | Use gap-tolerant interpolation for route/distance reconstruction; do not assume fixed 15s cadence |
| DQ-05 | Informational | payment | Failed/Pending payment statuses | 6.47% / 3.68% | Expected business condition, not a defect — track separately in `Fact_Payments` for reconciliation reporting |

No **Medium**, **High**, or **Critical** severity issues were identified in this assessment.

---

## 6. Recommendations for the ETL Layer

1. **Staging layer deduplication** — apply `ROW_NUMBER() OVER (PARTITION BY <primary_key> ORDER BY <ingestion_time>) = 1` (or equivalent Pandas `drop_duplicates`) on all event streams before loading into fact tables.
2. **Nullable schema design** — do not enforce `NOT NULL` on `ride_rating_customer`, `ride_rating_driver`, `resolved_at`, or `online_since`; these are legitimately optional and null-handling logic in BI tools should treat them as "not yet rated / not yet resolved," not zero.
2a. **Timestamp normalization** — parse all ISO-8601 `+06:00` timestamps into a single standardized `TIMESTAMP WITH TIME ZONE` (or normalize to UTC) column type to avoid ambiguity when joining across files generated at different offsets in the future.
3. **GPS pre-processing** — sort `gps_logs` by `(ride_id, timestamp)` at load time; do not rely on file order. Treat missing pings as expected sparsity, not a load failure.
4. **Referential integrity enforcement** — safe to apply strict foreign key constraints (`REFERENCES ... NOT VALID` then `VALIDATE CONSTRAINT`) given the 0% orphan rate observed; re-run this same orphan check after every future data load to confirm the guarantee still holds.
5. **Fare reconciliation check** — add an automated post-load assertion that `driver_payout + platform_commission = fare` for every row in `Fact_Rides` / `Fact_Payments`; this is currently 100% consistent and any future failure would indicate an upstream calculation bug, not expected noise.
6. **Data quality monitoring** — re-run this same profiling script after each new data generation run (or real ETL load) and track the scorecard over time; a sudden change in duplicate rate, orphan count, or validity violations would indicate a regression worth investigating.

---

## 7. Conclusion

This dataset passes all structural and integrity checks required for a production-style ETL and warehouse build. The only quality issues present are the ones intentionally engineered into the generator to mirror real-world event-stream imperfections — duplicate delivery, dropped/out-of-order GPS pings, and legitimately nullable fields. None of these represent defects in the data generation process; they represent exactly the kind of noise a production ride-sharing platform's data engineering team would need to handle, which is the point of this project.

**Recommendation: proceed to ETL pipeline development**, incorporating the five items in Section 6 as explicit pipeline requirements rather than one-off fixes.