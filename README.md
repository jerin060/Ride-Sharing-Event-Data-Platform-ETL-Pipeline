# Ride-Sharing-Event-Data-Platform-ETL-Pipeline

## Goal: 
- Build an analytics-ready data warehouse

## Data Source
Raw data generated using python faker, random, numpy, and uuid library. 
Generate the raw data using generator.py to recreate the dataset.

# Pathao Synthetic Dataset — Schema Overview (Actual Generated Sample)

| File | Purpose | Records Generated | Primary Key |
|---|---|---|---|
| customers.json | Customer master | 3,000 | customer_id |
| drivers.json | Driver master | 1,200 | driver_id |
| vehicles.json | Vehicle master | 1,300 | vehicle_id |
| ride_requested.json | Ride requests | 12,202 | event_id |
| driver_assigned.json | Assignment events | 11,139 | event_id |
| driver_accepted.json | Acceptance events | 8,844 | event_id |
| driver_arrived.json | Driver arrival events | 8,390 | event_id |
| ride_started.json | Ride started | 8,402 | event_id |
| ride_completed.json | Completed rides | 8,398 | event_id |
| ride_cancelled.json | Cancelled/failed rides | 3,779 | ride_id |
| payment.json | Payment events | 8,398 | payment_id |
| promotion.json | Promotion campaigns | 6 | promotion_id |
| gps_logs.json | GPS events | 887,961 | gps_id |
| customer_support.json | Support tickets | 1,332 | ticket_id |
| **Total** | | **~964,351** | |

# Pathao Synthetic Dataset — Column Schema (All JSON Files)

## Master Data

### customers.json

| Column | Type | Description |
|---|---|---|
| customer_id | string | Primary key (e.g. CUST0000001) |
| full_name | string | Customer's full name |
| gender | string | Male / Female |
| age | integer | Customer age (18–60) |
| signup_date | date | Date customer signed up |
| home_zone | string | Home Dhaka zone |
| preferred_payment_method | string | Cash / Card / Pathao Wallet |
| customer_segment | string | New / Regular / Frequent / Premium |
| device_type | string | Android / iOS |
| app_version | string | App version at last known use |

### drivers.json

| Column | Type | Description |
|---|---|---|
| driver_id | string | Primary key (e.g. DRV0000001) |
| full_name | string | Driver's full name |
| gender | string | Male / Female |
| rating | float | Driver rating (3.8–5.0) |
| experience_years | float | Years of driving experience |
| join_date | date | Date driver joined the platform |
| online_status | boolean | Whether driver is currently online |
| online_since | datetime (nullable) | Timestamp driver came online (null if offline) |
| vehicle_id | string | Foreign key → vehicles.json |

### vehicles.json

| Column | Type | Description |
|---|---|---|
| vehicle_id | string | Primary key (e.g. VEH0000001) |
| vehicle_type | string | Bike / Car / CNG |
| brand | string | Manufacturer (Yamaha, Toyota, Bajaj, etc.) |
| model | string | Vehicle model name |
| registration_number | string | Dhaka Metro-style registration plate |
| manufacturing_year | integer | Year vehicle was manufactured (2015–2025) |

### promotion.json

| Column | Type | Description |
|---|---|---|
| promotion_id | string | Primary key (e.g. PROMO0001) |
| coupon_code | string | Redeemable code (NEWUSER100, EID50, etc.) |
| campaign_name | string | Human-readable campaign name |
| discount_type | string | Flat / Percentage |
| discount_value | number | Discount amount or percentage |
| minimum_fare | number | Minimum fare required to redeem |
| maximum_discount | number | Cap on discount amount |
| campaign_start | date | Campaign start date |
| campaign_end | date | Campaign end date |
| funded_by | string | Pathao / Merchant Partner / Marketing Budget |
| eligible_payment_methods | array[string] | Payment methods the coupon applies to |
| maximum_usage | integer | Max number of redemptions allowed |

---

## Event Streams

### ride_requested.json

| Column | Type | Description |
|---|---|---|
| event_id | string | Primary key |
| ride_id | string | Foreign key → ride lifecycle |
| customer_id | string | Foreign key → customers.json |
| pickup | object | Nested: `zone`, `latitude`, `longitude` |
| destination | object | Nested: `zone`, `latitude`, `longitude` |
| requested_timestamp | datetime | When the ride was requested |
| estimated_distance_km | float | Estimated trip distance |
| estimated_duration_min | float | Estimated trip duration |
| estimated_fare | float | Estimated fare (incl. surge) |
| ride_type | string | Bike / Car / CNG |
| service_type | string | Standard / Premium / Pool |
| metadata | object | Nested: `app_version`, `device_type`, `platform`, `city`, `coupon_applied`, `coupon_code`, `promotion_id`, `discount_amount`, `promotion_success`, `coupon_redemption_status`, `surge_reasons` |

### driver_assigned.json

| Column | Type | Description |
|---|---|---|
| event_id | string | Primary key |
| ride_id | string | Foreign key → ride_requested.json |
| driver_id | string | Foreign key → drivers.json |
| assignment_algorithm | string | Matching algorithm used |
| assigned_timestamp | datetime | When driver was assigned |
| estimated_arrival_min | float | Estimated time for driver to reach pickup |
| distance_to_pickup_km | float | Driver's distance from pickup point |

### driver_accepted.json

| Column | Type | Description |
|---|---|---|
| event_id | string | Primary key |
| ride_id | string | Foreign key → ride_requested.json |
| driver_id | string | Foreign key → drivers.json |
| accepted_timestamp | datetime | When driver accepted the ride |
| driver_rating | float | Driver's rating at time of acceptance |
| acceptance_delay_seconds | integer | Seconds between assignment and acceptance |

### driver_arrived.json

| Column | Type | Description |
|---|---|---|
| event_id | string | Primary key |
| ride_id | string | Foreign key → ride_requested.json |
| arrival_timestamp | datetime | When driver arrived at pickup |
| waiting_time_seconds | integer | Customer wait time after arrival |

### ride_started.json

| Column | Type | Description |
|---|---|---|
| event_id | string | Primary key |
| ride_id | string | Foreign key → ride_requested.json |
| start_timestamp | datetime | When the ride started |
| OTP_verified | boolean | Whether OTP verification succeeded |

### ride_completed.json

| Column | Type | Description |
|---|---|---|
| event_id | string | Primary key |
| ride_id | string | Foreign key → ride_requested.json |
| driver_id | string | Foreign key → drivers.json |
| customer_id | string | Foreign key → customers.json |
| distance_km | float | Actual trip distance |
| duration_min | float | Actual trip duration |
| average_speed_kmh | float | Average speed during trip |
| fare | float | Final fare charged |
| waiting_charge | float | Extra charge for excess wait time |
| surge_multiplier | float | Surge multiplier applied (1.0–2.0) |
| discount | float | Discount amount applied |
| driver_payout | float | Amount paid to driver |
| platform_commission | float | Platform's commission amount |
| ride_rating_customer | float (nullable) | Rating given by customer |
| ride_rating_driver | float (nullable) | Rating given by driver |
| completed_timestamp | datetime | When the ride was completed |

### ride_cancelled.json

| Column | Type | Description |
|---|---|---|
| ride_id | string | Primary key — foreign key → ride_requested.json |
| cancelled_by | string | Customer / Driver / System |
| reason | string | Cancellation reason |
| timestamp | datetime | When the cancellation occurred |
| stage_reached | string | Last lifecycle stage reached before cancellation |

### payment.json

| Column | Type | Description |
|---|---|---|
| payment_id | string | Primary key |
| ride_id | string | Foreign key → ride_requested.json |
| payment_method | string | Cash / Card / Pathao Wallet |
| payment_status | string | Completed / Pending / Failed |
| transaction_time | datetime | When the transaction was processed |
| amount | float | Amount charged |
| discount | float | Discount applied |
| commission | float | Platform commission |
| driver_payout | float | Amount paid to driver (0 if payment failed) |

### gps_logs.json

| Column | Type | Description |
|---|---|---|
| ride_id | string | Foreign key → ride_requested.json |
| driver_id | string | Foreign key → drivers.json |
| timestamp | datetime | GPS ping timestamp |
| latitude | float | Latitude at ping time |
| longitude | float | Longitude at ping time |
| speed_kmh | float | Speed at ping time |
| heading | integer | Direction of travel (0–359°) |
| accuracy_m | float | GPS accuracy in meters |

*Note: no standalone `gps_id` field currently — records are uniquely identified by the composite `(ride_id, driver_id, timestamp)`.*

### customer_support.json

| Column | Type | Description |
|---|---|---|
| ticket_id | string | Primary key |
| ride_id | string | Foreign key → ride_requested.json |
| customer_id | string | Foreign key → customers.json |
| issue_category | string | Type of issue reported |
| priority | string | Low / Medium / High / Critical |
| resolution_status | string | Resolved / Pending / Escalated |
| created_at | datetime | When the ticket was created |
| resolved_at | datetime (nullable) | When the ticket was resolved (null if unresolved) |


# insight:
"I found that rides using promotional coupons had a cancellation rate of 14%, compared with 7% for rides without coupons. By analyzing cancellation reasons and payment methods, I observed that many cancellations occurred immediately after fare confirmation, suggesting user confusion about coupon redemption. At the same time, customers who completed their first coupon-assisted ride averaged 2.8× more rides over the following 60 days. I recommended redesigning the payment confirmation screen and adding a brief coupon explanation to reduce unnecessary cancellations while preserving the long-term retention benefits of promotions."
