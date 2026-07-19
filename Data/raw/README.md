Raw data is not included because it exceeds GitHub's file size limit.

Run:

python- generator.py to recreate the dataset.

# Pathao-style Ride-Sharing Synthetic Event Dataset

This package contains a realistic, referentially-consistent, newline-delimited
JSON (JSONL) event log dataset simulating a ride-sharing platform's
microservice architecture, plus the generator script used to produce it.

## What's in here

| File                     | Description                                            |
|--------------------------|--------------------------------------------------------|
| customers.json           | Customer master data                                    |
| drivers.json             | Driver master data (each references a vehicle_id)       |
| vehicles.json            | Vehicle master data (Bike / Car / CNG)                  |
| promotion.json           | Promo campaign definitions (NEWUSER100, EID50, etc.)    |
| ride_requested.json      | Ride request events (pickup/destination, fare estimate, coupon attempt metadata) |
| driver_assigned.json     | Driver assignment events                                |
| driver_accepted.json     | Driver acceptance events                                |
| driver_arrived.json      | Driver arrival-at-pickup events                         |
| ride_started.json        | Ride start (OTP verification) events                    |
| ride_completed.json      | Ride completion events (fare, payout, commission, ratings) |
| ride_cancelled.json      | Cancellations (by Customer / Driver / System) with reason |
| payment.json             | Payment transactions (Cash / Card / Pathao Wallet)      |
| gps_logs.json            | GPS pings every 10-20s for every started ride           |
| customer_support.json    | Support tickets tied to problem rides                   |
| generate.py              | The full generator script (pure Python stdlib, no deps) |

## Scale of this dataset (sample, not full production spec)

- 3,000 customers
- 1,200 drivers / 1,300 vehicles
- 12,000 ride requests over Jan 1 - Mar 31, 2026 (Asia/Dhaka)
- ~8,270 completed rides, ~3,730 cancelled/failed rides
- ~888,000 GPS pings

This is roughly a 2-3% sample of the full spec (50,000 customers / 20,000
drivers / 500,000 rides). It was generated at this size so it's easy to
download, inspect, and build your ETL/warehouse/dashboard pipeline against
immediately. All business logic, distributions, and data-quality issues
(duplicates, missing GPS points, out-of-order timestamps, failed payments,
null optional fields) are already representative of full-scale production
behavior -- just at smaller volume.

## Generating the FULL production-scale dataset

Open `generate.py` and edit the CONFIG block near the top:

```python
N_CUSTOMERS = 50000
N_DRIVERS   = 20000
N_VEHICLES  = 21000
N_RIDES     = 500000
```

Then run:

```bash
python3 generate.py
```

No external dependencies are required (pure Python standard library).

**Resource note:** at full scale, `gps_logs.json` alone will be roughly
40x larger than in this sample (~7-8 GB), and total runtime will likely be
1-3+ hours depending on your hardware, since GPS log generation scales with
total ride-minutes simulated. Run it on a machine with several GB of free
disk and let it run in the background; progress is printed every 20,000
rides.

## Design notes / realism baked in

- **Business rules:** morning (7-10am) and evening (5-9pm) peak hours
  increase ride requests, wait times, and cancellation rates while
  lowering driver acceptance; Banani, Gulshan, Motijheel, Dhanmondi, and
  Airport get extra demand weighting during peaks.
- **Surge pricing:** 1.0x baseline, with 1.2x/1.5x during peak hours and
  1.5x-2.0x during simulated rain or special events.
- **Ride lifecycle paths:** Normal completion, Customer Cancelled,
  Driver Cancelled, No Driver Found, and Driver Timeout -- each emitting
  only the events that would actually fire for that path.
- **Promotions:** ~22% of rides attempt a coupon; outcomes include
  successful redemption, forgetting to apply it, misunderstanding terms,
  or falling below the minimum fare -- including some cancellations
  attributed to "Coupon Payment Confusion".
- **Data quality issues (intentional):** ~1.5% duplicate events per
  stream, ~5% dropped GPS pings, ~2% out-of-order GPS timestamps, ~4%
  null optional fields, and a realistic mix of Completed/Pending/Failed
  payments.
- **Referential integrity:** every driver references a real vehicle_id;
  every ride-lifecycle/payment/GPS/support event references real
  ride_id/driver_id/customer_id values (validated against master data).
