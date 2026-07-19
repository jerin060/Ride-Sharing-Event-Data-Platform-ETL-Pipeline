#!/usr/bin/env python3
"""
Pathao-style Ride-Sharing Synthetic Event Dataset Generator
=============================================================

Generates realistic, referentially-consistent, newline-delimited JSON (JSONL)
event logs simulating a ride-sharing platform's microservice architecture:

  customers.json, drivers.json, vehicles.json,
  ride_requested.json, driver_assigned.json, driver_accepted.json,
  driver_arrived.json, ride_started.json, ride_completed.json,
  ride_cancelled.json, payment.json, promotion.json,
  gps_logs.json, customer_support.json

------------------------------------------------------------------
SCALING TO FULL PRODUCTION VOLUME
------------------------------------------------------------------
This script is fully parameterized via the CONFIG block below. The values
shipped by default are a representative SAMPLE (~2-3% of the full spec)
chosen so the whole pipeline runs in well under a minute and produces a
few hundred MB at most -- suitable for interactive use, uploading, and
building/testing an ETL pipeline right away.

To generate the FULL production-scale dataset described in the spec
(50,000 customers / 20,000 drivers / 20,000 vehicles / 500,000 rides /
tens of millions of GPS pings), edit CONFIG below to:

    N_CUSTOMERS = 50_000
    N_DRIVERS   = 20_000
    N_VEHICLES  = 21_000
    N_RIDES     = 500_000

and run this script in an environment with:
  - several GB of free disk (GPS logs alone will be 10-40 GB at that scale)
  - enough time (expect 1-3+ hours depending on hardware, since GPS log
    generation is O(rides * avg_gps_points_per_ride))
  - Python 3.9+ (no external dependencies -- pure standard library)

Everything else (business logic, distributions, referential integrity,
data-quality issue injection) scales automatically with these constants.
"""

import json
import math
import random
import os
from datetime import datetime, timedelta

# ============================================================
# CONFIG -- tune these to change dataset scale
# ============================================================
SEED = 42
random.seed(SEED)

OUTPUT_DIR = "D:\Ride-Sharing Event Data Platform & ETL Pipeline\Data\raw"

# --- SAMPLE SCALE (default). Set to full spec values to go to production scale. ---
N_CUSTOMERS = 3000
N_DRIVERS = 1200
N_VEHICLES = 1300
N_RIDES = 12000

# Full production spec (uncomment / edit CONFIG above to use):
# N_CUSTOMERS = 50000
# N_DRIVERS = 20000
# N_VEHICLES = 21000
# N_RIDES = 500000

START_DATE = datetime(2026, 1, 1, 0, 0, 0)
END_DATE = datetime(2026, 3, 31, 23, 59, 59)
TOTAL_SECONDS = int((END_DATE - START_DATE).total_seconds())

TZ_OFFSET = "+06:00"  # Asia/Dhaka, fixed offset, no DST

# Data quality injection rates
DUPLICATE_RATE = 0.015       # 1.5% duplicate events per stream
MISSING_GPS_RATE = 0.05      # 5% of expected GPS points missing
OUT_OF_ORDER_RATE = 0.02     # 2% of GPS points shifted out of order
NULL_OPTIONAL_RATE = 0.04    # 4% chance an optional field is null
LATE_ARRIVING_RATE = 0.02    # 2% of events get a small "ingestion lag" flag

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ============================================================
# REFERENCE / MASTER DATA
# ============================================================

ZONES = {
    "Banani":       (23.7937, 90.4066),
    "Gulshan":      (23.7925, 90.4078),
    "Mohakhali":    (23.7797, 90.4053),
    "Dhanmondi":    (23.7461, 90.3742),
    "Mirpur":       (23.8223, 90.3654),
    "Uttara":       (23.8759, 90.3795),
    "Bashundhara":  (23.8149, 90.4249),
    "Motijheel":    (23.7333, 90.4172),
    "Farmgate":     (23.7551, 90.3897),
    "Mohammadpur":  (23.7657, 90.3588),
    "Badda":        (23.7809, 90.4265),
    "Tejgaon":      (23.7683, 90.3931),
    "Old Dhaka":    (23.7104, 90.4074),
    "Airport":      (23.8433, 90.3978),
    "Rampura":      (23.7581, 90.4256),
    "Khilgaon":     (23.7443, 90.4257),
    "Shahbag":      (23.7383, 90.3958),
    "Panthapath":   (23.7508, 90.3831),
    "Kuril":        (23.8213, 90.4256),
    "Baridhara":    (23.7937, 90.4172),
}
ZONE_NAMES = list(ZONES.keys())
HIGH_DEMAND_ZONES = ["Banani", "Gulshan", "Motijheel", "Dhanmondi", "Airport"]

MALE_FIRST = ["Abdul", "Mohammad", "Rafiqul", "Kamal", "Jamal", "Shahin", "Rashed",
    "Nazrul", "Habibur", "Aminul", "Rezaul", "Shakil", "Tanvir", "Rakib", "Sohel",
    "Farid", "Anisur", "Mizanur", "Delwar", "Zahid", "Asif", "Imran", "Faisal",
    "Hasan", "Ashraf", "Mahbub", "Nazmul", "Shariful", "Golam", "Enamul", "Rubel",
    "Masud", "Shamim", "Jubayer", "Arif", "Sajib", "Emon", "Rony", "Sabbir", "Naim"]
FEMALE_FIRST = ["Fatima", "Ayesha", "Nusrat", "Farzana", "Sultana", "Rehana",
    "Shirin", "Nasrin", "Salma", "Rina", "Shahnaz", "Rubina", "Taslima", "Kohinoor",
    "Momtaz", "Shabnam", "Rowshan", "Jesmin", "Nargis", "Parveen", "Lima", "Mim",
    "Tania", "Sabrina", "Nishat", "Ismat", "Afroza", "Marium", "Kulsum", "Runa",
    "Sharmin", "Popy", "Moushumi", "Jannat", "Sumaiya", "Tahmina", "Ruma", "Liza"]
LAST_NAMES = ["Islam", "Rahman", "Hossain", "Ahmed", "Khan", "Chowdhury", "Akter",
    "Begum", "Uddin", "Alam", "Karim", "Hoque", "Sarkar", "Mia", "Talukder",
    "Bhuiyan", "Molla", "Sheikh", "Miah", "Haque"]

APP_VERSIONS = ["8.3.0", "8.4.1", "8.4.2", "8.5.0", "8.5.2", "8.6.0", "8.6.1"]
DEVICE_TYPES = ["Android"] * 75 + ["iOS"] * 25
CUSTOMER_SEGMENTS = ["New"] * 30 + ["Regular"] * 40 + ["Frequent"] * 20 + ["Premium"] * 10
PAYMENT_METHODS = ["Cash"] * 50 + ["Card"] * 25 + ["Pathao Wallet"] * 25

BIKE_BRANDS = {"Yamaha": ["FZS-V3", "R15 V4", "MT-15"], "Honda": ["CB Hornet", "CB150R", "CBR150R"],
    "Bajaj": ["Pulsar 150", "Pulsar NS160"], "TVS": ["Apache RTR 160", "Apache RTR 200"],
    "Suzuki": ["Gixxer", "Gixxer SF"]}
CAR_BRANDS = {"Toyota": ["Axio", "Premio", "Corolla"], "Honda": ["Vezel", "City", "Fit"],
    "Hyundai": ["Elantra", "Creta"], "Suzuki": ["Alto", "Wagon R"]}
CNG_BRANDS = {"Bajaj": ["RE Auto CNG"], "Piaggio": ["Ape Auto CNG"], "TVS": ["King CNG"]}

ASSIGNMENT_ALGORITHMS = ["nearest_driver_v3", "highest_rated_match", "least_busy_pool", "hybrid_ml_v2"]
SERVICE_TYPES = ["Standard"] * 65 + ["Premium"] * 15 + ["Pool"] * 20

PROMO_TEMPLATES = [
    ("NEWUSER100", "New User Welcome Offer", "Flat", 100, "Regular"),
    ("EID50", "Eid Celebration Discount", "Flat", 50, "Regular"),
    ("RAIN20", "Rainy Day Surge Relief", "Percentage", 20, "Regular"),
    ("FLASH30", "Flash Sale 30% Off", "Percentage", 30, "Flash"),
    ("WEEKEND40", "Weekend Special", "Percentage", 40, "Regular"),
    ("OFFICE25", "Office Commute Discount", "Percentage", 25, "Corporate"),
]
FUNDED_BY = ["Pathao", "Merchant Partner", "Marketing Budget"]

CANCEL_REASONS = ["Driver Taking Too Long", "Customer Changed Mind", "Wrong Pickup",
    "Traffic", "Coupon Payment Confusion", "No Driver Found", "Payment Issue", "Emergency"]

SUPPORT_ISSUES = ["Coupon not applied", "Driver behavior", "Wrong fare", "Late driver",
    "Payment deducted twice", "Wallet issue", "Ride cancelled automatically"]
SUPPORT_PRIORITIES = ["Low"] * 40 + ["Medium"] * 40 + ["High"] * 15 + ["Critical"] * 5
RESOLUTION_STATUSES = ["Resolved"] * 60 + ["Pending"] * 25 + ["Escalated"] * 15

# Demand weight per hour of day (index 0-23)
HOUR_WEIGHTS = [0.3, 0.2, 0.15, 0.15, 0.2, 0.4, 0.8, 2.6, 3.0, 2.6, 1.4, 1.2,
                1.3, 1.2, 1.1, 1.2, 1.5, 2.8, 3.2, 2.6, 1.6, 1.1, 0.7, 0.45]

PEAK_HOURS = set(list(range(7, 10)) + list(range(17, 21)))

# ============================================================
# HELPERS
# ============================================================

def iso(dt):
    """Format a naive datetime as an Asia/Dhaka ISO-8601 string."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + "." + f"{dt.microsecond // 1000:03d}" + TZ_OFFSET


def random_name():
    is_male = random.random() < 0.55
    first = random.choice(MALE_FIRST) if is_male else random.choice(FEMALE_FIRST)
    last = random.choice(LAST_NAMES)
    gender = "Male" if is_male else "Female"
    return f"{first} {last}", gender


def weighted_hour():
    return random.choices(range(24), weights=HOUR_WEIGHTS, k=1)[0]


def random_timestamp_in_range(start=START_DATE, end=END_DATE):
    day_span = (end.date() - start.date()).days
    day_offset = random.randint(0, max(day_span, 0))
    day = start + timedelta(days=day_offset)
    hour = weighted_hour()
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    micro = random.randint(0, 999) * 1000
    ts = day.replace(hour=hour, minute=minute, second=second, microsecond=micro)
    if ts < start:
        ts = start
    if ts > end:
        ts = end
    return ts


def is_peak(dt):
    return dt.hour in PEAK_HOURS


def weighted_zone(dt, exclude=None):
    weights = []
    names = []
    for z in ZONE_NAMES:
        if z == exclude:
            continue
        names.append(z)
        w = 2.5 if (z in HIGH_DEMAND_ZONES and is_peak(dt)) else 1.0
        weights.append(w)
    return random.choices(names, weights=weights, k=1)[0]


def jitter_coord(lat, lon, spread=0.006):
    return (round(lat + random.uniform(-spread, spread), 6),
            round(lon + random.uniform(-spread, spread), 6))


def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


def maybe_null(value, rate=NULL_OPTIONAL_RATE):
    return None if random.random() < rate else value


def new_id(prefix, n, width=7):
    return f"{prefix}{n:0{width}d}"


class JSONLWriter:
    """Streams dict records to a JSONL file, with light duplicate injection."""

    def __init__(self, path, dup_rate=DUPLICATE_RATE):
        self.f = open(path, "w", encoding="utf-8")
        self.dup_rate = dup_rate
        self.count = 0

    def write(self, record):
        line = json.dumps(record, ensure_ascii=False)
        self.f.write(line + "\n")
        self.count += 1
        if random.random() < self.dup_rate:
            self.f.write(line + "\n")
            self.count += 1

    def close(self):
        self.f.close()

# ============================================================
# MASTER DATA GENERATION
# ============================================================

def generate_vehicles():
    vehicles = []
    for i in range(1, N_VEHICLES + 1):
        vtype = random.choices(["Bike", "Car", "CNG"], weights=[55, 35, 10], k=1)[0]
        brands = {"Bike": BIKE_BRANDS, "Car": CAR_BRANDS, "CNG": CNG_BRANDS}[vtype]
        brand = random.choice(list(brands.keys()))
        model = random.choice(brands[brand])
        series_letter = random.choice(["GA", "HA", "JA", "KA", "LA", "MA"])
        reg = f"DHAKA METRO-{series_letter}-{random.randint(11,19)}-{random.randint(1000,9999)}"
        vehicles.append({
            "vehicle_id": new_id("VEH", i),
            "vehicle_type": vtype,
            "brand": brand,
            "model": model,
            "registration_number": reg,
            "manufacturing_year": random.randint(2015, 2025),
        })
    return vehicles


def generate_drivers(vehicles):
    drivers = []
    vehicle_pool = vehicles[:N_DRIVERS]  # each active driver gets one vehicle
    for i in range(1, N_DRIVERS + 1):
        name, gender = random_name()
        join_offset_days = random.randint(30, 1500)
        join_date = START_DATE - timedelta(days=join_offset_days)
        online_status = random.random() < 0.55
        online_since = None
        if online_status:
            online_since = iso(random_timestamp_in_range(
                max(START_DATE, END_DATE - timedelta(days=1)), END_DATE))
        drivers.append({
            "driver_id": new_id("DRV", i),
            "full_name": name,
            "gender": gender,
            "rating": round(random.uniform(3.8, 5.0), 2),
            "experience_years": round(min(15, join_offset_days / 365.0), 1),
            "join_date": join_date.strftime("%Y-%m-%d"),
            "online_status": online_status,
            "online_since": online_since,
            "vehicle_id": vehicle_pool[i - 1]["vehicle_id"],
        })
    return drivers


def generate_customers():
    customers = []
    for i in range(1, N_CUSTOMERS + 1):
        name, gender = random_name()
        # 80% signed up before the analysis window, 20% sign up during it
        if random.random() < 0.8:
            signup_date = START_DATE - timedelta(days=random.randint(1, 1800))
        else:
            signup_date = random_timestamp_in_range()
        customers.append({
            "customer_id": new_id("CUST", i),
            "full_name": name,
            "gender": gender,
            "age": random.randint(18, 60),
            "signup_date": signup_date.strftime("%Y-%m-%d"),
            "home_zone": random.choice(ZONE_NAMES),
            "preferred_payment_method": random.choice(PAYMENT_METHODS),
            "customer_segment": random.choice(CUSTOMER_SEGMENTS),
            "device_type": random.choice(DEVICE_TYPES),
            "app_version": random.choice(APP_VERSIONS),
        })
    return customers


def generate_promotions():
    promos = []
    for i, (code, name, dtype, dval, camp_type) in enumerate(PROMO_TEMPLATES, start=1):
        start = START_DATE + timedelta(days=random.randint(0, 60))
        end = start + timedelta(days=random.randint(7, 30))
        promos.append({
            "promotion_id": new_id("PROMO", i, width=4),
            "coupon_code": code,
            "campaign_name": name,
            "discount_type": dtype,
            "discount_value": dval,
            "minimum_fare": random.choice([50, 80, 100, 120]),
            "maximum_discount": random.choice([50, 100, 150, 200]),
            "campaign_start": start.strftime("%Y-%m-%d"),
            "campaign_end": end.strftime("%Y-%m-%d"),
            "funded_by": random.choice(FUNDED_BY),
            "eligible_payment_methods": random.sample(["Cash", "Card", "Pathao Wallet"], k=random.randint(1, 3)),
            "maximum_usage": random.choice([1000, 5000, 10000, 20000]),
        })
    return promos

# ============================================================
# RIDE LIFECYCLE SIMULATION
# ============================================================

RIDE_PATHS = ["normal", "customer_cancelled", "driver_cancelled", "no_driver_found", "driver_timeout"]


def choose_path(dt):
    # base weights
    w = {"normal": 75, "customer_cancelled": 8, "driver_cancelled": 4,
         "no_driver_found": 6, "driver_timeout": 7}
    if is_peak(dt):
        w["normal"] -= 10
        w["no_driver_found"] += 5
        w["customer_cancelled"] += 5
    return random.choices(list(w.keys()), weights=list(w.values()), k=1)[0]


def estimate_fare(distance_km, duration_min, vtype, surge):
    base = {"Bike": 20, "Car": 50, "CNG": 35}[vtype]
    per_km = {"Bike": 12, "Car": 28, "CNG": 20}[vtype]
    per_min = {"Bike": 0.8, "Car": 1.5, "CNG": 1.0}[vtype]
    fare = base + per_km * distance_km + per_min * duration_min
    return round(fare * surge, 2)


def choose_surge(dt):
    reasons = []
    multiplier = 1.0
    if is_peak(dt):
        reasons.append("Peak Hours")
        multiplier = random.choice([1.2, 1.5])
    if random.random() < 0.06:
        reasons.append("Rain")
        multiplier = max(multiplier, random.choice([1.5, 2.0]))
    if random.random() < 0.02:
        reasons.append("Special Event")
        multiplier = max(multiplier, 2.0)
    if not reasons:
        multiplier = 1.0
    return multiplier, reasons


def pick_vehicle_type():
    return random.choices(["Bike", "Car", "CNG"], weights=[55, 35, 10], k=1)[0]


def simulate_gps_track(ride_id, driver_id, start_ts, end_ts, start_zone_coord, end_zone_coord, writer):
    """Interpolate a GPS track between pickup and destination, with realistic noise."""
    duration_s = max(1, int((end_ts - start_ts).total_seconds()))
    t = 0
    lat1, lon1 = start_zone_coord
    lat2, lon2 = end_zone_coord
    points = []
    while t < duration_s:
        points.append(t)
        t += random.randint(10, 20)

    n = len(points)
    for idx, t_off in enumerate(points):
        if random.random() < MISSING_GPS_RATE:
            continue  # simulate a dropped GPS ping
        frac = t_off / duration_s if duration_s > 0 else 0
        # slight random walk around the straight interpolated path
        lat = lat1 + (lat2 - lat1) * frac + random.uniform(-0.0015, 0.0015)
        lon = lon1 + (lon2 - lon1) * frac + random.uniform(-0.0015, 0.0015)
        ts = start_ts + timedelta(seconds=t_off)
        if random.random() < OUT_OF_ORDER_RATE:
            ts = ts - timedelta(seconds=random.randint(5, 30))  # out-of-order arrival
        speed = round(max(0, random.gauss(22 if idx not in (0, n - 1) else 5, 6)), 1)
        heading = random.randint(0, 359)
        record = {
            "ride_id": ride_id,
            "driver_id": driver_id,
            "timestamp": iso(ts),
            "latitude": round(lat, 6),
            "longitude": round(lon, 6),
            "speed_kmh": speed,
            "heading": heading,
            "accuracy_m": round(random.uniform(3, 25), 1),
        }
        writer.write(record)


def run_simulation():
    print("Generating master data...")
    vehicles = generate_vehicles()
    drivers = generate_drivers(vehicles)
    customers = generate_customers()
    promotions = generate_promotions()

    print(f"  vehicles={len(vehicles)} drivers={len(drivers)} customers={len(customers)} promotions={len(promotions)}")

    # Write master data
    for name, records in [("vehicles.json", vehicles), ("drivers.json", drivers),
                           ("customers.json", customers), ("promotion.json", promotions)]:
        w = JSONLWriter(os.path.join(OUTPUT_DIR, name), dup_rate=0.0)
        for r in records:
            w.write(r)
        w.close()
        print(f"  wrote {name}: {w.count} lines")

    driver_ids = [d["driver_id"] for d in drivers]
    driver_by_id = {d["driver_id"]: d for d in drivers}
    customer_ids = [c["customer_id"] for c in customers]

    writers = {
        "ride_requested": JSONLWriter(os.path.join(OUTPUT_DIR, "ride_requested.json")),
        "driver_assigned": JSONLWriter(os.path.join(OUTPUT_DIR, "driver_assigned.json")),
        "driver_accepted": JSONLWriter(os.path.join(OUTPUT_DIR, "driver_accepted.json")),
        "driver_arrived": JSONLWriter(os.path.join(OUTPUT_DIR, "driver_arrived.json")),
        "ride_started": JSONLWriter(os.path.join(OUTPUT_DIR, "ride_started.json")),
        "ride_completed": JSONLWriter(os.path.join(OUTPUT_DIR, "ride_completed.json")),
        "ride_cancelled": JSONLWriter(os.path.join(OUTPUT_DIR, "ride_cancelled.json")),
        "payment": JSONLWriter(os.path.join(OUTPUT_DIR, "payment.json")),
        "gps_logs": JSONLWriter(os.path.join(OUTPUT_DIR, "gps_logs.json"), dup_rate=0.0),
        "customer_support": JSONLWriter(os.path.join(OUTPUT_DIR, "customer_support.json")),
    }

    promo_by_code = {p["coupon_code"]: p for p in promotions}
    ticket_counter = 1
    payment_counter = 1

    print(f"Simulating {N_RIDES} rides...")
    for i in range(1, N_RIDES + 1):
        if i % 20000 == 0:
            print(f"  ...{i}/{N_RIDES}")

        ride_id = new_id("RIDE", i)
        customer_id = random.choice(customer_ids)
        requested_ts = random_timestamp_in_range()
        pickup_zone = weighted_zone(requested_ts)
        dest_zone = weighted_zone(requested_ts, exclude=pickup_zone)
        p_lat, p_lon = jitter_coord(*ZONES[pickup_zone])
        d_lat, d_lon = jitter_coord(*ZONES[dest_zone])

        straight_km = haversine_km(p_lat, p_lon, d_lat, d_lon)
        road_factor = random.uniform(1.25, 1.6)
        distance_km = round(straight_km * road_factor, 2)

        vtype = pick_vehicle_type()
        avg_speed_kmh = random.uniform(14, 22) if is_peak(requested_ts) else random.uniform(20, 32)
        duration_min = round((distance_km / avg_speed_kmh) * 60, 1)

        surge, surge_reasons = choose_surge(requested_ts)
        est_fare = estimate_fare(distance_km, duration_min, vtype, surge)

        service_type = random.choice(SERVICE_TYPES)
        ride_type = vtype

        # --- promotion attempt ---
        coupon_applied = False
        coupon_code = None
        promotion_id = None
        discount_amount = 0
        promotion_success = None
        coupon_redemption_status = "not_attempted"
        if random.random() < 0.22:
            attempted_code = random.choice(list(promo_by_code.keys()))
            promo = promo_by_code[attempted_code]
            coupon_code = attempted_code
            promotion_id = promo["promotion_id"]
            outcome = random.choices(
                ["applied_success", "forgot_to_apply", "misunderstood", "below_minimum_fare"],
                weights=[60, 15, 15, 10], k=1)[0]
            if outcome == "applied_success" and est_fare >= promo["minimum_fare"]:
                coupon_applied = True
                if promo["discount_type"] == "Flat":
                    discount_amount = min(promo["discount_value"], promo["maximum_discount"])
                else:
                    discount_amount = min(round(est_fare * promo["discount_value"] / 100, 2), promo["maximum_discount"])
                promotion_success = True
                coupon_redemption_status = "redeemed"
            elif outcome == "forgot_to_apply":
                coupon_applied = False
                promotion_success = False
                coupon_redemption_status = "not_applied_forgot"
            elif outcome == "misunderstood":
                coupon_applied = False
                promotion_success = False
                coupon_redemption_status = "misunderstood_terms"
            else:
                coupon_applied = False
                promotion_success = False
                coupon_redemption_status = "below_minimum_fare"

        # --- ride_requested event ---
        writers["ride_requested"].write({
            "event_id": new_id("EVT-RQ", i),
            "ride_id": ride_id,
            "customer_id": customer_id,
            "pickup": {"zone": pickup_zone, "latitude": p_lat, "longitude": p_lon},
            "destination": {"zone": dest_zone, "latitude": d_lat, "longitude": d_lon},
            "requested_timestamp": iso(requested_ts),
            "estimated_distance_km": distance_km,
            "estimated_duration_min": duration_min,
            "estimated_fare": est_fare,
            "ride_type": ride_type,
            "service_type": service_type,
            "metadata": {
                "app_version": random.choice(APP_VERSIONS),
                "device_type": random.choice(DEVICE_TYPES),
                "platform": "mobile_app",
                "city": "Dhaka",
                "coupon_applied": coupon_applied,
                "coupon_code": coupon_code,
                "promotion_id": promotion_id,
                "discount_amount": discount_amount,
                "promotion_success": promotion_success,
                "coupon_redemption_status": coupon_redemption_status,
                "surge_reasons": surge_reasons,
            },
        })

        path = choose_path(requested_ts)
        cur_ts = requested_ts
        assigned_driver_id = None

        # --- driver_assigned (all paths except no_driver_found) ---
        if path != "no_driver_found":
            assigned_driver_id = random.choice(driver_ids)
            assign_delay = timedelta(seconds=random.randint(5, 45))
            cur_ts = cur_ts + assign_delay
            distance_to_pickup = round(random.uniform(0.3, 4.5), 2)
            est_arrival_min = round(distance_to_pickup / random.uniform(15, 25) * 60, 1)
            writers["driver_assigned"].write({
                "event_id": new_id("EVT-DA", i),
                "ride_id": ride_id,
                "driver_id": assigned_driver_id,
                "assignment_algorithm": random.choice(ASSIGNMENT_ALGORITHMS),
                "assigned_timestamp": iso(cur_ts),
                "estimated_arrival_min": est_arrival_min,
                "distance_to_pickup_km": distance_to_pickup,
            })

        if path == "no_driver_found":
            cancel_ts = cur_ts + timedelta(seconds=random.randint(60, 180))
            writers["ride_cancelled"].write({
                "ride_id": ride_id,
                "cancelled_by": "System",
                "reason": "No Driver Found",
                "timestamp": iso(cancel_ts),
                "stage_reached": "ride_requested",
            })
            continue

        if path == "customer_cancelled":
            cancel_ts = cur_ts + timedelta(seconds=random.randint(10, 120))
            reason = random.choice(["Customer Changed Mind", "Wrong Pickup", "Coupon Payment Confusion", "Emergency"])
            writers["ride_cancelled"].write({
                "ride_id": ride_id,
                "cancelled_by": "Customer",
                "reason": reason,
                "timestamp": iso(cancel_ts),
                "stage_reached": "driver_assigned",
            })
            if reason in ("Coupon Payment Confusion",) or random.random() < 0.3:
                writers["customer_support"].write(make_ticket(ticket_counter, ride_id, customer_id, cancel_ts))
                ticket_counter += 1
            continue

        if path == "driver_timeout":
            cancel_ts = cur_ts + timedelta(seconds=random.randint(30, 90))
            writers["ride_cancelled"].write({
                "ride_id": ride_id,
                "cancelled_by": "System",
                "reason": "Driver Taking Too Long",
                "timestamp": iso(cancel_ts),
                "stage_reached": "driver_assigned",
            })
            continue

        # --- driver_accepted (normal + driver_cancelled paths) ---
        acceptance_delay = random.randint(3, 40)
        cur_ts = cur_ts + timedelta(seconds=acceptance_delay)
        driver_rating = driver_by_id[assigned_driver_id]["rating"]
        writers["driver_accepted"].write({
            "event_id": new_id("EVT-DC", i),
            "ride_id": ride_id,
            "driver_id": assigned_driver_id,
            "accepted_timestamp": iso(cur_ts),
            "driver_rating": driver_rating,
            "acceptance_delay_seconds": acceptance_delay,
        })

        if path == "driver_cancelled":
            cancel_ts = cur_ts + timedelta(seconds=random.randint(20, 200))
            reason = random.choice(["Traffic", "Wrong Pickup", "Emergency", "Payment Issue"])
            writers["ride_cancelled"].write({
                "ride_id": ride_id,
                "cancelled_by": "Driver",
                "reason": reason,
                "timestamp": iso(cancel_ts),
                "stage_reached": "driver_accepted",
            })
            if random.random() < 0.4:
                writers["customer_support"].write(make_ticket(ticket_counter, ride_id, customer_id, cancel_ts))
                ticket_counter += 1
            continue

        # --- normal path continues: driver_arrived ---
        waiting_time_s = random.randint(60, 480) if is_peak(requested_ts) else random.randint(30, 300)
        cur_ts = cur_ts + timedelta(seconds=random.randint(60, 400))  # travel time to pickup
        arrival_ts = cur_ts
        writers["driver_arrived"].write({
            "event_id": new_id("EVT-DR", i),
            "ride_id": ride_id,
            "arrival_timestamp": iso(arrival_ts),
            "waiting_time_seconds": waiting_time_s,
        })

        # --- ride_started ---
        start_ts = arrival_ts + timedelta(seconds=waiting_time_s)
        otp_verified = random.random() > 0.02
        writers["ride_started"].write({
            "event_id": new_id("EVT-RS", i),
            "ride_id": ride_id,
            "start_timestamp": iso(start_ts),
            "OTP_verified": otp_verified,
        })

        # --- ride_completed ---
        actual_duration_min = max(1.0, round(duration_min * random.uniform(0.85, 1.35), 1))
        end_ts = start_ts + timedelta(minutes=actual_duration_min)
        actual_distance_km = max(0.3, round(distance_km * random.uniform(0.92, 1.15), 2))
        avg_speed = round(actual_distance_km / (actual_duration_min / 60), 1) if actual_duration_min > 0 else 0
        waiting_charge = round(max(0, (waiting_time_s - 180)) / 60 * 2, 2)  # BDT 2/min after free 3 min
        fare = estimate_fare(actual_distance_km, actual_duration_min, vtype, surge) + waiting_charge
        fare = round(fare - discount_amount, 2)
        fare = max(fare, 10)
        commission_rate = 0.20
        driver_payout = round(fare * (1 - commission_rate), 2)
        platform_commission = round(fare - driver_payout, 2)
        rating_customer = round(random.uniform(3.5, 5.0), 1)
        rating_driver = round(random.uniform(3.5, 5.0), 1)

        writers["ride_completed"].write({
            "event_id": new_id("EVT-RC", i),
            "ride_id": ride_id,
            "driver_id": assigned_driver_id,
            "customer_id": customer_id,
            "distance_km": actual_distance_km,
            "duration_min": actual_duration_min,
            "average_speed_kmh": avg_speed,
            "fare": fare,
            "waiting_charge": waiting_charge,
            "surge_multiplier": surge,
            "discount": discount_amount,
            "driver_payout": driver_payout,
            "platform_commission": platform_commission,
            "ride_rating_customer": maybe_null(rating_customer, 0.08),
            "ride_rating_driver": maybe_null(rating_driver, 0.08),
            "completed_timestamp": iso(end_ts),
        })

        # --- payment ---
        payment_status = random.choices(["Completed", "Pending", "Failed"], weights=[90, 4, 6], k=1)[0]
        payment_method = random.choice(PAYMENT_METHODS)
        payment_ts = end_ts + timedelta(seconds=random.randint(2, 60))
        writers["payment"].write({
            "payment_id": new_id("PAY", payment_counter, width=8),
            "ride_id": ride_id,
            "payment_method": payment_method,
            "payment_status": payment_status,
            "transaction_time": iso(payment_ts),
            "amount": fare,
            "discount": discount_amount,
            "commission": platform_commission,
            "driver_payout": driver_payout if payment_status == "Completed" else 0,
        })
        payment_counter += 1

        if payment_status == "Failed" and random.random() < 0.5:
            writers["customer_support"].write(make_ticket(ticket_counter, ride_id, customer_id, payment_ts,
                                                            forced_issue="Payment deducted twice" if random.random() < 0.4 else "Wrong fare"))
            ticket_counter += 1

        # --- GPS logs for the driving segment ---
        simulate_gps_track(ride_id, assigned_driver_id, start_ts, end_ts,
                            (p_lat, p_lon), (d_lat, d_lon), writers["gps_logs"])

        # --- occasional unrelated support ticket ---
        if random.random() < 0.03:
            writers["customer_support"].write(make_ticket(ticket_counter, ride_id, customer_id, end_ts))
            ticket_counter += 1

    for w in writers.values():
        w.close()

    print("Done. Line counts:")
    for name in ["ride_requested", "driver_assigned", "driver_accepted", "driver_arrived",
                 "ride_started", "ride_completed", "ride_cancelled", "payment",
                 "gps_logs", "customer_support"]:
        path = os.path.join(OUTPUT_DIR, f"{name}.json")
        print(f"  {name}.json")


def make_ticket(counter, ride_id, customer_id, around_ts, forced_issue=None):
    issue = forced_issue or random.choice(SUPPORT_ISSUES)
    priority = random.choice(SUPPORT_PRIORITIES)
    created_at = around_ts + timedelta(minutes=random.randint(1, 240))
    status = random.choice(RESOLUTION_STATUSES)
    resolved_at = None
    if status == "Resolved":
        resolved_at = iso(created_at + timedelta(hours=random.randint(1, 72)))
    return {
        "ticket_id": new_id("TICKET", counter, width=6),
        "ride_id": ride_id,
        "customer_id": customer_id,
        "issue_category": issue,
        "priority": priority,
        "resolution_status": status,
        "created_at": iso(created_at),
        "resolved_at": resolved_at,
    }


if __name__ == "__main__":
    run_simulation()
