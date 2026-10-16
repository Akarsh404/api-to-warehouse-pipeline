"""Synthetic data generator for customers and orders datasets.

Generates reproducible synthetic datasets with a fixed seed. Deliberately
includes anomalies (duplicates, null fields, wrong data types, orphan orders)
to test pipeline resilience, deduplication, quarantine, and validation logic.
"""

import argparse
from datetime import datetime, timezone, timedelta
import json
from pathlib import Path
import random
from typing import Any, Dict, List, Tuple

# Sample pools for deterministic generator
FIRST_NAMES = [
    "Aarav", "Aditi", "Alex", "Amara", "Carlos", "Elena", "Fatima",
    "Kenji", "Liam", "Maya", "Noah", "Olivia", "Priya", "Sofia", "Yuki"
]
LAST_NAMES = [
    "Chen", "Devi", "Garcia", "Ivanov", "Kim", "Muller", "Patel",
    "Rossi", "Santos", "Smith", "Tanaka", "Wang", "Williams", "Zhang"
]
COUNTRIES = ["US", "CA", "GB", "DE", "FR", "IN", "JP", "AU", "BR", "SG"]
ORDER_STATUSES = ["completed", "pending", "cancelled", "refunded"]


def generate_synthetic_data(
    num_customers: int = 50,
    num_orders: int = 120,
    seed: int = 42,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Generate reproducible customer and order records with deliberate anomalies."""
    rng = random.Random(seed)
    base_time = datetime(2026, 10, 1, 10, 0, 0, tzinfo=timezone.utc)

    # ---------------------------------------------------------
    # 1. Base Valid Customers
    # ---------------------------------------------------------
    customers: List[Dict[str, Any]] = []
    valid_customer_ids: List[str] = []

    for i in range(1, num_customers + 1):
        cust_id = f"CUST-{i:04d}"
        valid_customer_ids.append(cust_id)
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        name = f"{first} {last}"
        email = f"{first.lower()}.{last.lower()}{i}@example.com"
        country = rng.choice(COUNTRIES)
        # updated_at spread over 10 days
        updated_at = (base_time + timedelta(hours=i * 4)).isoformat()

        customers.append({
            "customer_id": cust_id,
            "name": name,
            "email": email,
            "country": country,
            "updated_at": updated_at,
        })

    # ---------------------------------------------------------
    # 2. Deliberate Customer Anomalies
    # ---------------------------------------------------------
    # Duplicate with LATER updated_at (should supersede original during dedup)
    dup_cust_1 = dict(customers[2])  # CUST-0003
    dup_cust_1["name"] = dup_cust_1["name"] + " Updated"
    dup_cust_1["updated_at"] = (base_time + timedelta(days=20)).isoformat()
    customers.append(dup_cust_1)

    # Duplicate with EARLIER updated_at (should be discarded during dedup)
    dup_cust_2 = dict(customers[4])  # CUST-0005
    dup_cust_2["name"] = "Stale Old Name"
    dup_cust_2["updated_at"] = (base_time - timedelta(days=5)).isoformat()
    customers.append(dup_cust_2)

    # Null required fields (should be quarantined)
    customers.append({
        "customer_id": "CUST-9001",
        "name": None,
        "email": "null.name@example.com",
        "country": "US",
        "updated_at": (base_time + timedelta(days=1)).isoformat(),
    })
    customers.append({
        "customer_id": "CUST-9002",
        "name": "Missing Email",
        "email": None,
        "country": "GB",
        "updated_at": (base_time + timedelta(days=1)).isoformat(),
    })

    # Wrong data types (should be quarantined)
    customers.append({
        "customer_id": "CUST-9003",
        "name": "Invalid Email Type",
        "email": 12345,  # int instead of string
        "country": "DE",
        "updated_at": (base_time + timedelta(days=1)).isoformat(),
    })
    customers.append({
        "customer_id": "CUST-9004",
        "name": "Invalid Date",
        "email": "bad.date@example.com",
        "country": "FR",
        "updated_at": "not-a-valid-timestamp",
    })

    # ---------------------------------------------------------
    # 3. Base Valid Orders
    # ---------------------------------------------------------
    orders: List[Dict[str, Any]] = []

    for i in range(1, num_orders + 1):
        order_id = f"ORD-{i:05d}"
        cust_id = rng.choice(valid_customer_ids)
        amount = round(rng.uniform(10.0, 999.0), 2)
        status = rng.choice(ORDER_STATUSES)
        order_ts = (base_time + timedelta(hours=i * 2)).isoformat()
        updated_at = (base_time + timedelta(hours=i * 2 + 1)).isoformat()

        orders.append({
            "order_id": order_id,
            "customer_id": cust_id,
            "amount": amount,
            "status": status,
            "order_ts": order_ts,
            "updated_at": updated_at,
        })

    # ---------------------------------------------------------
    # 4. Deliberate Order Anomalies
    # ---------------------------------------------------------
    # Duplicate with LATER updated_at (status changed to completed, should win)
    dup_order_1 = dict(orders[5])  # ORD-00006
    dup_order_1["status"] = "completed"
    dup_order_1["updated_at"] = (base_time + timedelta(days=25)).isoformat()
    orders.append(dup_order_1)

    # Duplicate with EARLIER updated_at (should be discarded during dedup)
    dup_order_2 = dict(orders[9])  # ORD-00010
    dup_order_2["status"] = "pending"
    dup_order_2["updated_at"] = (base_time - timedelta(days=2)).isoformat()
    orders.append(dup_order_2)

    # Orphan orders (customer_id does not exist in customers, should be quarantined)
    orders.append({
        "order_id": "ORD-99001",
        "customer_id": "CUST-NONEXISTENT-999",
        "amount": 149.99,
        "status": "completed",
        "order_ts": (base_time + timedelta(days=3)).isoformat(),
        "updated_at": (base_time + timedelta(days=3, hours=1)).isoformat(),
    })
    orders.append({
        "order_id": "ORD-99002",
        "customer_id": "CUST-UNKNOWN-888",
        "amount": 49.50,
        "status": "pending",
        "order_ts": (base_time + timedelta(days=3)).isoformat(),
        "updated_at": (base_time + timedelta(days=3, hours=1)).isoformat(),
    })

    # Null required fields (should be quarantined)
    orders.append({
        "order_id": "ORD-99003",
        "customer_id": valid_customer_ids[0],
        "amount": None,
        "status": "completed",
        "order_ts": (base_time + timedelta(days=4)).isoformat(),
        "updated_at": (base_time + timedelta(days=4, hours=1)).isoformat(),
    })
    orders.append({
        "order_id": "ORD-99004",
        "customer_id": None,
        "amount": 75.00,
        "status": "pending",
        "order_ts": (base_time + timedelta(days=4)).isoformat(),
        "updated_at": (base_time + timedelta(days=4, hours=1)).isoformat(),
    })

    # Wrong data types (should be quarantined)
    orders.append({
        "order_id": "ORD-99005",
        "customer_id": valid_customer_ids[1],
        "amount": "NOT_A_DECIMAL",  # non-numeric string
        "status": "completed",
        "order_ts": (base_time + timedelta(days=5)).isoformat(),
        "updated_at": (base_time + timedelta(days=5, hours=1)).isoformat(),
    })
    orders.append({
        "order_id": "ORD-99006",
        "customer_id": valid_customer_ids[2],
        "amount": 120.00,
        "status": 12345,  # int instead of status string
        "order_ts": (base_time + timedelta(days=5)).isoformat(),
        "updated_at": (base_time + timedelta(days=5, hours=1)).isoformat(),
    })

    return customers, orders


def save_synthetic_data(
    output_dir: Path,
    num_customers: int = 50,
    num_orders: int = 120,
    seed: int = 42,
) -> Tuple[Path, Path]:
    """Generate and write synthetic datasets to JSON files."""
    output_dir.mkdir(parents=True, exist_ok=True)
    customers, orders = generate_synthetic_data(
        num_customers=num_customers,
        num_orders=num_orders,
        seed=seed,
    )

    customers_path = output_dir / "customers.json"
    orders_path = output_dir / "orders.json"

    with open(customers_path, "w", encoding="utf-8") as f:
        json.dump(customers, f, indent=2)

    with open(orders_path, "w", encoding="utf-8") as f:
        json.dump(orders, f, indent=2)

    return customers_path, orders_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic pipeline datasets.")
    parser.add_argument("--customers", type=int, default=50, help="Number of base customers")
    parser.add_argument("--orders", type=int, default=120, help="Number of base orders")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(Path(__file__).resolve().parent),
        help="Directory to save JSON fixtures",
    )
    args = parser.parse_args()

    cust_file, ord_file = save_synthetic_data(
        output_dir=Path(args.output_dir),
        num_customers=args.customers,
        num_orders=args.orders,
        seed=args.seed,
    )

    with open(cust_file, "r", encoding="utf-8") as f:
        c_data = json.load(f)
    with open(ord_file, "r", encoding="utf-8") as f:
        o_data = json.load(f)

    print(f"Generated {len(c_data)} customer records -> {cust_file}")
    print(f"Generated {len(o_data)} order records -> {ord_file}")
