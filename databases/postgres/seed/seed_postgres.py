#!/usr/bin/env python3
"""
Sokti OTT Platform - PostgreSQL OLTP Data Seeder
Populates plans, users, profiles, subscriptions, devices, and watchlists.
"""

import os
import random
import uuid
from datetime import datetime, timedelta, timezone
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = int(os.getenv("POSTGRES_PORT", "15432"))
DB_NAME = os.getenv("POSTGRES_DB", "sokti")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")

COUNTRIES = ["US", "IN", "GB", "CA", "DE", "FR", "BR", "JP", "AU", "SG"]
FIRST_NAMES = [
    "Aarav", "Aditi", "Alex", "Amara", "Carlos", "Chloe", "David", "Elena", 
    "Fatima", "Hiroshi", "Isabella", "James", "Kenji", "Liam", "Maya", 
    "Nina", "Oliver", "Priya", "Rahul", "Sara", "Tariq", "Vikram", "Zara", "Zoe"
]
LAST_NAMES = [
    "Sharma", "Smith", "Patel", "Johnson", "Garcia", "Tanaka", "Muller", 
    "Dubois", "Silva", "Williams", "Brown", "Jones", "Kumar", "Singh", "Chen"
]
DEVICE_TYPES = [
    ("smart_tv", "Samsung Smart TV Tizen", "Tizen OS 7.0"),
    ("smart_tv", "LG OLED webOS", "webOS 23"),
    ("streaming_stick", "Amazon Fire TV Stick 4K", "FireOS 7"),
    ("streaming_stick", "Apple TV 4K", "tvOS 17.2"),
    ("mobile", "Apple iPhone 15 Pro", "iOS 17.4"),
    ("mobile", "Samsung Galaxy S24", "Android 14"),
    ("tablet", "Apple iPad Air", "iPadOS 17"),
    ("web", "Google Chrome Desktop", "Windows 11"),
    ("web", "Apple Safari Desktop", "macOS Sonoma")
]
APP_VERSIONS = ["3.12.0", "3.12.1", "3.13.0-beta", "3.11.4"]

def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

def seed_plans(cur):
    print("Seeding subscription plans...")
    plans = [
        (str(uuid.uuid4()), "Mobile Only (SD)", 399, "USD", 1, "480p"),
        (str(uuid.uuid4()), "Standard HD", 999, "USD", 2, "1080p"),
        (str(uuid.uuid4()), "Premium 4K Ultra", 1599, "USD", 4, "4K+HDR"),
        (str(uuid.uuid4()), "Family Bundle (4K)", 1999, "USD", 6, "4K+HDR")
    ]
    cur.execute("SELECT plan_id, plan_name FROM plans;")
    existing = cur.fetchall()
    if existing:
        print(f"Plans already seeded ({len(existing)} found).")
        return [row[0] for row in existing]
    
    insert_query = """
    INSERT INTO plans (plan_id, plan_name, price_cents, currency, max_streams, resolution)
    VALUES (%s, %s, %s, %s, %s, %s);
    """
    cur.executemany(insert_query, plans)
    print(f"Seeded {len(plans)} plans.")
    return [p[0] for p in plans]

def seed_users_and_relations(cur, plan_ids, num_users=1000):
    print(f"Checking existing users...")
    cur.execute("SELECT count(*) FROM users;")
    count = cur.fetchone()[0]
    if count >= num_users:
        print(f"Users already seeded ({count} found). Skipping user seeding.")
        return

    print(f"Generating and inserting {num_users} users with profiles, subscriptions, and devices...")
    now = datetime.now(timezone.utc)

    users_data = []
    profiles_data = []
    subscriptions_data = []
    devices_data = []
    watchlists_data = []

    sample_content_ids = [f"cnt_mov_{i:04d}" for i in range(1, 101)]

    for i in range(num_users):
        u_id = str(uuid.uuid4())
        fname = random.choice(FIRST_NAMES)
        lname = random.choice(LAST_NAMES)
        email = f"{fname.lower()}.{lname.lower()}_{i}_{random.randint(100, 999)}@sokti-stream.com"
        country = random.choice(COUNTRIES)
        phone = f"+{random.randint(1, 99)} {random.randint(1000000000, 9999999999)}"
        created_days_ago = random.randint(1, 365)
        created_at = now - timedelta(days=created_days_ago, seconds=random.randint(0, 86400))
        updated_at = created_at + timedelta(days=random.randint(0, created_days_ago))

        users_data.append((u_id, email, phone, country, created_at, updated_at))

        # 1-3 profiles per user
        num_profiles = random.choices([1, 2, 3], weights=[0.4, 0.4, 0.2])[0]
        profiles_data.append((
            str(uuid.uuid4()), u_id, f"{fname}'s Profile", "avatar_default_1", False, "en", created_at
        ))
        if num_profiles >= 2:
            profiles_data.append((
                str(uuid.uuid4()), u_id, "Kids Corner", "avatar_kids_star", True, "en", created_at
            ))
        if num_profiles == 3:
            profiles_data.append((
                str(uuid.uuid4()), u_id, f"{lname} Family", "avatar_family_popcorn", False, 
                random.choice(["hi", "es", "fr", "ja"]), created_at
            ))

        # Subscriptions
        plan_id = random.choice(plan_ids)
        sub_status = random.choices(["active", "past_due", "canceled"], weights=[0.85, 0.08, 0.07])[0]
        sub_started = created_at
        sub_expires = now + timedelta(days=random.randint(5, 30)) if sub_status == "active" else now - timedelta(days=random.randint(1, 60))
        subscriptions_data.append((
            str(uuid.uuid4()), u_id, plan_id, sub_status, sub_started, sub_expires,
            random.choice(["visa", "mastercard", "apple_pay", "upi"]), updated_at
        ))

        # Devices (1-3 devices)
        num_devices = random.choices([1, 2, 3], weights=[0.3, 0.5, 0.2])[0]
        chosen_devices = random.sample(DEVICE_TYPES, num_devices)
        for dev_type, dev_name, os_name in chosen_devices:
            devices_data.append((
                str(uuid.uuid4()), u_id, dev_type, dev_name, os_name,
                random.choice(APP_VERSIONS), updated_at
            ))

        # Watchlist (0-5 items)
        watchlist_items = random.sample(sample_content_ids, random.randint(0, 5))
        for cid in watchlist_items:
            watchlists_data.append((
                str(uuid.uuid4()), u_id, cid, created_at + timedelta(hours=random.randint(1, 72))
            ))

    # Batch inserts
    print("Executing batch insert for users...")
    execute_batch(cur, """
        INSERT INTO users (user_id, email, phone, country_code, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, users_data)

    print("Executing batch insert for profiles...")
    execute_batch(cur, """
        INSERT INTO profiles (profile_id, user_id, profile_name, avatar_id, is_kids, language_preference, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, profiles_data)

    print("Executing batch insert for subscriptions...")
    execute_batch(cur, """
        INSERT INTO subscriptions (subscription_id, user_id, plan_id, status, started_at, expires_at, payment_method, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, subscriptions_data)

    print("Executing batch insert for devices...")
    execute_batch(cur, """
        INSERT INTO devices (device_id, user_id, device_type, device_name, os, app_version, last_used_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, devices_data)

    print("Executing batch insert for watchlists...")
    execute_batch(cur, """
        INSERT INTO watchlists (watchlist_id, user_id, content_id, added_at)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (user_id, content_id) DO NOTHING
    """, watchlists_data)

    print(f"Successfully seeded {len(users_data)} users and associated entities.")

def main():
    print(f"Connecting to PostgreSQL at {DB_HOST}:{DB_PORT}/{DB_NAME}...")
    conn = get_connection()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            plan_ids = seed_plans(cur)
            seed_users_and_relations(cur, plan_ids, num_users=1000)
            conn.commit()
            print("PostgreSQL seeding completed successfully!")
    except Exception as e:
        conn.rollback()
        print(f"Error seeding PostgreSQL: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
