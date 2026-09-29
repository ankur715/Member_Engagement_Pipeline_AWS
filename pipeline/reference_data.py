"""Deterministic synthetic world shared by every simulated source: health
plans (the customers), members, community health workers, event venues.
Health plans and people are fictional; seeded so every generator and the
mock APIs agree on the same members without querying the warehouse.
"""
import random
from datetime import date, timedelta
from functools import lru_cache

from faker import Faker

# (plan_key, display name, plan codes offered)
HEALTH_PLANS = [
    ("evergreen", "Evergreen Health Plan", ["MA-HMO", "MA-PPO", "D-SNP"]),
    ("harbor", "Harbor Medicare Advantage", ["MA-HMO", "C-SNP"]),
]

# (county, first 3 digits of its ZIP codes) -- where members live.
COUNTIES = [("Kings", "112"), ("Queens", "113"), ("Bronx", "104"), ("Nassau", "115"), ("Westchester", "105")]

# Kinds of CHW work logged in Salesforce (the Task "Type" field).
ACTIVITY_TYPES = ["Wellness Call", "Home Visit", "Care Gap Outreach", "Transportation Assist", "Welcome Call"]

# Kinds of community events members can attend.
EVENT_TYPES = ["Neighborhood Group", "Health Fair", "Exercise Class", "Nutrition Workshop", "Tech Help Session"]

# Where events happen, per county.
VENUES = {
    "Kings": ["Flatbush Senior Center", "Park Slope Library"],
    "Queens": ["Jamaica Community Hall", "Flushing YMCA"],
    "Bronx": ["Fordham Senior Center", "Mott Haven Community Room"],
    "Nassau": ["Hempstead Rec Center"],
    "Westchester": ["Yonkers Community Center"],
}

NUM_MEMBERS = 60
ACTIVITY_START = date(2026, 6, 1)  # history start for the mock Salesforce/events APIs


@lru_cache(maxsize=1)  # build once per process; every caller gets the same list
def member_roster() -> list[dict]:
    """60 synthetic Medicare-age members, identical on every call."""
    fake = Faker()
    Faker.seed(42)           # fixed seeds = the same fake people every run
    rng = random.Random(42)
    roster = []
    for i in range(1, NUM_MEMBERS + 1):
        # Alternate members between the two health plans.
        plan_key, plan_name, plan_codes = HEALTH_PLANS[i % len(HEALTH_PLANS)]
        county, zip_prefix = rng.choice(COUNTIES)
        gender = rng.choice(["F", "M"])
        roster.append({
            "member_id": f"MEM{10000 + i}",                    # MEM10001, MEM10002, ...
            "plan_key": plan_key,
            "health_plan": plan_name,
            "base_plan_code": rng.choice(plan_codes),          # the member's usual plan...
            "alt_plan_code": rng.choice(plan_codes),           # ...and the one they sometimes switch to
            "first_name": fake.first_name_female() if gender == "F" else fake.first_name_male(),
            "last_name": fake.last_name(),
            "dob": date(1940, 1, 1) + timedelta(days=rng.randint(0, 365 * 19)),  # born 1940-1958 (Medicare age)
            "gender": gender,
            "phone": f"{rng.randint(200, 989)}{rng.randint(200, 999)}{rng.randint(1000, 9999)}",  # 10 digits
            "zip": f"{zip_prefix}{rng.randint(10, 99)}",       # 5-digit ZIP in the member's county
            "county": county,
            "coverage_start": date(2025, 1, 1) + timedelta(days=30 * rng.randint(0, 12)),  # joined during 2025
        })
    return roster


def member_ids() -> list[str]:
    # Just the ids, for generators that only need to pick a member.
    return [m["member_id"] for m in member_roster()]


@lru_cache(maxsize=1)
def chw_names() -> list[str]:
    # Six fake community health workers (activity owners / event hosts).
    fake = Faker()
    Faker.seed(7)
    return [fake.name() for _ in range(6)]
