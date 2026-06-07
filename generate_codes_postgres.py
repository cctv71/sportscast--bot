"""
SportsCast UK - Access Code Generator (Postgres edition)

Writes one-time access codes directly into the Postgres database that the bot
reads from. The bot deletes each code on redemption, so these are single-use.

USAGE (run on your laptop with DATABASE_URL set, or via `heroku run`):
    python generate_codes_postgres.py                 # 10 codes, valid 48 hours
    python generate_codes_postgres.py 100             # 100 codes, valid 48 hours
    python generate_codes_postgres.py 100 720         # 100 codes, valid 720 hours (30 days)

Easiest on Heroku:
    heroku run python generate_codes_postgres.py 100 720 --app your-app-name

Each code is written to the database AND saved to codes_to_send.txt in the
ready-to-paste format:  /code SPORTXXXXXXXXXX
"""

import os
import sys
import uuid
from datetime import datetime, timedelta

import pytz
import psycopg2

DATABASE_URL = os.environ['DATABASE_URL']
if DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)


def get_db():
    return psycopg2.connect(DATABASE_URL, sslmode='require')


def ensure_table(cur):
    """Make sure the access_codes table exists (matches the bot's schema)."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS access_codes (
            code TEXT PRIMARY KEY,
            created_at TIMESTAMPTZ NOT NULL,
            expires_at TIMESTAMPTZ NOT NULL
        );
    """)


def generate_codes(count=10, hours=48):
    now = datetime.now(pytz.utc)
    expires = now + timedelta(hours=hours)
    created = []

    conn = get_db()
    try:
        cur = conn.cursor()
        ensure_table(cur)
        for _ in range(count):
            code = f"SPORT{uuid.uuid4().hex[:10].upper()}"
            cur.execute(
                'INSERT INTO access_codes (code, created_at, expires_at) VALUES (%s, %s, %s) ON CONFLICT (code) DO NOTHING',
                (code, now, expires)
            )
            created.append(code)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    # Save a ready-to-send file: each line is what the customer pastes
    with open('codes_to_send.txt', 'w') as f:
        for code in created:
            f.write(f'/code {code}\n')

    print(f'\n✅ Generated {len(created)} codes (valid {hours} hours, expires {expires:%Y-%m-%d %H:%M UTC})')
    print('Saved to codes_to_send.txt (ready to paste / send to customers)\n')
    for code in created:
        print(f'/code {code}')


if __name__ == '__main__':
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    hours = int(sys.argv[2]) if len(sys.argv) > 2 else 48
    generate_codes(count, hours)
