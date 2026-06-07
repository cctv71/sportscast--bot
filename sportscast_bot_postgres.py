"""
SportsCast UK Bot - Heroku Postgres Edition

All data stored in PostgreSQL database (no JSON files).
Survives Heroku 24-hour resets.

Required Heroku environment variables:
- TELEGRAM_TOKEN
- TELEGRAM_CHAT_ID
- DATABASE_URL (auto-set by Heroku Postgres)
"""

import os
import time
import asyncio
import logging
from datetime import datetime, timedelta, date
from collections import defaultdict
from contextlib import contextmanager

import pytz
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)
log = logging.getLogger('sportscast')

TELEGRAM_TOKEN = os.environ['TELEGRAM_TOKEN']
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')
SPORTSDB_KEY = '198616'
DATABASE_URL = os.environ['DATABASE_URL']

# Fix for Heroku Postgres URL format
if DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

LOCAL_TZ = pytz.timezone('Europe/London')
V1_BASE = f'https://www.thesportsdb.com/api/v1/json/{SPORTSDB_KEY}'
V2_BASE = 'https://www.thesportsdb.com/api/v2/json'
V2_HEADERS = {'X-API-KEY': SPORTSDB_KEY, 'Content-Type': 'application/json'}
REQ_TIMEOUT = 15

CACHE_EXPIRY_HOURS = 0.5
FORM_CACHE_EXPIRY_HOURS = 6
LIVE_SCORES_CACHE_MINUTES = 2
HIDE_PAST_MATCHES_AFTER_HOURS = 2
NOTIFICATION_CHECK_INTERVAL = 60
INACTIVITY_RESET_HOURS = 8

RATE_LIMIT_MAX_FAILS = 5
RATE_LIMIT_WINDOW_MINS = 5
RATE_LIMIT_BLOCK_HOURS = 2

ADMIN_USER_IDS = set()


# ===========================================================================
# Database setup
# ===========================================================================

def get_db():
    """Get database connection. Heroku Postgres requires SSL."""
    return psycopg2.connect(DATABASE_URL, sslmode='require')


def utc_now():
    """Single source of truth for timestamps. Always timezone-aware UTC."""
    return datetime.now(pytz.utc)


@contextmanager
def db_cursor(dict_cursor=False):
    """Safe DB access: commits on success, rolls back on error, always closes.
    Use:  with db_cursor() as cur: cur.execute(...)
    """
    conn = get_db()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor) if dict_cursor else conn.cursor()
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """Create all tables if they don't exist."""
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS access_codes (
            code TEXT PRIMARY KEY,
            created_at TIMESTAMPTZ NOT NULL,
            expires_at TIMESTAMPTZ NOT NULL
        );

        CREATE TABLE IF NOT EXISTS authorized_users (
            user_id TEXT PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            code_used TEXT,
            redeemed_at TIMESTAMPTZ NOT NULL
        );

        CREATE TABLE IF NOT EXISTS blacklist (
            user_id TEXT PRIMARY KEY,
            reason TEXT,
            blocked_at TIMESTAMPTZ NOT NULL,
            blocked_by TEXT
        );

        CREATE TABLE IF NOT EXISTS rate_limits (
            user_id TEXT PRIMARY KEY,
            fails TEXT,
            blocked_until TIMESTAMPTZ
        );

        CREATE TABLE IF NOT EXISTS audit_log (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMPTZ NOT NULL,
            event_type TEXT,
            user_id TEXT,
            username TEXT,
            first_name TEXT,
            detail TEXT
        );

        CREATE TABLE IF NOT EXISTS user_activity (
            user_id TEXT PRIMARY KEY,
            last_active TIMESTAMPTZ NOT NULL
        );

        CREATE TABLE IF NOT EXISTS match_alerts (
            id SERIAL PRIMARY KEY,
            event_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            windows TEXT NOT NULL,
            matchup TEXT,
            league TEXT,
            venue TEXT,
            sport_key TEXT,
            datetime_utc TIMESTAMPTZ,
            UNIQUE(event_id, user_id)
        );

        CREATE TABLE IF NOT EXISTS notified_matches (
            notified_key TEXT PRIMARY KEY,
            notified_at TIMESTAMPTZ NOT NULL
        );
    """)
    # Indexes on frequently queried columns
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_authorized_user ON authorized_users(user_id);
        CREATE INDEX IF NOT EXISTS idx_blacklist_user ON blacklist(user_id);
        CREATE INDEX IF NOT EXISTS idx_alerts_user ON match_alerts(user_id);
        CREATE INDEX IF NOT EXISTS idx_alerts_datetime ON match_alerts(datetime_utc);
    """)
    conn.commit()
    cur.close()
    conn.close()
    log.info('✅ Database tables and indexes initialized')


# ===========================================================================
# Sport categories
# ===========================================================================

SPORT_CATEGORIES = [
    {'key': 'football', 'icon': '⚽', 'label': 'Football',
     'leagues': [('Premier League', '4328'), ('Championship', '4329'), ('League One', '4396'),
                 ('League Two', '4397'), ('FA Cup', '4482'), ('EFL Cup', '4483'),
                 ('Scottish Premiership', '4330'), ('🌍 International Football', 'INTL')]},
    {'key': 'cricket', 'icon': '🏏', 'label': 'Cricket',
     'leagues': [('IPL', '4460'), ('Big Bash League', '4461'), ('T20 Blast', '4463'), ('ICC Test Championship', '4451')]},
    {'key': 'rugby', 'icon': '🏉', 'label': 'Rugby',
     'leagues': [('English Premiership Rugby', '4414'), ('Super League (Rugby League)', '4415'),
                 ('Six Nations', '4714'), ('RFU Championship', '4722')]},
    {'key': 'combat', 'icon': '🥊', 'label': 'Combat',
     'leagues': [('Boxing', '4445'), ('UFC / MMA', '4443'), ('WWE', '4449')]},
    {'key': 'darts', 'icon': '🎯', 'label': 'Darts',
     'leagues': [('PDC Darts', '4554')]},
    {'key': 'snooker', 'icon': '🎱', 'label': 'Snooker',
     'leagues': [('World Snooker Tour', '4555')]},
    {'key': 'basketball', 'icon': '🏀', 'label': 'Basketball',
     'leagues': [('NBA', '4387'), ('EuroLeague', '4470'), ('WNBA', '4516')]},
    {'key': 'tennis', 'icon': '🎾', 'label': 'Tennis',
     'leagues': [('ATP World Tour', '4489'), ('WTA Tour', '4490'),
                 ('Australian Open', '4498'), ('French Open', '4499'),
                 ('Wimbledon', '4500'), ('US Open', '4501')]},
    {'key': 'motorsport', 'icon': '🏎️', 'label': 'Motorsport',
     'leagues': [('Formula 1', '4370'), ('Formula E', '4371'), ('MotoGP', '4407'),
                 ('NASCAR Cup Series', '4376'), ('IndyCar Series', '4373'),
                 ('BTCC', '4374'), ('British Superbikes', '4375'),
                 ('Dakar Rally', '4379'), ('World Rally Championship', '4409')]},
    {'key': 'golf', 'icon': '⛳', 'label': 'Golf',
     'leagues': [('PGA Tour', '4425'), ('DP World Tour', '4426'),
                 ('LIV Golf', '4753'), ('LPGA Tour', '4422')]},
    {'key': 'american_football', 'icon': '🏈', 'label': 'American Football',
     'leagues': [('NFL', '4391'), ('NCAA Football', '4479')]},
    {'key': 'baseball', 'icon': '⚾', 'label': 'Baseball',
     'leagues': [('MLB', '4424')]},
    {'key': 'ice_hockey', 'icon': '🏒', 'label': 'Ice Hockey',
     'leagues': [('NHL', '4380')]},
]

SPORT_MAP = {s['key']: s for s in SPORT_CATEGORIES}

INTL_FOOTBALL_LEAGUES = [
    ('Bundesliga', '4331'), ('Serie A', '4332'), ('La Liga', '4335'), ('Ligue 1', '4334'),
    ('Eredivisie', '4337'), ('Champions League', '4480'), ('Europa League', '4481'),
    ('FIFA World Cup', '4429'), ('FIFA Club World Cup', '4503'),
]

SPORT_UK_BROADCASTERS = {}

SEASON_FALLBACK_SPORTS = {'snooker', 'darts', 'golf', 'motorsport', 'tennis', 'combat'}

FORM_SUPPORTED_SPORTS = {'football', 'basketball', 'american_football', 'baseball', 'ice_hockey', 'rugby', 'cricket'}

LIVESCORE_UK_LEAGUES = [
    ('Premier League', '4328'), ('Championship', '4329'), ('League One', '4396'),
    ('League Two', '4397'), ('FA Cup', '4482'), ('EFL Cup', '4483'), ('Scottish Premiership', '4330'),
]


def find_league_name(sport_key, league_id):
    sport = SPORT_MAP.get(sport_key)
    if sport:
        for name, lid in sport.get('leagues', []):
            if lid == league_id:
                return name
    for name, lid in INTL_FOOTBALL_LEAGUES:
        if lid == league_id:
            return name
    return league_id


def find_sport_key_for_league(league_id):
    for sport in SPORT_CATEGORIES:
        for _, lid in sport.get('leagues', []):
            if lid == league_id:
                return sport['key']
    for _, lid in INTL_FOOTBALL_LEAGUES:
        if lid == league_id:
            return 'football'
    return None


# ===========================================================================
# Database operations
# ===========================================================================

def is_authorized(user_id):
    with db_cursor() as cur:
        cur.execute('SELECT 1 FROM authorized_users WHERE user_id = %s', (str(user_id),))
        return cur.fetchone() is not None


def redeem_code(code, user_id, username, first_name):
    code = code.strip().upper()
    with db_cursor() as cur:
        cur.execute('SELECT 1 FROM authorized_users WHERE user_id = %s', (str(user_id),))
        if cur.fetchone():
            return False, '✅ You\'re already authorized. Tap /start to begin.'

        cur.execute('SELECT 1 FROM blacklist WHERE user_id = %s', (str(user_id),))
        if cur.fetchone():
            return False, '❌ Your account is blocked.'

        cur.execute('SELECT expires_at FROM access_codes WHERE code = %s', (code,))
        row = cur.fetchone()
        if not row:
            return False, '❌ Invalid code. Please check and try again.\n\nFormat: <code>/code SPORTXXXXXXXXXX</code>'

        expires_at = row[0]
        if expires_at < utc_now():
            cur.execute('DELETE FROM access_codes WHERE code = %s', (code,))
            return False, '❌ This code has expired. Contact the bot owner for a new one.'

        # SUCCESS - Authorize user and DELETE the code (one-time use, permanently deleted)
        cur.execute(
            'INSERT INTO authorized_users (user_id, username, first_name, code_used, redeemed_at) VALUES (%s, %s, %s, %s, %s)',
            (str(user_id), username or '', first_name or '', code, utc_now())
        )
        cur.execute('DELETE FROM access_codes WHERE code = %s', (code,))
        return True, '✅ Code accepted! You now have access.\n\nTap /start to begin.'


def is_blacklisted(user_id):
    with db_cursor() as cur:
        cur.execute('SELECT 1 FROM blacklist WHERE user_id = %s', (str(user_id),))
        return cur.fetchone() is not None


def blacklist_user(user_id, reason, blocked_by):
    now = utc_now()
    with db_cursor() as cur:
        cur.execute(
            'INSERT INTO blacklist (user_id, reason, blocked_at, blocked_by) VALUES (%s, %s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET reason = %s, blocked_at = %s',
            (str(user_id), reason, now, str(blocked_by), reason, now)
        )
        cur.execute('DELETE FROM authorized_users WHERE user_id = %s', (str(user_id),))


def unblacklist_user(user_id):
    with db_cursor() as cur:
        cur.execute('DELETE FROM blacklist WHERE user_id = %s', (str(user_id),))


def check_rate_limit(user_id):
    with db_cursor() as cur:
        cur.execute('SELECT blocked_until FROM rate_limits WHERE user_id = %s', (str(user_id),))
        row = cur.fetchone()
    now = utc_now()
    if row and row[0] and now < row[0]:
        mins = int((row[0] - now).total_seconds() / 60) + 1
        return True, mins
    return False, 0


def record_failed_attempt(user_id):
    now = utc_now()
    cutoff = now - timedelta(minutes=RATE_LIMIT_WINDOW_MINS)
    with db_cursor() as cur:
        cur.execute('SELECT fails FROM rate_limits WHERE user_id = %s', (str(user_id),))
        row = cur.fetchone()

        fails = []
        if row and row[0]:
            parsed = []
            for ts in row[0].split('|'):
                if not ts:
                    continue
                dt = datetime.fromisoformat(ts)
                if dt.tzinfo is None:
                    dt = pytz.utc.localize(dt)
                parsed.append(dt)
            fails = [ts for ts in parsed if ts > cutoff]
        fails.append(now)

        is_blocked = False
        blocked_until = None
        if len(fails) >= RATE_LIMIT_MAX_FAILS:
            blocked_until = now + timedelta(hours=RATE_LIMIT_BLOCK_HOURS)
            is_blocked = True

        fails_str = '|'.join(ts.isoformat() for ts in fails)
        cur.execute(
            'INSERT INTO rate_limits (user_id, fails, blocked_until) VALUES (%s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET fails = %s, blocked_until = %s',
            (str(user_id), fails_str, blocked_until, fails_str, blocked_until)
        )
    return is_blocked, len(fails)


def audit_log_event(event_type, user_id, username='', first_name='', detail=''):
    with db_cursor() as cur:
        cur.execute(
            'INSERT INTO audit_log (timestamp, event_type, user_id, username, first_name, detail) VALUES (%s, %s, %s, %s, %s, %s)',
            (utc_now(), event_type, str(user_id), username or '', first_name or '', detail)
        )


def update_activity(user_id):
    now = utc_now()
    with db_cursor() as cur:
        cur.execute(
            'INSERT INTO user_activity (user_id, last_active) VALUES (%s, %s) ON CONFLICT (user_id) DO UPDATE SET last_active = %s',
            (str(user_id), now, now)
        )


def is_inactive(user_id):
    with db_cursor() as cur:
        cur.execute('SELECT last_active FROM user_activity WHERE user_id = %s', (str(user_id),))
        row = cur.fetchone()
    if not row:
        return True
    hours_since = (utc_now() - row[0]).total_seconds() / 3600
    return hours_since >= INACTIVITY_RESET_HOURS


def get_match_alerts(event_id, user_id):
    with db_cursor() as cur:
        cur.execute('SELECT windows FROM match_alerts WHERE event_id = %s AND user_id = %s', (str(event_id), str(user_id)))
        row = cur.fetchone()
    if not row:
        return []
    return row[0].split(',') if row[0] else []


def set_match_alerts(event_id, user_id, windows, event):
    with db_cursor() as cur:
        if not windows:
            cur.execute('DELETE FROM match_alerts WHERE event_id = %s AND user_id = %s', (str(event_id), str(user_id)))
        else:
            home = event.get('strHomeTeam') or event.get('strEvent') or 'TBC'
            away = event.get('strAwayTeam') or ''
            matchup = f'{home} vs {away}' if away else home
            event_dt = event_utc_datetime(event)
            cur.execute(
                'INSERT INTO match_alerts (event_id, user_id, windows, matchup, league, venue, sport_key, datetime_utc) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (event_id, user_id) DO UPDATE SET windows = %s, matchup = %s, league = %s, venue = %s, sport_key = %s, datetime_utc = %s',
                (str(event_id), str(user_id), ','.join(windows), matchup, event.get('strLeague', ''),
                 event.get('strVenue', '') or event.get('strCircuit', ''),
                 find_sport_key_for_league(event.get('idLeague', '')) or '', event_dt,
                 ','.join(windows), matchup, event.get('strLeague', ''),
                 event.get('strVenue', '') or event.get('strCircuit', ''),
                 find_sport_key_for_league(event.get('idLeague', '')) or '', event_dt)
            )


def get_all_user_alerts(user_id):
    with db_cursor(dict_cursor=True) as cur:
        cur.execute('SELECT * FROM match_alerts WHERE user_id = %s ORDER BY datetime_utc', (str(user_id),))
        return cur.fetchall()


def get_all_pending_alerts():
    with db_cursor(dict_cursor=True) as cur:
        cur.execute('SELECT * FROM match_alerts WHERE datetime_utc > %s', (utc_now() - timedelta(hours=1),))
        return cur.fetchall()


def is_already_notified(event_id, user_id, window):
    notified_key = f'{event_id}:{user_id}:{window}'
    with db_cursor() as cur:
        cur.execute('SELECT 1 FROM notified_matches WHERE notified_key = %s', (notified_key,))
        return cur.fetchone() is not None


def mark_notified(event_id, user_id, window):
    notified_key = f'{event_id}:{user_id}:{window}'
    with db_cursor() as cur:
        cur.execute(
            'INSERT INTO notified_matches (notified_key, notified_at) VALUES (%s, %s) ON CONFLICT (notified_key) DO NOTHING',
            (notified_key, utc_now())
        )
        cur.execute('DELETE FROM notified_matches WHERE notified_at < %s', (utc_now() - timedelta(hours=24),))


# ===========================================================================
# API
# ===========================================================================

_session = requests.Session()


def _get(url, headers=None, max_retries=3):
    """GET with retry + exponential backoff. Retries on 429 and 5xx. Reuses a shared session."""
    backoff = 1.0
    for attempt in range(1, max_retries + 1):
        log.info('➡️  %s (attempt %d)', url, attempt)
        try:
            response = _session.get(url, headers=headers, timeout=REQ_TIMEOUT)
            # Retry only on rate limit and known transient server errors
            if response.status_code in (429, 500, 502, 503, 504):
                # Respect Retry-After header if present
                retry_after = response.headers.get('Retry-After')
                wait = float(retry_after) if retry_after and retry_after.isdigit() else backoff
                log.warning('API %s on %s — waiting %.1fs then retrying', response.status_code, url, wait)
                if attempt < max_retries:
                    time.sleep(wait)
                    backoff *= 2
                    continue
                return None
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as exc:
            log.warning('API error (attempt %d): %s', attempt, exc)
            if attempt < max_retries:
                time.sleep(backoff)
                backoff *= 2
                continue
            return None
    return None


def get_next_events_by_league(league_id):
    return (_get(f'{V1_BASE}/eventsnextleague.php?id={league_id}') or {}).get('events') or []


def get_season_events(league_id, season):
    return (_get(f'{V1_BASE}/eventsseason.php?id={league_id}&s={season}') or {}).get('events') or []


def get_league_details(league_id):
    data = _get(f'{V1_BASE}/lookupleague.php?id={league_id}')
    leagues = (data or {}).get('leagues') or []
    return leagues[0] if leagues else {}


def lookup_event(event_id):
    data = _get(f'{V1_BASE}/lookupevent.php?id={event_id}')
    events = (data or {}).get('events') or []
    return events[0] if events else None


def get_last_team_events(team_id):
    if not team_id:
        return []
    data = _get(f'{V1_BASE}/eventslast.php?id={team_id}')
    return (data or {}).get('results') or []


_livescore_cache = {}


def get_livescores(sport_slug):
    now = datetime.now()
    if sport_slug in _livescore_cache:
        fetched_at, scores = _livescore_cache[sport_slug]
        if now - fetched_at < timedelta(minutes=LIVE_SCORES_CACHE_MINUTES):
            return scores
    data = _get(f'{V2_BASE}/livescore/{sport_slug}', V2_HEADERS)
    scores = (data or {}).get('livescore') or []
    _livescore_cache[sport_slug] = (now, scores)
    return scores


_form_cache = {}


def get_team_form(team_id, team_name):
    if not team_id:
        return []
    now = datetime.now()
    if team_id in _form_cache:
        fetched_at, form = _form_cache[team_id]
        if now - fetched_at < timedelta(hours=FORM_CACHE_EXPIRY_HOURS):
            return form
    events = get_last_team_events(team_id)
    form = []
    team_lower = (team_name or '').lower().strip()
    for event in events[:5]:
        home = (event.get('strHomeTeam') or '').lower().strip()
        away = (event.get('strAwayTeam') or '').lower().strip()
        hs = event.get('intHomeScore')
        as_ = event.get('intAwayScore')
        if hs is None or as_ is None or hs == '' or as_ == '':
            continue
        try:
            hs, as_ = int(hs), int(as_)
        except (ValueError, TypeError):
            continue
        is_home = home == team_lower or team_lower in home
        is_away = away == team_lower or team_lower in away
        if not (is_home or is_away):
            continue
        if is_home:
            form.append('W' if hs > as_ else ('L' if hs < as_ else 'D'))
        else:
            form.append('W' if as_ > hs else ('L' if as_ < hs else 'D'))
    _form_cache[team_id] = (now, form)
    return form


def format_form_string(form):
    if not form:
        return '<i>No recent form available</i>'
    icon_map = {'W': '🟢', 'D': '⚪', 'L': '🔴'}
    icons = ' '.join(icon_map.get(letter, '·') for letter in form)
    letters = ' '.join(form)
    return f'{icons}  <code>{letters}</code>'


_tv_event_cache = {}


def get_tv_for_event(event_id):
    if not event_id:
        return []
    if event_id in _tv_event_cache:
        return _tv_event_cache[event_id]
    data = _get(f'{V1_BASE}/lookuptv.php?id={event_id}')
    if not data:
        _tv_event_cache[event_id] = []
        return []
    entries = data.get('tvevent') or data.get('tvs') or data.get('tv') or []
    results = []
    for entry in entries:
        channel = entry.get('strChannel') or entry.get('strChannelName', '')
        country = entry.get('strCountry') or 'Unknown'
        if channel:
            results.append((country, channel))
    _tv_event_cache[event_id] = results
    return results


def get_tv_with_fallback(event, sport_key=''):
    """Pulls REAL broadcaster data from TheSportsDB API only. No fallbacks, no guesses."""
    event_id = event.get('idEvent', '')
    tv_list = []
    # First check if event has strTVStation directly
    str_tv = (event.get('strTVStation') or '').strip()
    if str_tv:
        country = event.get('strCountry') or 'United Kingdom'
        tv_list.append((country, str_tv))
    # Then query lookuptv.php for full broadcaster data
    api_tv = get_tv_for_event(event_id)
    for entry in api_tv:
        if entry not in tv_list:
            tv_list.append(entry)
    return tv_list


_league_events_cache = {}


def filter_upcoming_only(events):
    now_utc = utc_now()
    cutoff = now_utc - timedelta(hours=HIDE_PAST_MATCHES_AFTER_HOURS)
    return [e for e in events if (event_utc_datetime(e) or now_utc) > cutoff]


def cached_league_events(league_id):
    now = datetime.now()
    entry = _league_events_cache.get(league_id)
    if entry is not None:
        fetched_at, events = entry
        if now - fetched_at < timedelta(hours=CACHE_EXPIRY_HOURS):
            return filter_upcoming_only(events)
    events = get_next_events_by_league(league_id)
    if not events:
        sport_key = find_sport_key_for_league(league_id)
        if sport_key in SEASON_FALLBACK_SPORTS:
            league_info = get_league_details(league_id)
            season = league_info.get('strCurrentSeason') or ''
            if season:
                events = get_season_events(league_id, season) or []
            if not events:
                current_year = datetime.now().year
                for try_season in [str(current_year), str(current_year - 1),
                                   f'{current_year - 1}-{current_year}',
                                   f'{current_year}-{current_year + 1}']:
                    events = get_season_events(league_id, try_season) or []
                    if events:
                        break
    events.sort(key=lambda e: (e.get('dateEvent', ''), e.get('strTime', '')))
    _league_events_cache[league_id] = (now, events)
    return filter_upcoming_only(events)


def group_events_by_date(events):
    grouped = defaultdict(list)
    for event in events:
        ev_dt = event_utc_datetime(event)
        if not ev_dt:
            continue
        local_date = ev_dt.astimezone(LOCAL_TZ).date()
        grouped[local_date].append(event)
    return dict(grouped)


# ===========================================================================
# Formatting
# ===========================================================================

def fmt_dt(date_str, time_str):
    if not date_str:
        return 'TBD'
    try:
        raw = (time_str or '').replace('+00:00', '')[:8] or '00:00:00'
        utc_dt = pytz.utc.localize(datetime.strptime(f'{date_str} {raw}', '%Y-%m-%d %H:%M:%S'))
        local_dt = utc_dt.astimezone(LOCAL_TZ)
        zone = 'BST' if local_dt.dst().seconds else 'GMT'
        return local_dt.strftime(f'%a %d %b %Y · %H:%M {zone}')
    except Exception:
        return date_str


def fmt_short_date(date_str, time_str):
    if not date_str:
        return ''
    try:
        raw = (time_str or '').replace('+00:00', '')[:8] or '00:00:00'
        utc_dt = pytz.utc.localize(datetime.strptime(f'{date_str} {raw}', '%Y-%m-%d %H:%M:%S'))
        return utc_dt.astimezone(LOCAL_TZ).strftime('%H:%M')
    except Exception:
        return ''


def fmt_uk_full(utc_dt):
    if not utc_dt:
        return 'TBD'
    if utc_dt.tzinfo is None:
        utc_dt = pytz.utc.localize(utc_dt)
    local_dt = utc_dt.astimezone(LOCAL_TZ)
    zone = 'BST' if local_dt.dst().seconds else 'GMT'
    return local_dt.strftime(f'%a %d %b %Y · %H:%M {zone}')


def fmt_date_label(d):
    today = datetime.now(LOCAL_TZ).date()
    tomorrow = today + timedelta(days=1)
    if d == today:
        return 'Today'
    if d == tomorrow:
        return 'Tomorrow'
    return d.strftime('%a %d %b')


def event_utc_datetime(event):
    date_str = event.get('dateEvent', '')
    time_str = event.get('strTime', '')
    if not date_str:
        return None
    try:
        raw = (time_str or '').replace('+00:00', '')[:8] or '00:00:00'
        return pytz.utc.localize(datetime.strptime(f'{date_str} {raw}', '%Y-%m-%d %H:%M:%S'))
    except Exception:
        return None


def short_team(name, limit=10):
    if not name:
        return ''
    if len(name) <= limit:
        return name
    swaps = {'Manchester': 'Man', 'Tottenham Hotspur': 'Spurs', 'Newcastle United': 'Newcastle',
             'Wolverhampton Wanderers': 'Wolves', 'Brighton & Hove Albion': 'Brighton',
             'West Ham United': 'West Ham', 'Nottingham Forest': "Nott'm F",
             'AFC Bournemouth': 'Bournemouth'}
    for full, abbr in swaps.items():
        if full in name:
            name = name.replace(full, abbr)
    return name if len(name) <= limit else name[:limit - 1] + '…'


def format_country(country):
    return {'United Kingdom': 'UK', 'United States': 'USA',
            'United Arab Emirates': 'UAE', 'The Netherlands': 'Netherlands'}.get(country, country)


def group_channels_vertical(tv_list):
    uk_entries, other_entries = [], []
    seen = set()
    for country, channel in tv_list:
        if channel in seen:
            continue
        seen.add(channel)
        if country in ('United Kingdom', 'UK', 'England'):
            uk_entries.append((format_country(country), channel))
        else:
            other_entries.append((format_country(country), channel))
    other_entries.sort(key=lambda x: x[0])
    all_entries = uk_entries + other_entries
    grouped, country_order = {}, []
    for country, channel in all_entries:
        if country not in grouped:
            grouped[country] = []
            country_order.append(country)
        grouped[country].append(channel)
    if not country_order:
        return []
    max_len = max(len(c) for c in country_order)
    lines = []
    for country in country_order:
        channels = grouped[country]
        lines.append(f'<code>{country.rjust(max_len)}</code>  {channels[0]}')
        for ch in channels[1:]:
            lines.append(f'<code>{" " * max_len}</code>  {ch}')
    return lines


def match_card(event, user_id=None, sport_key=''):
    home = event.get('strHomeTeam') or event.get('strEvent') or 'TBC'
    away = event.get('strAwayTeam') or ''
    home_id = event.get('idHomeTeam') or ''
    away_id = event.get('idAwayTeam') or ''
    matchup = f'{home} vs {away}' if away else home
    venue = event.get('strVenue') or event.get('strCircuit') or 'Venue TBC'
    league = event.get('strLeague') or ''
    event_id = event.get('idEvent', '')
    dt = fmt_dt(event.get('dateEvent', ''), event.get('strTime', ''))
    tv_list = get_tv_with_fallback(event, sport_key)
    sep = '━━━━━━━━━━━━━━━━━━━━'
    lines = [sep, f'🏆 <b>{matchup}</b>']
    if league:
        lines.append(f'🎯 <b>{league}</b>')
    lines.append(sep)
    lines.append(f'🕒 {dt}')
    lines.append(f'📍 {venue}')
    if user_id is not None:
        user_alerts = get_match_alerts(event_id, user_id)
        if user_alerts:
            alert_str = ', '.join(f'{w} min' for w in sorted(user_alerts, key=int))
            lines.append(f'🔔 Alerts: <b>{alert_str} before kickoff</b>')
        else:
            lines.append('🔕 No alerts set')
    if sport_key in FORM_SUPPORTED_SPORTS and home and away:
        lines.append('')
        lines.append('📊 <b>Recent Form (last 5)</b>')
        lines.append(sep)
        home_form = get_team_form(home_id, home) if home_id else []
        away_form = get_team_form(away_id, away) if away_id else []
        max_len = max(len(short_team(home, 14)), len(short_team(away, 14)))
        lines.append(f'<code>{short_team(home, 14).ljust(max_len)}</code>  {format_form_string(home_form)}')
        lines.append(f'<code>{short_team(away, 14).ljust(max_len)}</code>  {format_form_string(away_form)}')
    lines.append('')
    lines.append('📺 <b>Broadcasters</b>')
    lines.append(sep)
    channel_lines = group_channels_vertical(tv_list)
    if channel_lines:
        lines.extend(channel_lines)
    else:
        lines.append('<i>No broadcast info available yet.</i>')
    return '\n'.join(lines)


def chunks(text, max_len=4096):
    if len(text) <= max_len:
        return [text]
    parts, current = [], ''
    for line in text.split('\n'):
        if len(current) + len(line) + 1 > max_len:
            parts.append(current)
            current = line
        else:
            current += ('\n' if current else '') + line
    if current:
        parts.append(current)
    return parts


# ===========================================================================
# Keyboards
# ===========================================================================

def welcome_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton('🏆 Sports Schedule — Click Here', callback_data='menu:main')]])


def main_menu_keyboard():
    rows = [[InlineKeyboardButton('🔴 Live Scores', callback_data='live:menu'),
             InlineKeyboardButton('🔔 My Alerts', callback_data='my_alerts')]]
    row = []
    for sport in SPORT_CATEGORIES:
        row.append(InlineKeyboardButton(f"{sport['icon']} {sport['label']}", callback_data=f"sport:{sport['key']}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(rows)


def alert_main_menu_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton('🏅 Main Menu', callback_data='menu:main')]])


def league_keyboard(sport_key):
    sport = SPORT_MAP.get(sport_key)
    if not sport:
        return main_menu_keyboard()
    rows, row = [], []
    for league_name, league_id in sport['leagues']:
        if league_id == 'INTL':
            rows.append([InlineKeyboardButton(league_name, callback_data='intl_football_menu')])
            continue
        row.append(InlineKeyboardButton(league_name, callback_data=f"league:{sport_key}:{league_id}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton('◀ Back', callback_data='menu:main'),
                 InlineKeyboardButton('🏅 Main Menu', callback_data='menu:main')])
    return InlineKeyboardMarkup(rows)


def intl_football_keyboard():
    rows, row = [], []
    for league_name, league_id in INTL_FOOTBALL_LEAGUES:
        row.append(InlineKeyboardButton(league_name, callback_data=f'league:football:{league_id}'))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton('◀ Back to Football', callback_data='sport:football'),
                 InlineKeyboardButton('🏅 Main Menu', callback_data='menu:main')])
    return InlineKeyboardMarkup(rows)


def date_picker_keyboard(sport_key, league_id, events):
    grouped = group_events_by_date(events)
    rows = []
    for d in sorted(grouped.keys()):
        count = len(grouped[d])
        label = f'{fmt_date_label(d)}  ·  {count} match{"es" if count != 1 else ""}'
        rows.append([InlineKeyboardButton(label, callback_data=f'date:{sport_key}:{league_id}:{d.strftime("%Y-%m-%d")}')])
    rows.append([InlineKeyboardButton('📋 Show all upcoming', callback_data=f'date:{sport_key}:{league_id}:all')])
    rows.append([InlineKeyboardButton('◀ Back', callback_data=f'sport:{sport_key}'),
                 InlineKeyboardButton('🏅 Main Menu', callback_data='menu:main')])
    return InlineKeyboardMarkup(rows)


def league_matches_keyboard(sport_key, league_id, events, page=0, per_page=10, date_filter='all'):
    total = len(events)
    page_events = events[page * per_page:(page + 1) * per_page]
    rows = []
    for event in page_events:
        event_id = event.get('idEvent', '')
        if not event_id:
            continue
        home = short_team(event.get('strHomeTeam') or '')
        away = short_team(event.get('strAwayTeam') or '')
        matchup = f'{home} vs {away}' if home and away else event.get('strEvent', 'Match')[:28]
        time_bit = fmt_short_date(event.get('dateEvent', ''), event.get('strTime', ''))
        label = f'{matchup}  ·  {time_bit}' if time_bit else matchup
        if len(label) > 40:
            label = label[:38] + '…'
        rows.append([InlineKeyboardButton(label, callback_data=f'match:{sport_key}:{league_id}:{event_id}')])
    pag = []
    if page > 0:
        pag.append(InlineKeyboardButton('◀ Prev', callback_data=f'matches_page:{sport_key}:{league_id}:{date_filter}:{page - 1}'))
    if (page + 1) * per_page < total:
        pag.append(InlineKeyboardButton('Next ▶', callback_data=f'matches_page:{sport_key}:{league_id}:{date_filter}:{page + 1}'))
    if pag:
        rows.append(pag)
    rows.append([InlineKeyboardButton('◀ Back', callback_data=f'league:{sport_key}:{league_id}'),
                 InlineKeyboardButton('🏅 Main Menu', callback_data='menu:main')])
    return InlineKeyboardMarkup(rows)


def match_detail_keyboard(sport_key, league_id, event_id, user_id):
    user_alerts = get_match_alerts(event_id, user_id)

    def check(o):
        return '✅ ' if o in user_alerts else ''

    rows = [[InlineKeyboardButton(f'{check("15")}🔔 Alert 15 min', callback_data=f'alert:toggle:{event_id}:15:{sport_key}:{league_id}'),
             InlineKeyboardButton(f'{check("30")}🔔 Alert 30 min', callback_data=f'alert:toggle:{event_id}:30:{sport_key}:{league_id}')]]
    if user_alerts:
        rows.append([InlineKeyboardButton('🔕 Clear alerts', callback_data=f'alert:clear:{event_id}:{sport_key}:{league_id}')])
    rows.append([InlineKeyboardButton('◀ Back', callback_data=f'date:{sport_key}:{league_id}:all'),
                 InlineKeyboardButton('🏅 Main Menu', callback_data='menu:main')])
    return InlineKeyboardMarkup(rows)


def live_scores_menu_keyboard():
    rows = [[InlineKeyboardButton('⚽ All UK Football (live)', callback_data='live:show:all_football')]]
    for league_name, league_id in LIVESCORE_UK_LEAGUES:
        rows.append([InlineKeyboardButton(f'⚽ {league_name}', callback_data=f'live:show:{league_id}')])
    rows.append([InlineKeyboardButton('◀ Back', callback_data='menu:main'),
                 InlineKeyboardButton('🏅 Main Menu', callback_data='menu:main')])
    return InlineKeyboardMarkup(rows)


def live_scores_back_keyboard(league_id=''):
    rows = []
    if league_id:
        rows.append([InlineKeyboardButton('🔄 Refresh', callback_data=f'live:show:{league_id}')])
    rows.append([InlineKeyboardButton('◀ Back', callback_data='live:menu'),
                 InlineKeyboardButton('🏅 Main Menu', callback_data='menu:main')])
    return InlineKeyboardMarkup(rows)


def my_alerts_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton('🏅 Main Menu', callback_data='menu:main')]])


def build_welcome_message():
    today = datetime.now(LOCAL_TZ).strftime('%A %d %B')
    sep = '━━━━━━━━━━━━━━━━━━━━'
    return (f'🗓️ ⚽\n\n<b>SPORTSCAST UK</b>\n{sep}\n<i>{today}</i>\n\n'
            f'Welcome back to your premium\nsports schedule.\n\n'
            f'• All UK & World Sport\n• Live Scores\n• Match Alerts\n• UK Broadcaster Info\n'
            f'{sep}\n\n<b>Tap below to begin</b> 👇')


def build_main_menu_message():
    today = datetime.now(LOCAL_TZ).strftime('%A %d %B')
    return f'🏆 <b>SportsCast UK</b>  <i>{today}</i>\n\nAll times in UK time.'


def build_alert_confirmation(event, windows):
    home = event.get('strHomeTeam') or event.get('strEvent') or 'TBC'
    away = event.get('strAwayTeam') or ''
    matchup = f'{home} vs {away}' if away else home
    league = event.get('strLeague') or ''
    event_dt = event_utc_datetime(event)
    uk_time = fmt_uk_full(event_dt) if event_dt else 'Time TBD'
    sep = '━━━━━━━━━━━━━━━━━━━━'
    if windows:
        alert_str = ', '.join(f'<b>{w} min</b>' for w in sorted(windows, key=int))
        title = f'✅ Alert set for {matchup}'
        body = f'You\'ll be reminded <b>{alert_str}</b> before kickoff.\n\n🕒 <b>Kickoff (UK time):</b>\n{uk_time}'
    else:
        title = f'🔕 Alerts cleared for {matchup}'
        body = 'No more reminders will be sent for this match.'
    return f'{sep}\n{title}\n{sep}\n🏆 <b>{matchup}</b>\n' + (f'🎯 {league}\n' if league else '') + f'\n{body}'


def build_my_alerts_message(user_id):
    alerts = get_all_user_alerts(user_id)
    now_utc = utc_now()
    user_matches = []
    for a in alerts:
        dt = a.get('datetime_utc')
        if dt:
            if dt.tzinfo is None:
                dt = pytz.utc.localize(dt)
            if dt < now_utc:
                continue
            user_matches.append((dt, a.get('matchup', '?'), a.get('league', ''),
                                 fmt_uk_full(dt), (a.get('windows', '') or '').split(',')))
    user_matches.sort(key=lambda x: x[0])
    if not user_matches:
        return '🔔 <b>My Match Alerts</b>\n\n<i>You have no active alerts set.</i>'
    lines = ['🔔 <b>My Match Alerts</b>', '']
    for _, matchup, league, dt_display, windows in user_matches:
        alert_str = ', '.join(f'{w} min' for w in sorted(windows, key=int))
        lines.append(f'🏆 <b>{matchup}</b>')
        if league:
            lines.append(f'🎯 {league}')
        lines.append(f'🕒 {dt_display}')
        lines.append(f'🔔 <b>Alerts:</b> {alert_str}')
        lines.append('━━━━━━━━━━━━━━━━━━━━')
    return '\n'.join(lines)


def build_live_scores_message(league_id):
    scores = get_livescores('soccer')
    now_str = datetime.now(LOCAL_TZ).strftime('%H:%M %Z')
    sep = '━━━━━━━━━━━━━━━━━━━━'
    uk_ids = {lid for _, lid in LIVESCORE_UK_LEAGUES}
    if league_id == 'all_football':
        filtered = [s for s in scores if str(s.get('idLeague', '')) in uk_ids]
        title_label = 'All UK Football'
    else:
        filtered = [s for s in scores if str(s.get('idLeague', '')) == league_id]
        title_label = find_league_name('football', league_id)
    lines = [f'🔴 <b>LIVE — ⚽ {title_label}</b>', f'<i>As of {now_str}</i>', sep]
    if not filtered:
        lines.append('\n<i>No matches currently live.</i>')
        return '\n'.join(lines)
    for s in filtered[:20]:
        home = s.get('strHomeTeam') or '?'
        away = s.get('strAwayTeam') or '?'
        lines.append('')
        lines.append(f'<b>{home} {s.get("intHomeScore", "-")} - {s.get("intAwayScore", "-")} {away}</b>')
        if s.get('strProgress'):
            lines.append(f'⏱️ {s["strProgress"]}')
    return '\n'.join(lines)


async def safe_edit(query, text, reply_markup=None):
    try:
        if len(text) > 4096:
            for part in chunks(text):
                await query.message.chat.send_message(text=part, parse_mode=ParseMode.HTML)
            if reply_markup is not None:
                await query.message.chat.send_message(text='⬇️', reply_markup=reply_markup)
            return
        await query.edit_message_text(text=text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
    except BadRequest as exc:
        if 'not modified' in str(exc).lower():
            return
        await query.message.chat.send_message(text=text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)


_user_match_context = {}


async def cmd_code(update, context):
    user = update.effective_user
    user_id = user.id
    if is_blacklisted(user_id):
        await update.message.reply_text('❌ Your account is blocked.')
        return
    if is_authorized(user_id):
        await update.message.reply_text('✅ You\'re already authorized! Tap /start to begin.')
        return
    blocked, mins = check_rate_limit(user_id)
    if blocked:
        await update.message.reply_text(f'⚠️ Too many failed attempts. Try again in {mins} minutes.')
        return
    args = context.args
    if not args:
        await update.message.reply_text(
            '📝 <b>To redeem your code:</b>\n\nPaste the line your seller sent you:\n<code>/code SPORTXXXXXXXXXX</code>',
            parse_mode=ParseMode.HTML)
        return
    success, message = redeem_code(args[0], user_id, user.username or '', user.first_name or '')
    if success:
        audit_log_event('code_success', user_id, user.username, user.first_name, args[0])
        await update.message.reply_text(message, parse_mode=ParseMode.HTML)
    else:
        is_now_blocked, fails = record_failed_attempt(user_id)
        audit_log_event('code_failed', user_id, user.username, user.first_name, f'{args[0]} (fails: {fails})')
        if is_now_blocked:
            await update.message.reply_text(f'⚠️ Too many failed attempts. Blocked for {RATE_LIMIT_BLOCK_HOURS} hours.')
        else:
            remaining = RATE_LIMIT_MAX_FAILS - fails
            await update.message.reply_text(f'{message}\n\n<i>Attempts remaining: {remaining}</i>', parse_mode=ParseMode.HTML)


async def cmd_start(update, context):
    user = update.effective_user
    if is_blacklisted(user.id):
        await update.message.reply_text('❌ Your account is blocked.')
        return
    if not is_authorized(user.id):
        await update.message.reply_text(
            '🔒 <b>Access Required</b>\n\n'
            'To redeem your code, copy the line your seller sent you:\n'
            '<code>/code SPORTXXXXXXXXXX</code>',
            parse_mode=ParseMode.HTML)
        return
    audit_log_event('start', user.id, user.username, user.first_name, '')
    show_welcome = is_inactive(user.id)
    update_activity(user.id)
    if show_welcome:
        await update.message.reply_text(build_welcome_message(), parse_mode=ParseMode.HTML, reply_markup=welcome_keyboard())
    else:
        await update.message.reply_text(build_main_menu_message(), parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())


async def cmd_help(update, context):
    await update.message.reply_text(
        '📖 <b>SportsCast UK</b>\n\n/start — Open bot\n/code SPORTXXXX — Redeem access code\n/help — This message',
        parse_mode=ParseMode.HTML)


async def cmd_blacklist(update, context):
    if update.effective_user.id not in ADMIN_USER_IDS:
        return
    if not context.args:
        await update.message.reply_text('Usage: /blacklist <user_id> [reason]')
        return
    target = context.args[0]
    reason = ' '.join(context.args[1:]) if len(context.args) > 1 else 'No reason given'
    blacklist_user(target, reason, update.effective_user.id)
    await update.message.reply_text(f'✅ User {target} blacklisted.')


async def cmd_unblacklist(update, context):
    if update.effective_user.id not in ADMIN_USER_IDS:
        return
    if not context.args:
        await update.message.reply_text('Usage: /unblacklist <user_id>')
        return
    unblacklist_user(context.args[0])
    await update.message.reply_text(f'✅ User {context.args[0]} removed from blacklist.')


async def cmd_admin_stats(update, context):
    if update.effective_user.id not in ADMIN_USER_IDS:
        return
    with db_cursor() as cur:
        cur.execute('SELECT COUNT(*) FROM access_codes')
        codes_left = cur.fetchone()[0]
        cur.execute('SELECT COUNT(*) FROM authorized_users')
        authorized = cur.fetchone()[0]
        cur.execute('SELECT COUNT(*) FROM blacklist')
        blacklisted = cur.fetchone()[0]
    await update.message.reply_text(
        f'📊 <b>Admin Stats</b>\n\n<b>Codes remaining:</b> {codes_left}\n<b>Authorized users:</b> {authorized}\n<b>Blacklisted:</b> {blacklisted}',
        parse_mode=ParseMode.HTML)


async def button_handler(update, context):
    query = update.callback_query
    user = query.from_user
    if not is_authorized(user.id) or is_blacklisted(user.id):
        try:
            await query.answer('🔒 Access required. Send /code SPORTXXXX')
        except BadRequest:
            pass
        return
    update_activity(user.id)
    try:
        await query.answer()
    except BadRequest:
        pass
    data = query.data
    user_id = user.id

    if data == 'menu:main':
        await safe_edit(query, build_main_menu_message(), main_menu_keyboard())
        return
    if data == 'intl_football_menu':
        await safe_edit(query, '🌍 <b>International Football</b>\n\nChoose a league:', intl_football_keyboard())
        return
    if data.startswith('sport:'):
        key = data.split(':', 1)[1]
        sport = SPORT_MAP.get(key)
        if sport:
            await safe_edit(query, f"{sport['icon']} <b>{sport['label']}</b>\n\nChoose a league:", league_keyboard(key))
    elif data.startswith('league:'):
        _, sport_key, league_id = data.split(':', 2)
        league_name = find_league_name(sport_key, league_id)
        await safe_edit(query, f'⏳ Loading <b>{league_name}</b>…', None)
        events = cached_league_events(league_id)
        if not events:
            await safe_edit(query, f'🏆 <b>{league_name}</b>\n\n<i>No upcoming matches found.</i>', league_keyboard(sport_key))
            return
        await safe_edit(query, f'🏆 <b>{league_name}</b>\n\nChoose a day:', date_picker_keyboard(sport_key, league_id, events))
    elif data.startswith('date:'):
        _, sport_key, league_id, date_str = data.split(':', 3)
        league_name = find_league_name(sport_key, league_id)
        all_events = cached_league_events(league_id)
        if date_str == 'all':
            filtered = all_events
            header = f'🏆 <b>{league_name}</b>\n\nAll upcoming ({len(filtered)}):'
        else:
            try:
                target = datetime.strptime(date_str, '%Y-%m-%d').date()
                filtered = group_events_by_date(all_events).get(target, [])
                header = f'🏆 <b>{league_name}</b>\n📅 <b>{fmt_date_label(target)}</b>\n\n{len(filtered)} matches:'
            except ValueError:
                filtered = all_events
                header = f'🏆 <b>{league_name}</b>\n\nAll upcoming:'
        if not filtered:
            await safe_edit(query, f'🏆 <b>{league_name}</b>\n\n<i>No matches found.</i>', date_picker_keyboard(sport_key, league_id, all_events))
            return
        await safe_edit(query, header, league_matches_keyboard(sport_key, league_id, filtered, 0, date_filter=date_str))
    elif data.startswith('matches_page:'):
        _, sport_key, league_id, date_filter, page_str = data.split(':', 4)
        page = int(page_str)
        all_events = cached_league_events(league_id)
        if date_filter == 'all':
            filtered = all_events
        else:
            try:
                target = datetime.strptime(date_filter, '%Y-%m-%d').date()
                filtered = group_events_by_date(all_events).get(target, [])
            except ValueError:
                filtered = all_events
        league_name = find_league_name(sport_key, league_id)
        await safe_edit(query, f'🏆 <b>{league_name}</b>\n\n{len(filtered)} matches:',
                        league_matches_keyboard(sport_key, league_id, filtered, page, date_filter=date_filter))
    elif data.startswith('match:'):
        _, sport_key, league_id, event_id = data.split(':', 3)
        events = cached_league_events(league_id)
        event = next((e for e in events if e.get('idEvent') == event_id), None) or lookup_event(event_id)
        if not event:
            await safe_edit(query, '<i>Could not load match.</i>', None)
            return
        _user_match_context[user_id] = (sport_key, league_id)
        await safe_edit(query, '⏳ Loading match details…', None)
        await safe_edit(query, match_card(event, user_id, sport_key),
                        match_detail_keyboard(sport_key, league_id, event_id, user_id))
    elif data.startswith('alert:toggle:'):
        _, _, event_id, window, sport_key, league_id = data.split(':', 5)
        events = cached_league_events(league_id)
        event = next((e for e in events if e.get('idEvent') == event_id), None) or lookup_event(event_id)
        if not event:
            await safe_edit(query, '<i>Match not found.</i>', None)
            return
        current = get_match_alerts(event_id, user_id)
        if window in current:
            current.remove(window)
        else:
            current.append(window)
        set_match_alerts(event_id, user_id, current, event)
        await safe_edit(query, build_alert_confirmation(event, current),
                        match_detail_keyboard(sport_key, league_id, event_id, user_id))
    elif data.startswith('alert:clear:'):
        _, _, event_id, sport_key, league_id = data.split(':', 4)
        events = cached_league_events(league_id)
        event = next((e for e in events if e.get('idEvent') == event_id), None) or lookup_event(event_id) or {}
        set_match_alerts(event_id, user_id, [], event)
        await safe_edit(query, build_alert_confirmation(event, []),
                        match_detail_keyboard(sport_key, league_id, event_id, user_id))
    elif data == 'live:menu':
        await safe_edit(query, '🔴 <b>Live Scores</b>\n\nChoose:', live_scores_menu_keyboard())
    elif data.startswith('live:show:'):
        league_id = data.split(':', 2)[2]
        await safe_edit(query, '⏳ Loading live scores…', None)
        await safe_edit(query, build_live_scores_message(league_id), live_scores_back_keyboard(league_id))
    elif data == 'my_alerts':
        await safe_edit(query, build_my_alerts_message(user_id), my_alerts_keyboard())


async def check_upcoming_matches(app):
    while True:
        try:
            alerts = get_all_pending_alerts()
            now_utc = utc_now()
            for a in alerts:
                event_id = a['event_id']
                user_id = a['user_id']
                event_dt = a.get('datetime_utc')
                if not event_dt:
                    continue
                if event_dt.tzinfo is None:
                    event_dt = pytz.utc.localize(event_dt)
                minutes_to_kickoff = (event_dt - now_utc).total_seconds() / 60.0
                windows = (a.get('windows', '') or '').split(',')
                if is_blacklisted(user_id) or not is_authorized(user_id):
                    continue
                for window in windows:
                    if not window:
                        continue
                    target = int(window)
                    if target - 1 <= minutes_to_kickoff <= target + 1:
                        if is_already_notified(event_id, user_id, window):
                            continue
                        event = lookup_event(event_id) or {
                            'idEvent': event_id, 'strEvent': a.get('matchup', ''),
                            'strLeague': a.get('league', ''), 'strVenue': a.get('venue', ''),
                            'dateEvent': event_dt.strftime('%Y-%m-%d'),
                            'strTime': event_dt.strftime('%H:%M:%S')
                        }
                        full_message = f'🔔 <b>Starting in {window} minutes!</b>\n\n' + match_card(event, None, a.get('sport_key', ''))
                        try:
                            parts = chunks(full_message)
                            for i, part in enumerate(parts):
                                await app.bot.send_message(
                                    chat_id=int(user_id), text=part, parse_mode=ParseMode.HTML,
                                    reply_markup=alert_main_menu_keyboard() if i == len(parts) - 1 else None)
                        except Exception as exc:
                            log.warning('Failed to alert %s: %s', user_id, exc)
                        mark_notified(event_id, user_id, window)
        except Exception as exc:
            log.warning('Notification checker error: %s', exc)
        await asyncio.sleep(NOTIFICATION_CHECK_INTERVAL)


async def post_init(app):
    init_db()
    asyncio.create_task(check_upcoming_matches(app))
    log.info('🔔 Match alert checker started')


def build_app():
    app = Application.builder().token(TELEGRAM_TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler('start', cmd_start))
    app.add_handler(CommandHandler('code', cmd_code))
    app.add_handler(CommandHandler('help', cmd_help))
    app.add_handler(CommandHandler('blacklist', cmd_blacklist))
    app.add_handler(CommandHandler('unblacklist', cmd_unblacklist))
    app.add_handler(CommandHandler('stats', cmd_admin_stats))
    app.add_handler(CallbackQueryHandler(button_handler))
    return app


def main():
    log.info('🚀 Starting SportsCast UK bot (Postgres edition)')
    # Startup diagnostics — makes Heroku errors easy to spot in logs
    log.info('🔑 TELEGRAM_TOKEN present: %s', bool(TELEGRAM_TOKEN))
    log.info('🗄️  DATABASE_URL present: %s', bool(DATABASE_URL))
    try:
        with db_cursor() as cur:
            cur.execute('SELECT 1')
        log.info('✅ Database connection OK')
    except Exception as exc:
        log.error('❌ Database connection FAILED: %s', exc)
        raise
    log.info('📡 Starting polling…')
    app = build_app()
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()
