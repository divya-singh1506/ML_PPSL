#!/usr/bin/env python3
"""
ML Report — daily pipeline (Python 3.11, GitHub Actions)

Pulls the Google Ads account (EVERY campaign type) and AppsFlyer in-app events for
net.one97.paytm, and writes five raw tabs to the Google Sheet:

  raw              Date, Campaign ID, Campaign, Campaign Type, Impressions, Clicks, Cost (INR), GAds Leads
  af_raw           Date, Campaign ID, Campaign, Campaign Type, AF Leads, AF Click Leads, AF Impression Leads, AF Other Leads
  af_log           one line per AppsFlyer day pulled — the "Final counted" check
  conv_actions     every conversion action in the account, last 30 days
  campaign_events  which actions / AppsFlyer events each ML campaign is counted on

EACH CAMPAIGN IS COUNTED ON ITS OWN OPTIMISATION EVENTS
  GAds conversions   App campaigns: the in-app actions selected in the campaign's settings.
                     Other campaigns (no selected actions): the actions in Google Ads'
                     "Conversions" column for that campaign (its biddable goals).
  AF conversions     the same actions, as AppsFlyer events (the event name is read from
                     the imported action), credited only to campaigns optimising for them.

The raw column names still say "Leads" (the Apps Script reads them by those names and
shows them as Conversions).
"""
import csv
import io
import json
import os
import re
import sys
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import gspread
import requests
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.oauth2.service_account import Credentials

IST = ZoneInfo('Asia/Kolkata')

# ═════════════════════════════ CONFIG ═════════════════════════════
START_DATE = '2026-06-01'      # first date of Google Ads spend history

# Campaigns whose optimisation events are pulled from AppsFlyer. Keep it the same as
# campaignFilter in the Apps Script. Not case-sensitive. '' = every campaign.
CAMPAIGN_FILTER = 'ML_'
# 'conversions'     = Google Ads "Conversions" column (primary actions only)
# 'all_conversions' = Google Ads "All conv." column (use if the action is secondary)
GADS_CONV_METRIC = 'conversions'

# --- AppsFlyer ---------------------------------------------------
AF_ENABLED = True
AF_START_DATE = '2026-09-21'   # first AppsFlyer day (earlier = longer first backfill)
AF_LOOKBACK_DAYS = 3           # newest days re-pulled on every run
AF_INCLUDE_RETARGETING = True  # also pull re-engagement (retargeting) events
AF_MEDIA_SOURCE_FILTER = 'googleadwords_int'
AF_EVENT_FILTER_MODE = 'auto'  # 'server', 'local' or 'auto'
AF_SCOPE_MODE = 'campaign_id'  # 'campaign_id' = only this account's campaigns; 'off' = keep everything
AF_LAG_DAYS = None             # None = no lag rule; 0 = same-day only; 1 = next day only …
AF_DEDUP_SCOPE = 'daily'       # 'daily' = one per user per event per day; 'off' = every event
AF_DEDUP_FIELDS = ['Customer User ID', 'AppsFlyer ID', 'Advertising ID']
AF_TIMEZONE = 'Asia/Kolkata'   # set to None if AppsFlyer ever rejects it
AF_HOST = 'https://hq1.appsflyer.com'
AF_MAX_ROWS = 1_000_000
AF_REQUEST_TIMEOUT = 180
AF_TIME_BUDGET_SEC = 1500      # whole AppsFlyer stage; the workflow timeout is 45 min
GOOGLE_MEDIA_SOURCES = {'googleadwords_int', 'google', 'google ads', 'google_ads'}
# ══════════════════════════════════════════════════════════════════

RAW_HEADER = ['Date', 'Campaign ID', 'Campaign', 'Campaign Type',
              'Impressions', 'Clicks', 'Cost (INR)', 'GAds Leads']
AF_HEADER = ['Date', 'Campaign ID', 'Campaign', 'Campaign Type',
             'AF Leads', 'AF Click Leads', 'AF Impression Leads', 'AF Other Leads']
AF_LOG_HEADER = ['Date', 'Pulled At (IST)', 'Rows Downloaded', 'Leads Counted',
                 'Click Leads', 'Status', 'Events']
CONV_HEADER = ['Conversion Action', 'Conversions (last 30d)', 'All Conv. (last 30d)',
               'Counted for campaigns', 'AppsFlyer Event']
CAMP_EV_HEADER = ['Campaign ID', 'Campaign', 'Campaign Type', 'Counted on',
                  'Google Ads actions', 'AppsFlyer events']

TYPE_LABELS = {
    'SEARCH': 'Search', 'MULTI_CHANNEL': 'App', 'PERFORMANCE_MAX': 'PMax',
    'DEMAND_GEN': 'Demand Gen', 'DISCOVERY': 'Demand Gen', 'VIDEO': 'Video',
    'DISPLAY': 'Display', 'SHOPPING': 'Shopping', 'SMART': 'Smart',
}


# ───────────────────────────── helpers ─────────────────────────────
def log(msg=''):
    print(msg, flush=True)


def env(name, required=True):
    val = (os.environ.get(name) or '').strip()
    if required and not val:
        sys.exit(f'❌ Missing GitHub secret: {name}')
    return val


def digits(s):
    return re.sub(r'\D', '', s or '')


def norm(s):
    """Letters and digits only, lower case — survives curly quotes, spaces, dashes."""
    return re.sub(r'[^a-z0-9]', '', str(s or '').lower())


def name_ok(name):
    f = CAMPAIGN_FILTER.strip().upper()
    return not f or f in str(name or '').upper()


GOOGLE_NORM = {norm(m) for m in GOOGLE_MEDIA_SOURCES}


def daterange(start, end):
    d, e = date.fromisoformat(str(start)), date.fromisoformat(str(end))
    out = []
    while d <= e:
        out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def to_num(v):
    try:
        return float(str(v).replace(',', '').strip() or 0)
    except ValueError:
        return 0.0


# ─────────────────────────── Google Ads ───────────────────────────
def build_ga_client():
    return GoogleAdsClient.load_from_dict({
        'developer_token': env('DEVELOPER_TOKEN'),
        'client_id': env('CLIENT_ID'),
        'client_secret': env('CLIENT_SECRET'),
        'refresh_token': env('REFRESH_TOKEN'),
        'login_customer_id': digits(env('LOGIN_CUSTOMER_ID')),
        'use_proto_plus': True,
    })


def run_query(client, cid, query, label):
    svc = client.get_service('GoogleAdsService')
    out = []
    try:
        for batch in svc.search_stream(customer_id=cid, query=query):
            out.extend(batch.results)
    except GoogleAdsException as ex:
        log(f'❌ [{label}] Google Ads query failed (request id {ex.request_id})')
        for err in ex.failure.errors:
            log(f'     {err.message}')
        raise   # stop: better to keep yesterday's sheet than write an empty one
    log(f'   [{label}] {len(out):,} rows')
    return out


def channel_label(client, value):
    name = getattr(value, 'name', None)
    if not name:
        try:
            name = client.enums.AdvertisingChannelTypeEnum(value).name
        except Exception:
            name = str(value)
    return TYPE_LABELS.get(name, name.replace('_', ' ').title())


def fetch_campaign_map(client, cid):
    """campaign id -> current name, type and selected in-app actions (incl. paused/removed)."""
    q = ('SELECT campaign.id, campaign.name, campaign.advertising_channel_type, '
         'campaign.selective_optimization.conversion_actions FROM campaign')
    m = {}
    for r in run_query(client, cid, q, 'campaign list'):
        m[str(r.campaign.id)] = {
            'name': r.campaign.name,
            'type': channel_label(client, r.campaign.advertising_channel_type),
            'opt': list(r.campaign.selective_optimization.conversion_actions),
        }
    return m


AF_NAME_RE = re.compile(r'^\S+\s+\(android\)\s+(.+?)(?:\s+\d{4}-\d{2}-\d{2}T[\d:.]+)?$', re.I)


def af_event_for(name, tp_event=''):
    """AppsFlyer event behind an imported Android action, or '' if it isn't one."""
    if '(ios)' in (name or '').lower():
        return ''                       # the AppsFlyer app here is Android
    if tp_event:
        return tp_event.strip()
    m = AF_NAME_RE.match((name or '').strip())
    return m.group(1).strip() if m else ''


def fetch_conversion_actions(client, cid):
    """resource name -> {'name', 'af_event'} for every conversion action."""
    svc = client.get_service('GoogleAdsService')
    base = 'SELECT conversion_action.resource_name, conversion_action.name'
    out = {}
    for extra in (', conversion_action.third_party_app_analytics_settings.event_name', ''):
        try:
            rows = []
            for batch in svc.search_stream(customer_id=cid, query=base + extra + ' FROM conversion_action'):
                rows.extend(batch.results)
        except GoogleAdsException:
            if extra:
                log('   [conversion action list] event-name field not available — reading events from action names')
                continue
            raise
        for r in rows:
            ca = r.conversion_action
            tp = ca.third_party_app_analytics_settings.event_name if extra else ''
            out[ca.resource_name] = {'name': ca.name, 'af_event': af_event_for(ca.name, tp)}
        break
    log(f'   [conversion action list] {len(out):,} rows')
    return out


def fetch_campaign_metrics(client, cid, start, end):
    q = f"""
        SELECT segments.date, campaign.id,
               metrics.impressions, metrics.clicks, metrics.cost_micros
        FROM campaign
        WHERE segments.date BETWEEN '{start}' AND '{end}'"""
    agg = defaultdict(lambda: [0, 0, 0.0])
    for r in run_query(client, cid, q, 'campaign metrics'):
        a = agg[(r.segments.date, str(r.campaign.id))]
        a[0] += r.metrics.impressions
        a[1] += r.metrics.clicks
        a[2] += r.metrics.cost_micros / 1e6
    return agg


def fetch_conversions(client, cid, start, end):
    q = f"""
        SELECT segments.date, campaign.id, segments.conversion_action,
               segments.conversion_action_name,
               metrics.conversions, metrics.all_conversions
        FROM campaign
        WHERE segments.date BETWEEN '{start}' AND '{end}'"""
    return run_query(client, cid, q, 'conversions by action')


def build_leads(conv_rows, since, id_map, actions):
    """GAds conversions per (date, campaign), counting only each campaign's optimisation actions.

    Returns leads, per_action (last-30-day totals), camp_opt {cid: set(action resource names)},
    camp_src {cid: how the actions were picked} and used {action: matching campaigns using it}."""
    camp_conv = defaultdict(float)                 # (cid, action) -> conversions, whole window
    per_action = defaultdict(lambda: [0.0, 0.0])
    for r in conv_rows:
        rn = r.segments.conversion_action
        if rn not in actions:
            actions[rn] = {'name': r.segments.conversion_action_name,
                           'af_event': af_event_for(r.segments.conversion_action_name)}
        camp_conv[(str(r.campaign.id), rn)] += r.metrics.conversions
        if r.segments.date >= since:
            per_action[rn][0] += r.metrics.conversions
            per_action[rn][1] += r.metrics.all_conversions

    camp_opt, camp_src = {}, {}
    for cid, info in id_map.items():
        if info.get('opt'):
            camp_opt[cid] = set(info['opt'])
            camp_src[cid] = 'selected in-app actions'
        else:
            camp_opt[cid] = {rn for (c, rn), v in camp_conv.items() if c == cid and v > 0}
            camp_src[cid] = 'Conversions column (biddable goals)'

    leads = defaultdict(float)
    sel = defaultdict(lambda: [0.0, 0.0])
    for r in conv_rows:
        cid, rn = str(r.campaign.id), r.segments.conversion_action
        if rn not in camp_opt.get(cid, ()):
            continue
        conv, allc = r.metrics.conversions, r.metrics.all_conversions
        sel[cid][0] += conv
        sel[cid][1] += allc
        v = allc if GADS_CONV_METRIC == 'all_conversions' else conv
        if v:
            leads[(r.segments.date, cid)] += v

    used = defaultdict(int)
    for cid, rns in camp_opt.items():
        if name_ok(id_map[cid]['name']):
            for rn in rns:
                used[rn] += 1

    log('')
    log('== ALL CONVERSION ACTIONS IN THIS ACCOUNT (last 30 days) ==')
    log(f'   ✅ = counted for at least one campaign containing "{CAMPAIGN_FILTER}"')
    for rn, (c, a) in sorted(per_action.items(), key=lambda kv: -kv[1][1]):
        mark = '✅' if used.get(rn) else '  '
        log(f'  {mark} {actions[rn]["name"]}  |  conversions={c:,.1f}  all_conv={a:,.1f}')

    log('')
    log(f'== Optimisation events per campaign (name contains "{CAMPAIGN_FILTER}") ==')
    n = 0
    for cid, info in sorted(id_map.items(), key=lambda kv: kv[1]['name']):
        if not name_ok(info['name']):
            continue
        rns = camp_opt.get(cid, set())
        if not rns:
            continue                               # nothing selected and never converted
        n += 1
        evs = sorted({actions.get(rn, {}).get('af_event') for rn in rns} - {'', None})
        log(f'   {info["name"]}  [{info["type"]}] · {camp_src[cid]}')
        for rn in sorted(rns, key=lambda x: actions.get(x, {}).get('name', x)):
            log(f'        GAds: {actions.get(rn, {}).get("name", rn)}')
        log(f'        AF events: {", ".join(evs) if evs else "⚠️ none (no AppsFlyer-imported Android action)"}')
        c, a = sel.get(cid, [0, 0])
        if c == 0 and a > 0 and GADS_CONV_METRIC == 'conversions':
            log("        ⚠️ these actions have All conv. but 0 Conversions — probably SECONDARY. "
                "Set GADS_CONV_METRIC = 'all_conversions'.")
    if not n:
        log('   ⚠️ no matching campaign has selected actions or conversions')
    log(f'   GAds conversions (all campaigns): {sum(leads.values()):,.1f} across {len(leads):,} date|campaign rows')
    return leads, per_action, camp_opt, camp_src, used


def report_channel_breakdown(metrics, id_map, since):
    cost = defaultdict(float)
    for (d, cid), (_, _, c) in metrics.items():
        if d >= since:
            cost[id_map.get(cid, {}).get('type', 'Unknown')] += c
    log('')
    log('== Channel type breakdown (cost, last 30 days) ==')
    for t, c in sorted(cost.items(), key=lambda kv: -kv[1]):
        log(f'   {t:<12} ₹{c:,.0f}')


# ─────────────────────────── Google Sheet ───────────────────────────
def open_sheet():
    info = json.loads(env('GSHEET_SERVICE_ACCOUNT'))
    creds = Credentials.from_service_account_info(
        info, scopes=['https://www.googleapis.com/auth/spreadsheets'])
    log(f'   Sheet is written by: {info.get("client_email")}  (must be an Editor on the Sheet)')
    return gspread.authorize(creds).open_by_key(env('SHEET_ID'))


def find_ws(sh, title):
    """Find a tab ignoring capitals and spaces — Google treats 'Raw' and 'raw' as the same name."""
    want = title.strip().lower()
    for ws in sh.worksheets():
        if ws.title.strip().lower() == want:
            if ws.title != title:
                old = ws.title
                ws.update_title(title)
                log(f'   renamed tab "{old}" → "{title}"')
            return ws
    return None


def get_ws(sh, title, cols):
    return find_ws(sh, title) or sh.add_worksheet(title=title, rows=100, cols=max(cols, 5))


def write_tab(sh, title, header, rows):
    ws = get_ws(sh, title, len(header))
    data = [header] + rows
    ws.clear()
    ws.resize(rows=max(len(data) + 1, 10), cols=len(header))
    step = 10000
    for i in range(0, len(data), step):
        ws.update(range_name=f'A{i + 1}', values=data[i:i + step],
                  value_input_option='RAW')
    log(f'   ✍️  {title}: {len(rows):,} rows')


def read_tab(sh, title):
    ws = find_ws(sh, title)
    if ws is None:
        return []
    vals = ws.get_all_values()
    if len(vals) < 2:
        return []
    head = [h.strip() for h in vals[0]]
    return [dict(zip(head, v)) for v in vals[1:] if any(v)]


# ──────────────────────────── AppsFlyer ────────────────────────────
class AFAuthError(RuntimeError):
    pass


def af_request(app_id, token, day, retarget, event_filter, events):
    url = f'{AF_HOST}/api/raw-data/export/app/{app_id}/in_app_events_report/v5'
    params = {'from': day, 'to': day, 'maximum_rows': AF_MAX_ROWS}
    if AF_MEDIA_SOURCE_FILTER:
        params['media_source'] = AF_MEDIA_SOURCE_FILTER
    if event_filter:
        params['event_name'] = ','.join(events)
    if AF_TIMEZONE:
        params['timezone'] = AF_TIMEZONE
    if retarget:
        params['reattr'] = 'true'
    resp = requests.get(url, params=params, timeout=AF_REQUEST_TIMEOUT,
                        headers={'Authorization': f'Bearer {token}', 'accept': 'text/csv'})
    if resp.status_code == 401:
        raise AFAuthError('AppsFlyer 401 — API token invalid or revoked. Regenerate the V2 token.')
    if resp.status_code != 200:
        raise RuntimeError(f'AppsFlyer HTTP {resp.status_code}: {resp.text[:300]}')
    rows = list(csv.DictReader(io.StringIO(resp.content.decode('utf-8-sig', errors='replace'))))
    if len(rows) >= AF_MAX_ROWS - 1:
        log(f'⚠️  {day}: hit the {AF_MAX_ROWS:,}-row cap — this day is truncated')
    return rows


def pull_af_day(app_id, token, day, state, events):
    """Return (optimisation-event rows, rows downloaded) for one day, UA + retargeting."""
    targets = {norm(e) for e in events}
    out, downloaded = [], 0
    for retarget in ([False, True] if AF_INCLUDE_RETARGETING else [False]):
        tag = 'RT' if retarget else 'UA'
        server = state['mode'] in ('server', 'auto')
        rows = af_request(app_id, token, day, retarget, server, events)
        downloaded += len(rows)
        # 'auto': if the event filter returns nothing, check once without it
        if not rows and state['mode'] == 'auto' and state['fallback'] and not retarget:
            log(f'     {day} {tag}: 0 rows with the event filter — re-pulling unfiltered to check')
            rows = af_request(app_id, token, day, retarget, False, events)
            downloaded += len(rows)
            if any(norm(x.get('Event Name')) in targets for x in rows):
                state['mode'] = 'local'
                log('     event found locally — event_name filter not working; matching locally from now on')
            else:
                state['fallback'] = False
                log('     not in the unfiltered pull either — genuinely no events that day; fallback off')
        matched = [x for x in rows if norm(x.get('Event Name')) in targets]
        for x in matched:
            x['_src'] = tag
        state['src'][tag] += len(matched)
        log(f'     {day} {tag}: downloaded {len(rows):,}, event rows {len(matched):,}')
        out.extend(matched)
    return out, downloaded


def _real_id(v):
    v = (v or '').strip()
    if not v or v.lower() in ('none', 'null', 'n/a'):
        return ''
    if set(v.replace('-', '')) <= {'0'}:      # zeroed advertising ID (limited ad tracking)
        return ''
    return v


def process_af(rows, id_map, st, camp_events, scope_on=True):
    """Apply the gates and return {(date, campaign_id): [all, click, impression, other]}."""
    rows.sort(key=lambda x: x.get('Event Time') or '')
    seen_events, seen_users = set(), defaultdict(set)
    out = defaultdict(lambda: [0, 0, 0, 0])
    for x in rows:
        st['in'] += 1
        # 1. media source
        if norm(x.get('Media Source')) not in GOOGLE_NORM:
            st['not_google'] += 1
            continue
        # 2. account scope — the app is shared with other teams' Google Ads accounts
        cid = (x.get('Campaign ID') or '').strip()
        if scope_on and cid not in id_map:
            st['other_account'] += 1
            st['foreign'][x.get('Campaign') or cid or '(blank)'] += 1
            continue
        # 2b. the event must be one this campaign optimises for
        ev_n = norm(x.get('Event Name'))
        if ev_n not in camp_events.get(cid, ()):
            st['not_opt'] += 1
            st['not_opt_eg'][f"{x.get('Campaign') or cid} · {x.get('Event Name')}"] += 1
            continue
        # 3. lag
        ev_date = (x.get('Event Time') or '')[:10]
        try:
            lag = (date.fromisoformat(ev_date)
                   - date.fromisoformat((x.get('Attributed Touch Time') or '')[:10])).days
            st['lags'][str(lag) if lag < 8 else '8+'] += 1
        except ValueError:
            lag = None
            st['lags']['?'] += 1
        if AF_LAG_DAYS is not None and lag != AF_LAG_DAYS:
            st['lag'] += 1
            continue
        # 4. exact duplicate event (same event can appear in UA and RT reports)
        ekey = (x.get('Event Time'), x.get('AppsFlyer ID'), x.get('Event Name'))
        if ekey in seen_events:
            st['dup_event'] += 1
            continue
        seen_events.add(ekey)
        # 5. one per user per event per day
        if AF_DEDUP_SCOPE == 'daily':
            ids = [f'{f}:{_real_id(x.get(f))}' for f in AF_DEDUP_FIELDS if _real_id(x.get(f))]
            if any(i in seen_users[(ev_date, ev_n)] for i in ids):
                st['dup_user'] += 1
                continue
            seen_users[(ev_date, ev_n)].update(ids)
        # count, split by attributed touch type
        tt = (x.get('Attributed Touch Type') or '').strip().lower()
        a = out[(ev_date, cid)]
        a[0] += 1
        if tt == 'click':
            a[1] += 1
        elif tt in ('impression', 'view'):
            a[2] += 1
        else:
            a[3] += 1
        st['touch'][tt or '(blank)'] += 1
        st['counted'] += 1
    return out


def run_appsflyer(sh, id_map, yesterday, camp_events, events):
    app_id = env('APPSFLYER_APP_ID', False)
    token = env('APPSFLYER_API_TOKEN', False)
    if not (AF_ENABLED and app_id and token):
        log('⏭️  AppsFlyer skipped (disabled or secrets missing) — af_raw left as it was')
        return

    log('')
    log('== AppsFlyer ==')
    sig = ','.join(events)
    log(f'   app {app_id} · events {sig or "(none)"} · scope {AF_SCOPE_MODE} · '
        f'lag {AF_LAG_DAYS} · dedup {AF_DEDUP_SCOPE} · retargeting {AF_INCLUDE_RETARGETING}')
    scope_on = AF_SCOPE_MODE == 'campaign_id'
    if scope_on and not id_map:
        log('⚠️  campaign list is empty — scope would drop everything; switching scope off')
        scope_on = False

    if not events:
        log('⚠️  no AppsFlyer events found for the matching campaigns — af_raw cleared')
        write_tab(sh, 'af_raw', AF_HEADER, [])
        return []

    existing = read_tab(sh, 'af_raw')
    old_log = {r.get('Date'): r for r in read_tab(sh, 'af_log')}
    # a day counts as done only if it was pulled for exactly today's event list
    done_before = {d for d, r in old_log.items() if r.get('Status') == 'ok' and r.get('Events') == sig}
    if old_log and not done_before:
        log('   event list changed (or first run with per-campaign events) — re-pulling every day')

    all_days = daterange(AF_START_DATE, yesterday)
    lb_start = (yesterday - timedelta(days=AF_LOOKBACK_DAYS - 1)).isoformat()
    recent = [d for d in all_days if d >= lb_start][::-1]
    backfill = [d for d in all_days if d < lb_start and d not in done_before][::-1]
    todo = recent + backfill
    log(f'   days to pull: {len(recent)} recent + {len(backfill)} backfill')

    st = {'in': 0, 'not_google': 0, 'other_account': 0, 'lag': 0, 'dup_event': 0,
          'dup_user': 0, 'counted': 0, 'foreign': Counter(), 'lags': Counter(),
          'touch': Counter(), 'not_opt': 0, 'not_opt_eg': Counter()}
    state = {'mode': AF_EVENT_FILTER_MODE, 'fallback': True, 'src': Counter()}
    new_rows, new_log = {}, {}
    now = datetime.now(IST).strftime('%Y-%m-%d %H:%M')
    t0 = time.time()

    for d in todo:
        if time.time() - t0 > AF_TIME_BUDGET_SEC:
            log('⏱️  time budget reached — remaining days will be pulled on the next run')
            break
        try:
            rows, downloaded = pull_af_day(app_id, token, d, state, events)
        except AFAuthError as e:
            log(f'❌ {e}')
            break
        except Exception as e:  # keep yesterday's rows for this day
            log(f'❌ {d}: {e}')
            new_log[d] = [d, now, 0, 0, 0, f'error: {str(e)[:80]}', sig]
            continue
        before_counted = st['counted']
        agg = process_af(rows, id_map, st, camp_events, scope_on)
        new_rows[d] = [
            [k[0], k[1], id_map.get(k[1], {}).get('name', ''),
             id_map.get(k[1], {}).get('type', 'Other'), *v]
            for k, v in agg.items()]
        clicks = sum(v[1] for v in agg.values())
        new_log[d] = [d, now, downloaded, st['counted'] - before_counted, clicks, 'ok', sig]

    # merge: keep old rows for days not re-pulled this run
    final = []
    for r in existing:
        d = r.get('Date', '')
        if d in new_rows or d < AF_START_DATE or d not in done_before:
            continue                    # re-pulled now, too old, or counted on an old event list
        cid = r.get('Campaign ID', '')
        info = id_map.get(cid, {})
        final.append([d, cid, info.get('name') or r.get('Campaign', ''),
                      info.get('type') or r.get('Campaign Type', 'Other'),
                      int(to_num(r.get('AF Leads'))), int(to_num(r.get('AF Click Leads'))),
                      int(to_num(r.get('AF Impression Leads'))),
                      int(to_num(r.get('AF Other Leads')))])
    for rows in new_rows.values():
        final.extend(rows)
    final.sort(key=lambda r: (r[0], r[1]))

    log_rows = [[r.get(h, '') for h in AF_LOG_HEADER] for d, r in old_log.items()
                if d not in new_log]
    log_rows += list(new_log.values())
    log_rows.sort(key=lambda r: r[0], reverse=True)

    log('')
    log('== AppsFlyer summary (this run) ==')
    log(f'   event rows downloaded: {st["in"]:,}  (UA {state["src"]["UA"]:,} · RT {state["src"]["RT"]:,})')
    log(f'   dropped – not Google Ads:          {st["not_google"]:,}')
    log(f'   dropped – other accounts\' campaigns: {st["other_account"]:,}')
    for name, n in st['foreign'].most_common(5):
        log(f'        e.g. {name}: {n:,}')
    log(f'   dropped – not that campaign\'s optimisation event: {st["not_opt"]:,}')
    for name, n in st['not_opt_eg'].most_common(5):
        log(f'        e.g. {name}: {n:,}')
    log(f'   dropped – lag rule:                {st["lag"]:,}')
    log(f'   dropped – duplicate event:         {st["dup_event"]:,}')
    log(f'   dropped – same user same day:      {st["dup_user"]:,}')
    log(f'   Final counted: {st["counted"]:,}   touch types: {dict(st["touch"])}')
    log(f'   lag (days touch→event): {dict(sorted(st["lags"].items()))}')

    write_tab(sh, 'af_raw', AF_HEADER, final)
    write_tab(sh, 'af_log', AF_LOG_HEADER, log_rows)
    return final


# ─────────────────────────────── main ───────────────────────────────
def main():
    yesterday = (datetime.now(IST) - timedelta(days=1)).date()
    y = yesterday.isoformat()
    since30 = (yesterday - timedelta(days=29)).isoformat()
    log(f'ML Report · Google Ads {START_DATE} → {y} · AppsFlyer from {AF_START_DATE}')

    client = build_ga_client()
    cid = digits(env('CLIENT_CUSTOMER_ID'))
    log(f'   Google Ads account: {cid}')
    id_map = fetch_campaign_map(client, cid)
    actions = fetch_conversion_actions(client, cid)
    metrics = fetch_campaign_metrics(client, cid, START_DATE, y)
    conv_rows = fetch_conversions(client, cid, START_DATE, y)
    leads, per_action, camp_opt, camp_src, used = build_leads(conv_rows, since30, id_map, actions)
    report_channel_breakdown(metrics, id_map, since30)

    # AppsFlyer events: per campaign (for counting) and the list to pull (matching campaigns only)
    camp_events, ev_names = {}, {}
    for c, rns in camp_opt.items():
        evs = {actions.get(rn, {}).get('af_event') for rn in rns} - {'', None}
        camp_events[c] = {norm(e) for e in evs}
        if name_ok(id_map[c]['name']):
            for e in evs:
                ev_names.setdefault(norm(e), e)
    events = sorted(ev_names.values())

    raw_rows = []
    for key in sorted(set(metrics) | set(leads)):
        d, c = key
        impr, clicks, cost = metrics.get(key, [0, 0, 0.0])
        info = id_map.get(c, {})
        raw_rows.append([d, c, info.get('name', ''), info.get('type', 'Other'),
                         impr, clicks, round(cost, 2), round(leads.get(key, 0.0), 2)])

    conv_tab = [[actions[rn]['name'], round(c, 2), round(a, 2), used.get(rn) or '',
                 actions[rn]['af_event']]
                for rn, (c, a) in sorted(per_action.items(), key=lambda kv: -kv[1][1])]
    camp_tab = []
    for c, info in sorted(id_map.items(), key=lambda kv: kv[1]['name']):
        rns = camp_opt.get(c, set())
        if not name_ok(info['name']) or not rns:
            continue
        names = sorted(actions.get(rn, {}).get('name', rn) for rn in rns)
        evs = sorted({actions.get(rn, {}).get('af_event') for rn in rns} - {'', None})
        camp_tab.append([c, info['name'], info['type'], camp_src[c], ' | '.join(names), ', '.join(evs)])

    log('')
    log('== Writing sheet ==')
    sh = open_sheet()
    write_tab(sh, 'raw', RAW_HEADER, raw_rows)
    write_tab(sh, 'conv_actions', CONV_HEADER, conv_tab)
    write_tab(sh, 'campaign_events', CAMP_EV_HEADER, camp_tab)

    af_rows = run_appsflyer(sh, id_map, yesterday, camp_events, events)

    # month-to-date check for the matching campaigns, so the log alone answers "did it work?"
    ms = y[:8] + '01'
    ml = [r for r in raw_rows if r[0] >= ms and name_ok(r[2])]
    cost = sum(r[6] for r in ml)
    gl = sum(r[7] for r in ml)
    log('')
    log(f'== MTD check ({ms} → {y}, campaigns containing "{CAMPAIGN_FILTER}") ==')
    log(f'   Cost ₹{cost:,.0f} · GAds Conversions {gl:,.1f} · GAds Cost per Conversion '
        f'{"₹%.2f" % (cost / gl) if gl else "—"}')
    if af_rows:
        afm = [r for r in af_rows if r[0] >= ms and name_ok(r[2])]
        al = sum(r[4] for r in afm)
        ck = sum(r[5] for r in afm)
        log(f'   AF Conversions {al:,} · AF Click Conversions {ck:,} · AF Click Cost per Conversion '
            f'{"₹%.2f" % (cost / ck) if ck else "—"}')
    log('✅ Done')


if __name__ == '__main__':
    main()
