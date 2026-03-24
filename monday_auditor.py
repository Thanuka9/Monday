"""
Monday.com Data Completeness Auditor
Production-ready Streamlit app.

Fixes applied vs previous version:
  1. Checkbox false-positive: any explicit "checked" key (true OR false) is
     a deliberate user choice — no longer flagged as missing.
  2. System-managed column types expanded: auto_number, formula, button,
     subtasks, and mirror are all unreachable by the end user and are
     excluded from the audit.
  3. EMPTY_TEXT_VALUES extended with "undefined" and "tbd" (common Monday
     default / integration-written values).
  4. Meaningful-key coverage extended: tag_ids, rating, votersIds now
     prevent false positives on Tags, Rating, and Vote columns.
  5. GraphQL complexity-budget errors are caught and retried with back-off
     instead of surfacing as a raw error to the user.
  6. Timeline "from"/"to" keys added to meaningful_keys so a set timeline
     is never reported missing.
  7. Deleted-column guard: columns whose title resolves to an empty string
     after stripping are skipped at the API layer (already present) and
     also in _make_record to prevent phantom records.
  8. board_relation kept in audit (user can link items); mirror removed
     (reflects another board — user cannot fill it directly; the
     board_relation that feeds it is already audited separately).
"""

import json
import re
import time
import logging
from datetime import datetime

import requests
import pandas as pd
import streamlit as st

# =============================================================================
# Page config  (must be first Streamlit call)
# =============================================================================
st.set_page_config(page_title="Monday Auditor", layout="wide", page_icon="📋")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================================================================
# Constants — edit these to tune behaviour
# =============================================================================
API_URL          = "https://api.monday.com/v2"
MAX_RETRIES      = 3
RETRY_BACKOFF    = 2          # seconds; doubles on each retry
COMPLEXITY_WAIT  = 12         # seconds to wait after a complexity-budget error
ITEMS_PAGE_LIMIT = 500        # Monday.com hard max per page

# Column titles that are always skipped (system / structural display columns)
EXCLUDE_COLUMN_TITLES: frozenset = frozenset({"Subitems", "Item ID", "Name"})

# Column *types* that are always skipped.
#
# Rule: skip a type only when the end user CANNOT fill it — it is either
# auto-managed by Monday or is a pure structural/linking column whose
# presence is controlled elsewhere.
#
#   creation_log  — system: creation timestamp, written by Monday
#   last_updated  — system: last-edit timestamp, written by Monday
#   auto_number   — system: sequential ID, auto-incremented by Monday
#   formula       — system: computed from other column values
#   button        — system: action trigger, stores no persistent data
#   subtasks      — structural: the subitem-link column on a parent board
#   mirror        — derived: reflects a linked board's column value;
#                   the user must fix the source board_relation column
#                   (which IS audited) — reporting both creates duplicate noise
EXCLUDE_COLUMN_TYPES: frozenset = frozenset({
    "creation_log",
    "last_updated",
    "auto_number",
    "formula",
    "button",
    "subtasks",
    "mirror",
})

# Column names containing these words are flagged HIGH severity
HIGH_PRIORITY_KEYWORDS: frozenset = frozenset({
    "date", "owner", "status", "email", "deadline", "priority",
})

# Text values that count as "blank" — lowercased comparison.
# "undefined" appears when some Monday integrations write before a value
# is resolved; "tbd" is a common placeholder written by automations.
EMPTY_TEXT_VALUES: frozenset = frozenset({
    "", "-", "null", "none", "n/a", "undefined", "tbd",
})

# Board names matching any of these substrings are hidden from the picker
BLOCK_KEYWORDS: frozenset = frozenset({
    "subitem", "untitled", "test", "demo", "sample", "template",
    "backup", "archive copy", "archived copy", "do not use",
    "old board", "old version", "copy of", "duplicate",
    "sandbox", "training board",
})

# Board names that are blocked by exact match (after normalisation)
BLOCK_EXACT_NAMES: frozenset = frozenset({"", "subitems", "untitled", "untitled board"})

# Whitelist: leave empty to allow all boards, or add specific board IDs as strings
ALLOWED_BOARD_IDS: set = set()

# Subitem names that are auto-generated placeholders
JUNK_SUBITEM_NAMES: frozenset = frozenset({
    "", "subitem", "new item", "new subitem", "-", "test",
})

# =============================================================================
# Session-state defaults  (idempotent — only sets keys that don't exist yet)
# =============================================================================
_DEFAULTS: dict = {
    "audit_df":        pd.DataFrame(),
    "audit_ran":       False,
    "board_search":    "",
    "filter_severity": [],
    "filter_type":     [],
    "filter_board":    [],
    "filter_missing":  [],
    "filter_col_type": [],
    "filter_search":   "",
    "_board_selection": [],
}
for _k, _v in _DEFAULTS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v


# =============================================================================
# API layer
# =============================================================================

def _headers(api_key: str) -> dict:
    return {"Authorization": api_key, "Content-Type": "application/json"}


def _is_complexity_error(data: dict) -> bool:
    """Return True when Monday has exhausted the GraphQL complexity budget."""
    for err in data.get("errors", []):
        msg = str(err.get("message", "") or err)
        if "COMPLEXITY" in msg.upper() or "complexity" in msg:
            return True
    return False


def query_monday(
    query: str,
    variables: dict = None,
    api_key: str = "",
) -> dict | None:
    """
    POST a GraphQL query to the Monday.com v2 API.

    Retry strategy:
      • Network timeouts and transient request errors → exponential back-off.
      • HTTP 429 rate-limit → honour the Retry-After header.
      • GraphQL COMPLEXITY_BUDGET_EXHAUSTED → wait COMPLEXITY_WAIT seconds
        then retry (Monday resets the budget every 60 s; a short pause is
        usually enough for a single-page query).

    Returns the parsed response dict, or None on unrecoverable failure.
    """
    key     = api_key or st.session_state.get("api_key", "")
    payload = {"query": query, "variables": variables or {}}
    delay   = RETRY_BACKOFF

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            res = requests.post(
                API_URL, json=payload, headers=_headers(key), timeout=60,
            )

            # ── Rate-limit ──────────────────────────────────────────────────
            if res.status_code == 429:
                wait = int(res.headers.get("Retry-After", delay))
                logger.warning(
                    "Rate limited — waiting %ds (attempt %d/%d).",
                    wait, attempt, MAX_RETRIES,
                )
                time.sleep(wait)
                continue

            res.raise_for_status()
            data = res.json()

            # ── Complexity budget exhausted ──────────────────────────────────
            if _is_complexity_error(data):
                logger.warning(
                    "Complexity budget exhausted — waiting %ds (attempt %d/%d).",
                    COMPLEXITY_WAIT, attempt, MAX_RETRIES,
                )
                time.sleep(COMPLEXITY_WAIT)
                continue

            # ── Other GraphQL errors ─────────────────────────────────────────
            if "errors" in data:
                st.error(f"GraphQL error: {data['errors']}")
                return None

            return data

        except requests.exceptions.Timeout:
            logger.warning("Timeout on attempt %d/%d.", attempt, MAX_RETRIES)
        except requests.exceptions.RequestException as exc:
            logger.warning(
                "Request error on attempt %d/%d: %s", attempt, MAX_RETRIES, exc,
            )

        if attempt < MAX_RETRIES:
            time.sleep(delay)
            delay *= 2

    st.error(f"API unreachable after {MAX_RETRIES} attempts. Check your connection.")
    return None


# =============================================================================
# Board-level helpers
# =============================================================================

def _normalize(text: str) -> str:
    """Lowercase and collapse whitespace."""
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def should_block_board(name: str) -> bool:
    """Return True if this board name should be hidden from the picker."""
    lower = _normalize(name)
    if lower in BLOCK_EXACT_NAMES:
        return True
    return any(kw in lower for kw in BLOCK_KEYWORDS)


@st.cache_data(show_spinner=False)
def get_boards(api_key: str) -> dict[str, str]:
    """
    Return {display_label: board_id} for all valid active boards.

    api_key is an explicit parameter (not read from session state) so
    the Streamlit cache is correctly keyed per credential.
    """
    data = query_monday(
        "{ boards(limit: 1000, state: active) { id name board_kind } }",
        api_key=api_key,
    )
    if not data:
        return {}

    board_map: dict[str, str] = {}
    for b in data.get("data", {}).get("boards", []):
        name       = (b.get("name") or "").strip()
        board_id   = str(b.get("id", ""))
        board_kind = b.get("board_kind", "")

        if not name or should_block_board(name):
            continue
        # board_kind "sub" = auto-created subitem board — skip
        if board_kind not in {"public", "private", "share"}:
            continue
        if ALLOWED_BOARD_IDS and board_id not in ALLOWED_BOARD_IDS:
            continue

        label = f"{name} (ID: {board_id})"
        board_map.setdefault(label, board_id)

    return dict(sorted(board_map.items(), key=lambda x: x[0].lower()))


@st.cache_data(show_spinner=False)
def get_account_info(api_key: str) -> dict:
    """Return {name, slug} for the Monday account tied to this key."""
    data = query_monday("{ account { name slug } }", api_key=api_key)
    if data:
        return data.get("data", {}).get("account", {}) or {}
    return {}


# =============================================================================
# Column / value helpers
# =============================================================================

def _parse_json(value: str | None):
    """Parse a JSON string; return original value on failure."""
    try:
        return json.loads(value) if value else None
    except Exception:
        return value


def is_missing(col: dict) -> bool:
    """
    Return True when a column has no meaningful value filled in.

    Decision tree
    ─────────────
    1. Skip system / auto-managed column types entirely — the end user
       cannot fill these, so reporting them as missing is misleading.

    2. Text check — if the text representation contains real content
       (i.e. not in EMPTY_TEXT_VALUES) the field is considered filled.
       This single check handles: text, numbers, email, phone, link,
       dropdown (text = chosen label), date (text = formatted date),
       hour, location, rating, and most other column types.

    3. JSON value check — fallback for columns where the text field is
       legitimately empty even when data exists:
         • personsAndTeams  — person/team assigned
         • linkedPulseIds   — legacy connect-boards format
         • linked_item_ids  — current connect-boards format
         • files            — file attachment
         • label            — status / dropdown label object
         • date             — ISO date string
         • tag_ids          — tags column (text is usually blank)
         • rating           — rating value
         • votersIds        — vote column
         • from / to        — timeline column start and end dates
         • checked          — checkbox (TRUE or FALSE is a deliberate
                              choice; the key's presence is enough)
         • index            — status / dropdown selection index
                              (index 0 = first option, a valid selection)
    """
    # ── 1. Skip unauditable column types ────────────────────────────────────
    col_type = col.get("type", "")
    if col_type in EXCLUDE_COLUMN_TYPES:
        return False

    # ── 1b. Rating column early exit ────────────────────────────────────────
    # Monday writes "0" in the text field and {"rating": 0} in value when no
    # rating is set.  We cannot add "0" to EMPTY_TEXT_VALUES globally because
    # that would incorrectly flag a numbers column that legitimately holds 0.
    # Handle the rating type here — before the generic text check — so both
    # the text and JSON signals are evaluated consistently for this type.
    if col_type == "rating":
        _rv = _parse_json(col.get("value"))
        _n  = _rv.get("rating") if isinstance(_rv, dict) else None
        return not (isinstance(_n, (int, float)) and _n > 0)

    # ── 2. Text check ────────────────────────────────────────────────────────
    raw_text   = col.get("text")
    text_clean = raw_text.strip().lower() if isinstance(raw_text, str) else ""
    if text_clean not in EMPTY_TEXT_VALUES:
        return False   # field has visible content — not missing

    # ── 3. JSON value check ──────────────────────────────────────────────────
    parsed = _parse_json(col.get("value"))

    if isinstance(parsed, dict):
        # Any truthy value on a recognised key means the field is filled.
        # Note: "rating" is handled separately below because Monday stores
        # {"rating": 0} when the column is unset — 0 must not count as filled.
        meaningful_keys = (
            "label",           # status / dropdown label object
            "date",            # date column ISO string
            "personsAndTeams", # people / teams assigned
            "linkedPulseIds",  # connect-boards (legacy)
            "linked_item_ids", # connect-boards (current)
            "files",           # file attachments
            "tag_ids",         # tags column
            "votersIds",       # vote column
            "from",            # timeline start date
            "to",              # timeline end date
        )
        if any(parsed.get(k) not in (None, "", [], {}) for k in meaningful_keys):
            return False

        # Rating: Monday uses {"rating": 0} for "not set" and {"rating": N}
        # (N ≥ 1) for an actual selection. Treat 0 as missing.
        rating_val = parsed.get("rating")
        if isinstance(rating_val, (int, float)) and rating_val > 0:
            return False

        # Checkbox — the key's existence means the user has interacted with
        # the field (checked = True OR deliberately left unchecked = False).
        # Both are valid states, not missing data.
        if "checked" in parsed:
            return False

        # Status / dropdown index — index 0 is the first valid option, not
        # "unset". Unset fields have index = null in the Monday API.
        if parsed.get("index") not in (None, ""):
            return False

    return True   # no content found by any method → field is missing


def is_junk_subitem(sub: dict) -> bool:
    """
    Return True for subitems that are auto-created placeholder rows.
    These have never been touched by a user and clutter the audit output.
    """
    name = (sub.get("name") or "").strip().lower()

    # Auto-generated / placeholder name
    if name in JUNK_SUBITEM_NAMES:
        return True

    values = sub.get("column_values", [])

    # No column data returned — nothing to audit
    if not values:
        return True

    # Every column is blank — ghost row that was never filled
    if all(
        str(col.get("text") or "").strip().lower() in EMPTY_TEXT_VALUES
        for col in values
    ):
        return True

    return False


def get_severity(col_name: str) -> str:
    lower = (col_name or "").lower()
    return "HIGH" if any(k in lower for k in HIGH_PRIORITY_KEYWORDS) else "MEDIUM"


def suggest_fix(col_name: str, col_type: str = "") -> str:
    """Return a short, actionable instruction for the missing field."""
    name  = (col_name  or "").lower()
    ctype = (col_type  or "").lower()

    if any(w in name for w in ("owner", "person", "assignee", "responsible")):
        return "Assign responsible user"
    if any(w in name for w in ("date", "deadline", "due", "start", "end")):
        return "Set date"
    if "status" in name:
        return "Update status"
    if "email" in name:
        return "Add email address"
    if "phone" in name:
        return "Add phone number"
    if "priority" in name:
        return "Set priority"
    if any(w in name for w in ("budget", "cost", "amount", "price", "revenue")):
        return "Enter value"

    # Fall back to column-type hints
    type_hints = {
        "numbers":        "Enter numeric value",
        "numeric":        "Enter numeric value",
        "link":           "Add URL",
        "text":           "Enter text",
        "long_text":      "Enter text",
        "dropdown":       "Select option",
        "board_relation": "Link related item",
        "tags":           "Add tag(s)",
        "rating":         "Set rating",
        "vote":           "Cast vote",
        "timeline":       "Set date range",
        "week":           "Select week",
        "location":       "Enter location",
        "color":          "Choose colour",
        "doc":            "Attach document",
        "file":           "Upload file",
        "hour":           "Set time",
    }
    if ctype in type_hints:
        return type_hints[ctype]

    return "Fill required field"


# =============================================================================
# GraphQL queries
# =============================================================================

_FIRST_PAGE_QUERY = """
query ($id: [ID!]) {
  boards(ids: $id) {
    items_page(limit: """ + str(ITEMS_PAGE_LIMIT) + """) {
      cursor
      items {
        id name
        column_values { id type text value column { title } }
        subitems {
          id name
          column_values { id type text value column { title } }
        }
      }
    }
  }
}
"""

_NEXT_PAGE_QUERY = """
query ($cursor: String!) {
  next_items_page(limit: """ + str(ITEMS_PAGE_LIMIT) + """, cursor: $cursor) {
    cursor
    items {
      id name
      column_values { id type text value column { title } }
      subitems {
        id name
        column_values { id type text value column { title } }
      }
    }
  }
}
"""


# =============================================================================
# Board processing
# =============================================================================

def _make_record(
    board_id: str, board_name: str, slug: str,
    item_type: str, parent_name: str,
    task_name: str, task_id: str, col: dict,
) -> dict | None:
    """
    Build a single audit record dict.
    Returns None if the column title is empty (e.g. a deleted column that
    still appears in the API response) — callers must filter None values.
    """
    col_meta = col.get("column") or {}
    col_name = col_meta.get("title", "").strip()
    if not col_name:
        return None   # guard against deleted / phantom columns

    col_type = col.get("type", "")
    path = (
        f"{board_name} > {parent_name} > {task_name} > {col_name}"
        if item_type == "Subitem"
        else f"{board_name} > {task_name} > {col_name}"
    )
    return {
        "Board":       board_name,
        "Board ID":    board_id,
        "Type":        item_type,
        "Parent":      parent_name,
        "Task":        task_name,
        "Item ID":     task_id,
        "Missing":     col_name,
        "Column ID":   col.get("id", ""),
        "Column Type": col_type,
        "Severity":    get_severity(col_name),
        "Fix":         suggest_fix(col_name, col_type),
        "Path":        path,
        "Link":        f"https://{slug}.monday.com/boards/{board_id}/pulses/{task_id}",
    }


def _process_page(
    items: list,
    board_id: str,
    board_name: str,
    slug: str,
    results: list,
) -> None:
    """Evaluate one page of items and append any missing-field records."""
    for item in items:
        item_name = item.get("name") or "Unknown"
        item_id   = str(item.get("id", ""))

        # ── Main item columns ──────────────────────────────────────────────
        for col in item.get("column_values", []):
            col_title = (col.get("column") or {}).get("title", "").strip()
            if not col_title or col_title in EXCLUDE_COLUMN_TITLES:
                continue
            if col.get("type", "") in EXCLUDE_COLUMN_TYPES:
                continue
            if is_missing(col):
                record = _make_record(
                    board_id, board_name, slug,
                    "Main", item_name, item_name, item_id, col,
                )
                if record:
                    results.append(record)

        # ── Subitems ───────────────────────────────────────────────────────
        for sub in item.get("subitems", []):
            if is_junk_subitem(sub):
                continue
            sub_name = (sub.get("name") or "").strip()
            sub_id   = str(sub.get("id", ""))
            for col in sub.get("column_values", []):
                col_title = (col.get("column") or {}).get("title", "").strip()
                if not col_title or col_title in EXCLUDE_COLUMN_TITLES:
                    continue
                if col.get("type", "") in EXCLUDE_COLUMN_TYPES:
                    continue
                if is_missing(col):
                    record = _make_record(
                        board_id, board_name, slug,
                        "Subitem", item_name, sub_name, sub_id, col,
                    )
                    if record:
                        results.append(record)


def process_board(board_id: str, board_name: str, slug: str) -> list:
    """
    Fetch every item on a board (handling pagination) and return
    a list of missing-field records.

    Pagination strategy
    ───────────────────
    Page 1  → boards > items_page          (initialises the cursor)
    Page 2+ → root next_items_page(cursor) (lower API complexity cost)
    """
    results: list = []

    # ── Page 1 ────────────────────────────────────────────────────────────
    data = query_monday(_FIRST_PAGE_QUERY, {"id": [board_id]})
    if not data:
        return results

    try:
        page   = data["data"]["boards"][0]["items_page"]
        items  = page.get("items", [])
        cursor = page.get("cursor")
    except (KeyError, IndexError, TypeError) as exc:
        logger.warning(
            "Unexpected page-1 response for board %s: %s", board_id, exc,
        )
        return results

    _process_page(items, board_id, board_name, slug, results)

    # ── Pages 2+ ─────────────────────────────────────────────────────────
    while cursor:
        data = query_monday(_NEXT_PAGE_QUERY, {"cursor": cursor})
        if not data:
            break
        try:
            page   = data["data"]["next_items_page"]
            items  = page.get("items", [])
            cursor = page.get("cursor")
        except (KeyError, TypeError) as exc:
            logger.warning(
                "Unexpected next-page response for board %s: %s", board_id, exc,
            )
            break
        _process_page(items, board_id, board_name, slug, results)

    return results


# =============================================================================
# UI helpers
# =============================================================================

def safe_multiselect(label: str, options: list, session_key: str) -> list:
    """
    Multiselect whose default value is automatically sanitised against
    the current option list — prevents Streamlit's crash when persisted
    session-state values no longer exist in the available options.
    """
    clean_opts   = sorted(str(x) for x in options if pd.notna(x))
    current      = st.session_state.get(session_key, [])
    safe_default = [x for x in current if x in clean_opts]
    selection    = st.multiselect(
        label, options=clean_opts, default=safe_default,
    )
    st.session_state[session_key] = selection
    return selection


def safe_contains(df: pd.DataFrame, search_text: str) -> pd.Series:
    """
    Case-insensitive substring search across human-readable columns only.
    Avoids searching ID / link columns for performance and accuracy.
    """
    search_cols = [
        "Board", "Type", "Parent", "Task", "Missing",
        "Column Type", "Severity", "Fix", "Path",
    ]
    cols = [c for c in search_cols if c in df.columns]
    return (
        df[cols]
        .astype(str)
        .apply(lambda x: x.str.contains(
            search_text, case=False, na=False, regex=False,
        ))
        .any(axis=1)
    )


# =============================================================================
# App layout
# =============================================================================

st.title("📋 Monday Auditor")

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Configuration")
    st.text_input(
        "API Key", type="password", key="api_key",
        help="Monday.com → Profile picture → Developers → API token",
    )
    api_key = st.session_state.get("api_key", "")

    if api_key:
        _acct = get_account_info(api_key)
        if _acct:
            st.success(f"✔ Connected: **{_acct.get('name', 'Unknown')}**")
        else:
            st.error("Could not verify API key.")

    st.divider()

    if st.button("🔄 Refresh Board List", use_container_width=True):
        get_boards.clear()
        get_account_info.clear()
        st.rerun()

    if not st.session_state.audit_df.empty:
        st.divider()
        _adf = st.session_state.audit_df
        st.markdown("**Last audit**")
        st.markdown(f"- Total issues: **{len(_adf)}**")
        st.markdown(f"- Boards audited: **{_adf['Board'].nunique()}**")
        st.markdown(f"- HIGH severity: **{(_adf['Severity'] == 'HIGH').sum()}**")

# ── Guard — no key ────────────────────────────────────────────────────────────
if not api_key:
    st.info("👈 Enter your Monday.com API key in the sidebar to get started.")
    st.stop()

# ── Account / slug (fetched once, reused everywhere) ─────────────────────────
account = get_account_info(api_key)
slug    = account.get("slug", "")
if not slug:
    st.error(
        "Could not retrieve your Monday.com account slug. "
        "Check your API key and try refreshing."
    )
    st.stop()

# ── Board selection ───────────────────────────────────────────────────────────
boards = get_boards(api_key)
if not boards:
    st.warning(
        "No valid boards found. Check your API key or adjust "
        "`BLOCK_KEYWORDS` / `ALLOWED_BOARD_IDS` in the config section."
    )
    st.stop()

st.markdown("### 1 · Select Boards to Audit")

search_col, count_col = st.columns([3, 1])
with search_col:
    st.text_input(
        "Filter board list", key="board_search", placeholder="Type to narrow…",
    )
with count_col:
    st.metric("Boards available", len(boards))

visible_boards = list(boards.keys())
if st.session_state.board_search:
    q = st.session_state.board_search.lower().strip()
    visible_boards = [b for b in visible_boards if q in b.lower()]

sa_col, sd_col, _ = st.columns([1, 1, 6])
with sa_col:
    if st.button("✅ Select all", use_container_width=True):
        st.session_state["_board_selection"] = visible_boards
        st.rerun()
with sd_col:
    if st.button("✖ Clear", use_container_width=True):
        st.session_state["_board_selection"] = []
        st.rerun()

# Sanitise persisted selection against currently visible boards
st.session_state["_board_selection"] = [
    b for b in st.session_state["_board_selection"] if b in visible_boards
]

selected = st.multiselect(
    "Boards",
    options=visible_boards,
    default=st.session_state["_board_selection"],
    label_visibility="collapsed",
)
st.session_state["_board_selection"] = selected

# ── Run Audit ─────────────────────────────────────────────────────────────────
st.markdown("### 2 · Run Audit")
run_col, caption_col = st.columns([1, 4])
with run_col:
    run_audit = st.button(
        "▶ Run Audit", type="primary",
        use_container_width=True, disabled=not selected,
    )
with caption_col:
    st.caption(
        f"{len(selected)} board(s) selected · "
        "Audits all user-fillable columns on main items and subitems · Paginated"
    )

if run_audit:
    all_results: list = []
    errors:      list = []
    total        = len(selected)
    progress_bar = st.progress(0.0, text="Starting…")
    status_box   = st.empty()

    for i, board_label in enumerate(selected, start=1):
        board_id = boards.get(board_label)
        if not board_id:
            errors.append(f"Could not resolve ID for: {board_label}")
            progress_bar.progress(i / total, text=f"{i}/{total} — skipped")
            continue

        # Strip the "(ID: ...)" suffix for clean display
        display_name = re.sub(r"\s*\(ID:\s*\d+\)\s*$", "", board_label).strip()
        status_box.info(f"Processing {i}/{total}: {display_name}")

        try:
            board_results = process_board(board_id, display_name, slug)
            all_results.extend(board_results)
        except Exception as exc:
            errors.append(f"{display_name}: {exc}")
            logger.exception("Unhandled error processing board %s", board_id)

        progress_bar.progress(i / total, text=f"{i}/{total} complete")

    progress_bar.empty()
    status_box.empty()

    if errors:
        with st.expander(
            f"⚠ {len(errors)} board(s) had errors — expand for details",
        ):
            for e in errors:
                st.warning(e)

    st.session_state.audit_df  = pd.DataFrame(all_results)
    st.session_state.audit_ran = True
    st.success(
        f"Audit complete — **{len(all_results)}** issue(s) "
        f"across **{len(selected)}** board(s)."
    )

# ── Results ───────────────────────────────────────────────────────────────────
df = st.session_state.audit_df

if st.session_state.audit_ran and df.empty:
    st.success("🎉 No missing fields found across the audited boards.")
    st.stop()

if df.empty:
    st.info("No results yet. Select boards above and click **▶ Run Audit**.")
    st.stop()

# Sort: HIGH before MEDIUM, then alphabetically by board / task
severity_order = pd.CategoricalDtype(["HIGH", "MEDIUM"], ordered=True)
df["Severity"] = df["Severity"].astype(severity_order)
df             = df.sort_values(["Severity", "Board", "Type", "Task"])

st.markdown("---")
st.markdown("### 3 · Results")

# ── Pre-sanitise filter state before any widget renders ───────────────────────
# Prevents crashes when a quick-filter button sets a value (e.g. "Subitem")
# that no longer exists in the current dataset after a new audit.
_valid: dict = {
    "filter_severity": set(df["Severity"].dropna().astype(str).unique()),
    "filter_type":     set(df["Type"].dropna().astype(str).unique()),
    "filter_board":    set(df["Board"].dropna().astype(str).unique()),
    "filter_missing":  set(df["Missing"].dropna().astype(str).unique()),
    "filter_col_type": set(df["Column Type"].dropna().astype(str).unique()),
}
for _fk, _vs in _valid.items():
    st.session_state[_fk] = [
        v for v in st.session_state.get(_fk, []) if v in _vs
    ]

# ── Filter widgets ────────────────────────────────────────────────────────────
with st.expander("🔽 Filters", expanded=True):
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        severity_f = safe_multiselect(
            "Severity",
            df["Severity"].dropna().unique().tolist(),
            "filter_severity",
        )
    with c2:
        type_f = safe_multiselect(
            "Type",
            df["Type"].dropna().unique().tolist(),
            "filter_type",
        )
    with c3:
        board_f = safe_multiselect(
            "Board",
            df["Board"].dropna().unique().tolist(),
            "filter_board",
        )
    with c4:
        missing_f = safe_multiselect(
            "Missing Column",
            df["Missing"].dropna().unique().tolist(),
            "filter_missing",
        )
    with c5:
        coltype_f = safe_multiselect(
            "Column Type",
            df["Column Type"].dropna().unique().tolist(),
            "filter_col_type",
        )

    st.text_input(
        "🔍 Search across results", key="filter_search",
        placeholder="Search board, task, path, fix suggestion…",
    )

    b1, b2, b3, b4 = st.columns(4)
    with b1:
        if st.button("⚡ HIGH only"):
            st.session_state.filter_severity = ["HIGH"]
            st.rerun()
    with b2:
        if st.button("📄 Main only"):
            st.session_state.filter_type = ["Main"]
            st.rerun()
    with b3:
        if st.button("🔗 Subitems only"):
            if "Subitem" in df["Type"].values:
                st.session_state.filter_type = ["Subitem"]
            else:
                st.warning("No subitems in this dataset.")
            st.rerun()
    with b4:
        if st.button("↺ Reset filters"):
            for _fk in (
                "filter_severity", "filter_type", "filter_board",
                "filter_missing", "filter_col_type",
            ):
                st.session_state[_fk] = []
            st.session_state.filter_search = ""
            st.rerun()

# ── Apply filters ─────────────────────────────────────────────────────────────
filtered = df.copy()
if severity_f:
    filtered = filtered[filtered["Severity"].isin(severity_f)]
if type_f:
    filtered = filtered[filtered["Type"].isin(type_f)]
if board_f:
    filtered = filtered[filtered["Board"].isin(board_f)]
if missing_f:
    filtered = filtered[filtered["Missing"].isin(missing_f)]
if coltype_f:
    filtered = filtered[filtered["Column Type"].isin(coltype_f)]
if st.session_state.filter_search:
    filtered = filtered[safe_contains(filtered, st.session_state.filter_search)]

# ── Summary metrics ───────────────────────────────────────────────────────────
st.markdown("#### Summary")
m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("Filtered Issues",  len(filtered))
m2.metric("HIGH Priority",    (filtered["Severity"] == "HIGH").sum())
m3.metric("MEDIUM Priority",  (filtered["Severity"] == "MEDIUM").sum())
m4.metric("Main Item Issues", (filtered["Type"] == "Main").sum())
m5.metric("Subitem Issues",   (filtered["Type"] == "Subitem").sum())

# ── Top problem boards ────────────────────────────────────────────────────────
worst = (
    filtered.groupby("Board").size()
    .reset_index(name="Issues")
    .sort_values("Issues", ascending=False)
    .head(10)
)
st.markdown("#### Top Problem Boards")
st.dataframe(worst, width="stretch", hide_index=True)

# ── Detailed issues table ─────────────────────────────────────────────────────
st.markdown("#### Detailed Issues")

DISPLAY_COLS = [
    "Board", "Type", "Parent", "Task", "Missing",
    "Column Type", "Severity", "Fix", "Path", "Link",
]
display_df = filtered[DISPLAY_COLS].copy()
display_df["Severity"] = display_df["Severity"].astype(str)  # categorical → plain str

st.dataframe(
    display_df,
    width="stretch",
    hide_index=True,
    column_config={
        "Link": st.column_config.LinkColumn(
            "Open in Monday", display_text="Open ↗",
        ),
    },
)

# ── Export ────────────────────────────────────────────────────────────────────
ts = datetime.now().strftime("%Y%m%d_%H%M")
st.download_button(
    label="⬇ Download CSV",
    data=display_df.to_csv(index=False),
    file_name=f"monday_audit_{ts}.csv",
    mime="text/csv",
)