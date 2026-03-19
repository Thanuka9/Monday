import os
import json
import requests
import pandas as pd
import streamlit as st

# ==============================
# PAGE CONFIG
# ==============================
st.set_page_config(
    page_title="Monday.com Data Auditor",
    page_icon="📊",
    layout="wide",
)

# ==============================
# CONFIG
# ==============================
API_URL = "https://api.monday.com/v2"

# Prefer Streamlit secrets, then environment variable
API_KEY = None
try:
    API_KEY = st.secrets["MONDAY_API_KEY"]
except Exception:
    API_KEY = os.getenv("MONDAY_API_KEY")

if not API_KEY:
    st.error("Missing Monday API key. Add MONDAY_API_KEY to Streamlit secrets or environment variables.")
    st.stop()

HEADERS = {
    "Authorization": API_KEY,
    "API-Version": "2023-10",
    "Content-Type": "application/json",
}

# Columns to ignore in missing-data audit
EXCLUDE_COLUMN_TITLES = {
    "Subitems",
    "Item ID",
    "Name",
}

# Column types that are usually not useful to audit as missing
EXCLUDE_COLUMN_TYPES = {
    "mirror",
    "board_relation",
    "creation_log",
    "last_updated",
    "color_picker",
    "formula",
    "auto_number",
}

# More important keywords
HIGH_PRIORITY_KEYWORDS = {
    "date", "due", "owner", "person", "people", "status",
    "timeline", "priority", "email", "phone", "deadline"
}

# ==============================
# SESSION STATE DEFAULTS
# ==============================
DEFAULT_STATE = {
    "audit_df": pd.DataFrame(),
    "audit_ran": False,
    "selected_boards": [],
    "selected_columns": [],
    "selected_item_types": ["Main", "Subitem"],
    "selected_severity": ["HIGH", "MEDIUM"],
    "search": "",
    "board_search": "",
}

for key, value in DEFAULT_STATE.items():
    if key not in st.session_state:
        st.session_state[key] = value

# ==============================
# HELPERS
# ==============================
def safe_request(query, variables=None):
    try:
        res = requests.post(
            API_URL,
            json={"query": query, "variables": variables or {}},
            headers=HEADERS,
            timeout=30
        )

        if res.status_code != 200:
            st.error(f"API Error {res.status_code}: {res.text}")
            return None

        data = res.json()

        if "errors" in data:
            st.error(data["errors"])
            return None

        return data

    except requests.exceptions.Timeout:
        st.error("Request timed out.")
        return None
    except Exception as e:
        st.error(f"Request failed: {e}")
        return None


@st.cache_data(ttl=600)
def get_account_slug():
    data = safe_request("{ account { slug } }")
    if not data:
        return "workspace"

    try:
        return data["data"]["account"]["slug"]
    except Exception:
        return "workspace"


@st.cache_data(ttl=600)
def get_all_boards():
    query = """
    {
      boards(limit: 1000) {
        id
        name
      }
    }
    """
    data = safe_request(query)
    if not data:
        return {}

    try:
        boards = data["data"]["boards"]
        return {b["name"]: str(b["id"]) for b in boards if b.get("name") and b.get("id")}
    except Exception:
        return {}


def parse_json_value(raw_value):
    if raw_value in (None, "", "null"):
        return None
    try:
        return json.loads(raw_value)
    except Exception:
        return raw_value


def is_effectively_empty_value(value):
    if value is None:
        return True

    if isinstance(value, str):
        return value.strip() in {"", "-", "None", "null", "{}", "[]"}

    if isinstance(value, (list, tuple, set, dict)):
        return len(value) == 0

    return False


def is_missing(col):
    """
    Better missing detection using both text and underlying JSON value.
    """
    col_type = (col.get("type") or "").strip().lower()
    text = col.get("text")
    raw_value = col.get("value")
    parsed_value = parse_json_value(raw_value)

    if col_type in EXCLUDE_COLUMN_TYPES:
        return False

    if text is None and raw_value is None:
        return True

    if not is_effectively_empty_value(text):
        return False

    if not is_effectively_empty_value(parsed_value):
        if isinstance(parsed_value, dict):
            common_keys = ["label", "text", "name", "email", "phone", "date", "changed_at"]
            for key in common_keys:
                if key in parsed_value and not is_effectively_empty_value(parsed_value.get(key)):
                    return False

            persons = parsed_value.get("personsAndTeams") or parsed_value.get("persons_and_teams")
            if persons and len(persons) > 0:
                return False

            if parsed_value.get("from") or parsed_value.get("to") or parsed_value.get("date"):
                return False

            files = parsed_value.get("files") or parsed_value.get("assets")
            if files and len(files) > 0:
                return False

            linked = parsed_value.get("linkedPulseIds") or parsed_value.get("linked_item_ids")
            if linked and len(linked) > 0:
                return False

        elif isinstance(parsed_value, list) and len(parsed_value) > 0:
            return False

    return True


def get_severity(column_title, item_type):
    title = (column_title or "").lower()

    if any(keyword in title for keyword in HIGH_PRIORITY_KEYWORDS):
        return "HIGH"

    if item_type == "Main":
        return "HIGH"

    return "MEDIUM"


# ==============================
# DATA FETCH
# ==============================
def fetch_board_items_with_subitems(board_id):
    query = """
    query ($board_id: [ID!]) {
      boards(ids: $board_id) {
        id
        name
        items_page(limit: 500) {
          cursor
          items {
            id
            name
            column_values {
              id
              type
              text
              value
              column { title }
            }
            subitems {
              id
              name
              column_values {
                id
                type
                text
                value
                column { title }
              }
            }
          }
        }
      }
    }
    """

    res = safe_request(query, {"board_id": [board_id]})
    if not res:
        return None

    boards_data = res.get("data", {}).get("boards", [])
    if not boards_data:
        return None

    board = boards_data[0]
    items_page = board.get("items_page") or {}
    items = items_page.get("items", []) or []
    cursor = items_page.get("cursor")

    while cursor:
        next_query = """
        query ($cursor: String!) {
          next_items_page(limit: 500, cursor: $cursor) {
            cursor
            items {
              id
              name
              column_values {
                id
                type
                text
                value
                column { title }
              }
              subitems {
                id
                name
                column_values {
                  id
                  type
                  text
                  value
                  column { title }
                }
              }
            }
          }
        }
        """
        next_res = safe_request(next_query, {"cursor": cursor})
        if not next_res:
            break

        next_page = next_res.get("data", {}).get("next_items_page", {})
        next_items = next_page.get("items", []) or []
        items.extend(next_items)
        cursor = next_page.get("cursor")

    board["all_items"] = items
    return board


def fetch_and_analyze_boards(board_ids, slug):
    if not board_ids:
        return pd.DataFrame()

    results = []
    progress = st.progress(0, text="Starting audit...")

    total = len(board_ids)

    for i, board_id in enumerate(board_ids, start=1):
        progress.progress(i / total, text=f"Scanning board {i}/{total}")

        board = fetch_board_items_with_subitems(board_id)
        if not board:
            continue

        board_name = board.get("name", "Unknown Board")
        b_id = board.get("id", board_id)
        items = board.get("all_items", [])

        for item in items:
            item_name = item.get("name", "Unnamed Item")
            item_id = item.get("id")

            # Main items
            for col in item.get("column_values", []):
                col_name = (col.get("column") or {}).get("title", "Unknown")
                col_type = (col.get("type") or "").strip().lower()

                if col_name in EXCLUDE_COLUMN_TITLES or col_type in EXCLUDE_COLUMN_TYPES:
                    continue

                if is_missing(col):
                    results.append({
                        "Board": board_name,
                        "Item Type": "Main",
                        "Parent Item": item_name,
                        "Task": item_name,
                        "Missing Column": col_name,
                        "Column Type": col_type,
                        "Severity": get_severity(col_name, "Main"),
                        "Open": f"https://{slug}.monday.com/boards/{b_id}/pulses/{item_id}" if item_id else ""
                    })

            # Subitems
            for sub in item.get("subitems", []):
                sub_name = sub.get("name", "Unnamed Subitem")
                sub_id = sub.get("id")

                for col in sub.get("column_values", []):
                    col_name = (col.get("column") or {}).get("title", "Unknown")
                    col_type = (col.get("type") or "").strip().lower()

                    if col_name in EXCLUDE_COLUMN_TITLES or col_type in EXCLUDE_COLUMN_TYPES:
                        continue

                    if is_missing(col):
                        results.append({
                            "Board": board_name,
                            "Item Type": "Subitem",
                            "Parent Item": item_name,
                            "Task": sub_name,
                            "Missing Column": col_name,
                            "Column Type": col_type,
                            "Severity": get_severity(col_name, "Subitem"),
                            "Open": f"https://{slug}.monday.com/boards/{b_id}/pulses/{sub_id}" if sub_id else ""
                        })

    progress.empty()

    if not results:
        return pd.DataFrame(columns=[
            "Board", "Item Type", "Parent Item", "Task",
            "Missing Column", "Column Type", "Severity", "Open"
        ])

    return pd.DataFrame(results)


# ==============================
# UI HEADER
# ==============================
st.title("📊 Monday Data Auditor")
st.caption("Audit Monday boards for missing data in both main items and subitems.")

slug = get_account_slug()
boards = get_all_boards()

if not boards:
    st.error("Failed to load boards.")
    st.stop()

all_board_names = sorted(boards.keys())

# Optional search box for board picker
if st.session_state.board_search:
    visible_board_names = [
        b for b in all_board_names
        if st.session_state.board_search.lower() in b.lower()
    ]
else:
    visible_board_names = all_board_names


# ==============================
# CALLBACKS (MUST BE ABOVE SIDEBAR)
# ==============================
def select_visible_boards():
    st.session_state.selected_boards = st.session_state.visible_board_names.copy()

def clear_selected_boards():
    st.session_state.selected_boards = []

def refresh_boards():
    get_all_boards.clear()
    get_account_slug.clear()
    st.session_state.selected_boards = []
    st.session_state.audit_df = pd.DataFrame()
    st.session_state.audit_ran = False

def reset_filters():
    st.session_state.selected_columns = []
    st.session_state.selected_item_types = ["Main", "Subitem"]
    st.session_state.selected_severity = ["HIGH", "MEDIUM"]
    st.session_state.search = ""


# ==============================
# SIDEBAR
# ==============================
with st.sidebar:
    st.header("Controls")

    # 🔍 SEARCH
    st.text_input(
        "Search boards",
        key="board_search",
        placeholder="Type to filter board names..."
    )

    # 🔁 FILTER BOARD LIST (STORE IN STATE → IMPORTANT)
    if st.session_state.board_search:
        st.session_state.visible_board_names = [
            b for b in all_board_names
            if st.session_state.board_search.lower() in b.lower()
        ]
    else:
        st.session_state.visible_board_names = all_board_names.copy()

    # 🧠 SAFETY: ensure selected boards always valid
    st.session_state.selected_boards = [
        b for b in st.session_state.selected_boards
        if b in all_board_names
    ]

    # ==============================
    # BUTTONS (SAFE WITH CALLBACKS)
    # ==============================
    c1, c2 = st.columns(2)

    with c1:
        st.button(
            "Select visible",
            on_click=select_visible_boards,
            width="stretch"
        )

    with c2:
        st.button(
            "Clear boards",
            on_click=clear_selected_boards,
            width="stretch"
        )

    # ==============================
    # BOARD SELECTOR (SAFE)
    # ==============================
    st.multiselect(
        "Boards",
        options=st.session_state.visible_board_names,
        key="selected_boards",
        placeholder="Select one or more boards"
    )

    st.markdown("---")

    # ==============================
    # RUN AUDIT
    # ==============================
    if st.button("🚀 Run Audit", width="stretch"):
        if not st.session_state.selected_boards:
            st.warning("Select at least one board.")
        else:
            ids = [
                boards[name]
                for name in st.session_state.selected_boards
                if name in boards
            ]

            with st.spinner("Fetching and analyzing boards..."):
                st.session_state.audit_df = fetch_and_analyze_boards(ids, slug)
                st.session_state.audit_ran = True

    # ==============================
    # REFRESH
    # ==============================
    st.button(
        "🔄 Refresh Boards",
        on_click=refresh_boards,
        width="stretch"
    )

    # ==============================
    # RESET FILTERS
    # ==============================
    st.button(
        "🧹 Reset Filters",
        on_click=reset_filters,
        width="stretch"
    )
# ==============================
# RESULTS
# ==============================
if st.session_state.audit_ran:
    df = st.session_state.audit_df.copy()

    if df.empty:
        st.success("No missing data found 🎉")
    else:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Missing Cells", len(df))
        c2.metric("Boards Affected", df["Board"].nunique())
        c3.metric("Main Items Affected", df[df["Item Type"] == "Main"]["Task"].nunique())
        c4.metric("Subitems Affected", df[df["Item Type"] == "Subitem"]["Task"].nunique())

        st.markdown("## 🎯 Filters")

        f1, f2, f3, f4 = st.columns(4)

        with f1:
            selected_cols = st.multiselect(
                "Missing Column",
                sorted(df["Missing Column"].dropna().unique().tolist()),
                key="selected_columns"
            )

        with f2:
            item_type_filter = st.multiselect(
                "Item Type",
                ["Main", "Subitem"],
                key="selected_item_types"
            )

        with f3:
            severity_filter = st.multiselect(
                "Severity",
                ["HIGH", "MEDIUM"],
                key="selected_severity"
            )

        with f4:
            search = st.text_input(
                "Search results",
                key="search",
                placeholder="Search board, task, parent item, or column..."
            )

        display_df = df.copy()

        if selected_cols:
            display_df = display_df[display_df["Missing Column"].isin(selected_cols)]

        if item_type_filter:
            display_df = display_df[display_df["Item Type"].isin(item_type_filter)]

        if severity_filter:
            display_df = display_df[display_df["Severity"].isin(severity_filter)]

        if search:
            display_df = display_df[
                display_df["Board"].astype(str).str.contains(search, case=False, na=False) |
                display_df["Task"].astype(str).str.contains(search, case=False, na=False) |
                display_df["Parent Item"].astype(str).str.contains(search, case=False, na=False) |
                display_df["Missing Column"].astype(str).str.contains(search, case=False, na=False)
            ]

        st.markdown("## 📈 Summary")

        if display_df.empty:
            st.info("No matching results for the current filters.")
        else:
            s1, s2 = st.columns(2)

            with s1:
                st.markdown("### Missing by Column")
                col_summary = (
                    display_df.groupby("Missing Column")
                    .size()
                    .reset_index(name="Missing Count")
                    .sort_values("Missing Count", ascending=False)
                )
                st.dataframe(col_summary, use_container_width=True, hide_index=True)
                if not col_summary.empty:
                    st.bar_chart(col_summary.set_index("Missing Column")["Missing Count"])

            with s2:
                st.markdown("### Missing by Board")
                board_summary = (
                    display_df.groupby("Board")
                    .size()
                    .reset_index(name="Missing Count")
                    .sort_values("Missing Count", ascending=False)
                )
                st.dataframe(board_summary, use_container_width=True, hide_index=True)
                if not board_summary.empty:
                    st.bar_chart(board_summary.set_index("Board")["Missing Count"])

            s3, s4 = st.columns(2)

            with s3:
                st.markdown("### Missing by Severity")
                sev_summary = (
                    display_df.groupby("Severity")
                    .size()
                    .reset_index(name="Missing Count")
                    .sort_values("Missing Count", ascending=False)
                )
                st.dataframe(sev_summary, use_container_width=True, hide_index=True)

            with s4:
                st.markdown("### Missing by Item Type")
                type_summary = (
                    display_df.groupby("Item Type")
                    .size()
                    .reset_index(name="Missing Count")
                    .sort_values("Missing Count", ascending=False)
                )
                st.dataframe(type_summary, use_container_width=True, hide_index=True)

            st.markdown("## 📋 Detailed Results")

            display_df = display_df.sort_values(
                by=["Severity", "Board", "Item Type", "Missing Column", "Task"],
                ascending=[True, True, True, True, True]
            ).copy()

            display_df["Where Missing"] = display_df.apply(
                lambda row: (
                    f"Subitem: {row['Task']} | Parent: {row['Parent Item']} | Missing: {row['Missing Column']}"
                    if row["Item Type"] == "Subitem"
                    else f"Item: {row['Task']} | Missing: {row['Missing Column']}"
                ),
                axis=1
            )

            result_columns = [
                "Board",
                "Item Type",
                "Parent Item",
                "Task",
                "Missing Column",
                "Column Type",
                "Severity",
                "Where Missing",
                "Open",
            ]

            st.dataframe(
                display_df[result_columns],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Open": st.column_config.LinkColumn("Open Item", display_text="Open ↗"),
                    "Where Missing": "Issue Details",
                }
            )

            st.download_button(
                "📥 Download CSV",
                display_df[result_columns].to_csv(index=False).encode("utf-8"),
                file_name="monday_audit_results.csv",
                mime="text/csv"
            )

            st.markdown("### Grouped View")
            grouped = (
                display_df.groupby(["Board", "Item Type", "Missing Column"])
                .size()
                .reset_index(name="Count")
                .sort_values(["Board", "Count"], ascending=[True, False])
            )
            st.dataframe(grouped, use_container_width=True, hide_index=True)

else:
    st.info("Select one or more boards from the sidebar and run the audit.")