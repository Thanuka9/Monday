import streamlit as st
import requests
import pandas as pd

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
API_KEY = "eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjYzMzgyNDY1MywiYWFpIjoxMSwidWlkIjoxMDEwNjg4NDMsImlhZCI6IjIwMjYtMDMtMTZUMTk6NTE6NTUuMDAwWiIsInBlciI6Im1lOndyaXRlIiwiYWN0aWQiOjM0MjU0MjI2LCJyZ24iOiJhcHNlMiJ9.zvbqn3qIltIoxPRVSkLo-22qT-_TY9vvMFnMiWdMlso"
API_URL = "https://api.monday.com/v2"

HEADERS = {
    "Authorization": API_KEY,
    "API-Version": "2023-10",
    "Content-Type": "application/json",
}

# ==============================
# SESSION STATE
# ==============================
if "audit_df" not in st.session_state:
    st.session_state.audit_df = pd.DataFrame()

if "audit_ran" not in st.session_state:
    st.session_state.audit_ran = False

if "selected_boards" not in st.session_state:
    st.session_state.selected_boards = []

if "selected_columns" not in st.session_state:
    st.session_state.selected_columns = []

if "search" not in st.session_state:
    st.session_state.search = ""

# ==============================
# API HELPER
# ==============================
def safe_request(query, variables=None):
    try:
        res = requests.post(
            API_URL,
            json={"query": query, "variables": variables or {}},
            headers=HEADERS,
            timeout=20
        )

        if res.status_code != 200:
            st.error(f"API Error {res.status_code}")
            return None

        data = res.json()

        if "errors" in data:
            st.error(data["errors"])
            return None

        return data

    except Exception as e:
        st.error(f"Request failed: {e}")
        return None


@st.cache_data(ttl=600)
def get_account_slug():
    data = safe_request("{ account { slug } }")
    return data["data"]["account"]["slug"] if data else "workspace"


@st.cache_data(ttl=600)
def get_all_boards():
    data = safe_request("{ boards(limit: 1000) { id name } }")
    if not data:
        return {}
    return {b["name"]: str(b["id"]) for b in data["data"]["boards"]}


# ==============================
# MAIN LOGIC
# ==============================
def fetch_and_analyze_boards(board_ids, slug):
    results = []
    progress = st.progress(0)

    for i, board_id in enumerate(board_ids):
        progress.progress((i + 1) / len(board_ids), text=f"Scanning {i+1}/{len(board_ids)}")

        res = safe_request("""
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
                  column { title }
                  text
                }
              }
            }
          }
        }
        """, {"board_id": [board_id]})

        if not res:
            continue

        board = res["data"]["boards"][0]
        board_name = board["name"]
        b_id = board["id"]

        items = board["items_page"]["items"]
        cursor = board["items_page"]["cursor"]

        while cursor:
            next_res = safe_request("""
            query ($cursor: String!) {
              next_items_page(limit: 500, cursor: $cursor) {
                cursor
                items {
                  id
                  name
                  column_values {
                    column { title }
                    text
                  }
                }
              }
            }
            """, {"cursor": cursor})

            if not next_res:
                break

            next_page = next_res["data"]["next_items_page"]
            items.extend(next_page["items"])
            cursor = next_page["cursor"]

        for item in items:
            for col in item["column_values"]:
                if not col["text"] or str(col["text"]).strip() == "":
                    results.append({
                        "Board": board_name,
                        "Task": item["name"],
                        "Missing Column": col["column"]["title"],
                        "Open": f"https://{slug}.monday.com/boards/{b_id}/pulses/{item['id']}"
                    })

    progress.empty()
    return pd.DataFrame(results)


# ==============================
# UI
# ==============================
st.title("📊 Monday Data Auditor")

slug = get_account_slug()
boards = get_all_boards()

if not boards:
    st.error("Failed to load boards")
    st.stop()

board_names = sorted(boards.keys())

# ==============================
# SIDEBAR
# ==============================
with st.sidebar:
    st.header("Controls")

    # FIXED SELECT ALL (toggle)
    select_all = st.checkbox("Select all boards")

    if select_all:
        st.session_state.selected_boards = board_names
    elif not select_all and st.session_state.selected_boards == board_names:
        st.session_state.selected_boards = []

    selected = st.multiselect(
        "Boards",
        board_names,
        key="selected_boards"
    )

    if st.button("🚀 Run Audit"):
        if not selected:
            st.warning("Select boards")
        else:
            ids = [boards[x] for x in selected]
            st.session_state.audit_df = fetch_and_analyze_boards(ids, slug)
            st.session_state.audit_ran = True

    if st.button("🔄 Refresh Boards"):
        get_all_boards.clear()
        get_account_slug.clear()
        st.rerun()

    if st.button("🧹 Reset Filters"):
        st.session_state.selected_columns = []
        st.session_state.search = ""


# ==============================
# RESULTS
# ==============================
if st.session_state.audit_ran:

    df = st.session_state.audit_df

    if df.empty:
        st.success("No missing data 🎉")
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Missing Cells", len(df))
        c2.metric("Boards Affected", df["Board"].nunique())
        c3.metric("Tasks Affected", df[["Board","Task"]].drop_duplicates().shape[0])

        st.markdown("### 🎯 Filters")

        col1, col2 = st.columns(2)

        with col1:
            selected_cols = st.multiselect(
                "Filter by column",
                sorted(df["Missing Column"].unique()),
                key="selected_columns"
            )

        with col2:
            search = st.text_input(
                "Search",
                key="search",
                placeholder="Search board, task or column..."
            )

        display_df = df.copy()

        if selected_cols:
            display_df = display_df[display_df["Missing Column"].isin(selected_cols)]

        if search:
            display_df = display_df[
                display_df["Board"].str.contains(search, case=False, na=False) |
                display_df["Task"].str.contains(search, case=False, na=False) |
                display_df["Missing Column"].str.contains(search, case=False, na=False)
            ]

        st.markdown("### 📋 Results")

        if display_df.empty:
            st.info("No matching results")
        else:
            display_df["Info"] = "Missing → " + display_df["Missing Column"]

            st.dataframe(
                display_df,
                width="stretch",
                hide_index=True,
                column_config={
                    "Open": st.column_config.LinkColumn("Open Task", display_text="Open ↗"),
                    "Info": "Missing Field"
                }
            )

            st.download_button(
                "📥 Download CSV",
                display_df.to_csv(index=False).encode(),
                "audit.csv"
            )