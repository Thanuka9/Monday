import requests
import pandas as pd
import io
import time
import json
from flask import Flask, render_template, request, send_file

app = Flask(__name__)

# ---------------------------
# CONFIG
# ---------------------------
API_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjYzNTA4Njc1MiwiYWFpIjoxMSwidWlkIjo4MzgwOTQ0OSwiaWFkIjoiMjAyNi0wMy0xOVQwNzoyODo0My4wMDBaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6MzEzNzAwOTQsInJnbiI6ImFwc2UyIn0.UoF1r-r1lsnJQD6dz9u8KybfXnUTvjrdErcw065eI-E"
API_URL = "https://api.monday.com/v2"

HEADERS = {
    "Authorization": API_TOKEN,
    "Content-Type": "application/json"
}

EXCLUDED_TYPES = {"mirror", "board_relation", "creation_log", "last_updated"}

# ---------------------------
# API CALL
# ---------------------------
def query_monday(query):
    res = requests.post(API_URL, json={"query": query}, headers=HEADERS)
    data = res.json()

    if "errors" in data:
        raise Exception(data["errors"])

    return data


# ---------------------------
# FETCH BOARDS
# ---------------------------
def fetch_boards():
    query = """
    query {
      boards (limit: 100) {
        id
        name
        state
        items_count
      }
    }
    """

    data = query_monday(query)

    boards = data["data"]["boards"]

    return [
        b for b in boards
        if b["state"] == "active" and b["items_count"] > 0
    ]


# ---------------------------
# FETCH ALL ITEMS + SUBITEMS (PAGINATION)
# ---------------------------
def fetch_all_items(board_id):
    all_items = []
    cursor = None

    while True:
        query = f"""
        query {{
          boards(ids: {board_id}) {{
            items_page(limit: 50{', cursor: "' + cursor + '"' if cursor else ''}) {{
              cursor
              items {{
                id
                name
                column_values {{
                  id type text value
                }}
                subitems {{
                  id
                  name
                  column_values {{
                    id type text value
                  }}
                }}
              }}
            }}
          }}
        }}
        """

        data = query_monday(query)

        page = data["data"]["boards"][0]["items_page"]

        items = page["items"]
        all_items.extend(items)

        cursor = page["cursor"]

        if not cursor:
            break

        time.sleep(0.2)

    return all_items


# ---------------------------
# DATA VALIDATION HELPERS
# ---------------------------
def has_real_value(col):
    if col.get("text") and col["text"].strip():
        return True

    try:
        val = json.loads(col.get("value") or "{}")

        return any([
            val.get("label"),
            val.get("date"),
            val.get("personsAndTeams"),
            val.get("linkedPulseIds"),
            val.get("checked") is True,
            val.get("index") not in (None, "")
        ])
    except:
        return False


def is_empty_subitem(sub):
    return all(
        not col.get("text") and not col.get("value")
        for col in sub.get("column_values", [])
    )


def is_junk_name(name):
    if not name:
        return True

    name = name.lower().strip()

    return any(x in name for x in [
        "test", "demo", "sample", "copy", "untitled"
    ])


# ---------------------------
# PROCESS BOARD (MAIN LOGIC)
# ---------------------------
def process_board(board_id):
    items = fetch_all_items(board_id)
    rows = []

    for item in items:
        parent_name = item.get("name", "")
        item_id = item.get("id")

        # -------- MAIN ITEM --------
        row = {
            "type": "main",
            "parent": "",
            "item_name": parent_name,
            "item_id": item_id
        }

        valid_data = False

        for col in item.get("column_values", []):

            if col.get("type") in EXCLUDED_TYPES:
                continue

            if has_real_value(col):
                row[col["id"]] = col.get("text")
                valid_data = True

        if valid_data:
            rows.append(row)

        # -------- SUBITEMS --------
        for sub in item.get("subitems", []):
            sub_name = sub.get("name", "")
            sub_id = sub.get("id")

            if is_junk_name(sub_name):
                continue

            if is_empty_subitem(sub):
                continue

            sub_row = {
                "type": "subitem",
                "parent": parent_name,
                "item_name": sub_name,
                "item_id": sub_id
            }

            valid_data = False

            for col in sub.get("column_values", []):

                if col.get("type") in EXCLUDED_TYPES:
                    continue

                if has_real_value(col):
                    sub_row[col["id"]] = col.get("text")
                    valid_data = True

            if valid_data:
                rows.append(sub_row)

    return pd.DataFrame(rows)


# ---------------------------
# ROUTES
# ---------------------------
@app.route("/", methods=["GET"])
def index():
    boards = fetch_boards()
    return render_template("index.html", boards=boards, table=None)


@app.route("/preview", methods=["POST"])
def preview():
    board_id = request.form.get("board_id")

    boards = fetch_boards()

    df = process_board(board_id)

    table = df.head(50).to_html(classes="table", index=False)

    return render_template("index.html", boards=boards, table=table)


@app.route("/download", methods=["POST"])
def download():
    board_id = request.form.get("board_id")

    df = process_board(board_id)

    output = io.StringIO()
    df.to_csv(output, index=False)

    output.seek(0)

    return send_file(
        io.BytesIO(output.getvalue().encode()),
        mimetype="text/csv",
        as_attachment=True,
        download_name=f"board_{board_id}.csv"
    )


# ---------------------------
# RUN
# ---------------------------
if __name__ == "__main__":
    app.run(debug=True)