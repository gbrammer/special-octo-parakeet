import csv
import io
import os
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from flask import Flask, Response, render_template, request

load_dotenv()

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-key-change-in-production")


def get_db_connection():
    """Return a new psycopg2 connection using DATABASE_URL or individual env vars."""
    database_url = os.environ.get("DATABASE_URL")
    if database_url:
        return psycopg2.connect(database_url)
    return psycopg2.connect(
        host=os.environ.get("DB_HOST", "localhost"),
        port=int(os.environ.get("DB_PORT", 5432)),
        dbname=os.environ.get("DB_NAME", "postgres"),
        user=os.environ.get("DB_USER", "postgres"),
        password=os.environ.get("DB_PASSWORD"),
    )


QUERIES_LOG = os.path.join(os.path.dirname(__file__), "queries.txt")


def log_query(email, query, tag="start", remote_addr="0.0.0.0", nrows=0):
    """Append a log entry to queries.txt."""
    timestamp = datetime.now(timezone.utc).isoformat()
    safe_email = email.replace("\n", " ").replace("\r", " ")
    safe_query = query.replace("\n", " ").replace("\r", " ")
    with open(QUERIES_LOG, "a", encoding="utf-8") as f:
        f.write(
            f"[{timestamp}] email={safe_email!r} query={safe_query!r} remote_addr={remote_addr} tag={tag} nrows={nrows}\n"
        )


@app.route("/", methods=["GET", "POST"])
def index():
    columns = []
    rows = []
    error = None
    query = ""
    email = ""
    csv_output = False

    if request.method == "POST":
        email = request.form.get("email", "").strip()
        query = request.form.get("query", "").strip()
        csv_output = request.form.get("csv_output") == "1"
        if "@" not in email or "." not in email.split("@")[-1]:
            error = "A valid email address is required."
        elif query:
            log_query(email, query, remote_addr=request.remote_addr, tag="send")
            conn = None
            cursor = None
            try:
                conn = get_db_connection()
                cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                cursor.execute(query)
                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    rows = [list(row) for row in cursor.fetchall()]
                    conn.rollback()
                else:
                    # Non-SELECT statement: commit only if explicitly intended.
                    # Rolling back here keeps the tool read-safe by default.
                    conn.rollback()
                    rows = []
                    columns = []
            except Exception as exc:
                error = str(exc)
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()

            log_query(
                email,
                query,
                remote_addr=request.remote_addr,
                tag="complete",
                nrows=len(rows),
            )

            if csv_output and columns:
                si = io.StringIO()
                writer = csv.writer(si)
                writer.writerow(columns)
                writer.writerows(rows)
                output = si.getvalue()
                return Response(
                    output,
                    mimetype="text/csv",
                    headers={"Content-Disposition": "attachment; filename=query_results.csv"},
                )

    parse_rows_and_columns(rows, columns)

    return render_template(
        "index.html",
        query=query,
        email=email,
        columns=columns,
        rows=rows,
        error=error,
        csv_output=csv_output,
    )


def parse_rows_and_columns(rows, columns):
    FORMATS = {"ra": ".6f", "dec": ".6f", "expstart": ".2f"}

    for i, c in enumerate(columns):
        if c.lower() in FORMATS:
            for row in rows:
                row[i] = ("{0:" + FORMATS[c] + "}").format(float(row[i]))


if __name__ == "__main__":
    app.run(debug=False)
