import os

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from flask import Flask, render_template, request

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


@app.route("/", methods=["GET", "POST"])
def index():
    columns = []
    rows = []
    error = None
    query = ""

    if request.method == "POST":
        query = request.form.get("query", "").strip()
        if query:
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

    return render_template(
        "index.html",
        query=query,
        columns=columns,
        rows=rows,
        error=error,
    )


if __name__ == "__main__":
    app.run(debug=False)
