"""
GitHub Analytics Data Warehouse — Ingestion Script
Pulls repos, contributors, commits, and languages from GitHub API
into your local PostgreSQL database.

Requirements:
    pip install requests psycopg2-binary

Usage:
    1. Fill in your GITHUB_TOKEN and DB_PASSWORD below
    2. python ingest.py
"""

import time
import requests
import psycopg2

# ============================================================
# CONFIG — fill these in before running
# ============================================================
GITHUB_TOKEN = "MYTOKEN"        # paste your PAT here
REPO_OWNER   = "appwrite"
REPO_NAME    = "appwrite"
MAX_COMMITS  = 1000                     # increase if you want more data

DB_CONFIG = {
    "dbname":   "github_analytics",    # must already exist (see setup below)
    "user":     "postgres",            # your postgres username
    "password": "MYPASSWORD",    # your postgres password
    "host":     "localhost",
    "port":     5432
}
# ============================================================

BASE_URL = "https://api.github.com"
HEADERS  = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept":        "application/vnd.github.v3+json"
}


def get(url, params=None):
    """GET with automatic rate-limit handling."""
    while True:
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        if r.status_code == 403:
            reset = int(r.headers.get("X-RateLimit-Reset", time.time() + 60))
            wait  = max(reset - time.time(), 5)
            print(f"  [rate limit] waiting {wait:.0f}s ...")
            time.sleep(wait)
            continue
        if r.status_code != 200:
            print(f"  [error] {r.status_code} — {r.text[:120]}")
            return None
        return r


# ============================================================
# FETCH: REPOSITORY
# ============================================================
def fetch_repo(conn):
    print(f"\nFetching repo: {REPO_OWNER}/{REPO_NAME}")
    r = get(f"{BASE_URL}/repos/{REPO_OWNER}/{REPO_NAME}")
    if not r:
        raise RuntimeError("Could not fetch repo. Check your token and repo name.")

    d = r.json()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO repositories
            (id, name, full_name, description, url, stars, forks,
             open_issues, default_branch, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            stars        = EXCLUDED.stars,
            forks        = EXCLUDED.forks,
            open_issues  = EXCLUDED.open_issues,
            updated_at   = EXCLUDED.updated_at,
            last_synced_at = NOW()
    """, (
        d["id"], d["name"], d["full_name"], d.get("description"),
        d["html_url"], d["stargazers_count"], d["forks_count"],
        d["open_issues_count"], d["default_branch"],
        d["created_at"], d["updated_at"]
    ))
    conn.commit()
    print(f"  Saved: {d['full_name']}  |  {d['stargazers_count']:,} stars  |  {d['forks_count']:,} forks")
    return d["id"]


# ============================================================
# FETCH: CONTRIBUTORS
# ============================================================
def fetch_contributors(conn, repo_id):
    print(f"\nFetching contributors ...")
    page  = 1
    total = 0
    cur   = conn.cursor()

    while True:
        r = get(
            f"{BASE_URL}/repos/{REPO_OWNER}/{REPO_NAME}/contributors",
            params={"per_page": 100, "page": page, "anon": "false"}
        )
        if not r:
            break

        data = r.json()
        if not data:
            break

        for c in data:
            if c.get("type") == "Anonymous":
                continue
            cur.execute("""
                INSERT INTO contributors (id, login, avatar_url, profile_url, type)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    login       = EXCLUDED.login,
                    avatar_url  = EXCLUDED.avatar_url,
                    profile_url = EXCLUDED.profile_url
            """, (
                c["id"], c["login"],
                c.get("avatar_url"), c.get("html_url"),
                c.get("type", "User")
            ))

        conn.commit()
        total += len(data)
        print(f"  page {page:02d} — {total} contributors so far")

        if len(data) < 100:
            break
        page += 1

    print(f"  Done. Total contributors saved: {total}")
    return total


# ============================================================
# FETCH: COMMITS
# ============================================================
def fetch_commits(conn, repo_id):
    print(f"\nFetching up to {MAX_COMMITS} commits ...")
    page  = 1
    total = 0
    cur   = conn.cursor()

    # Build contributor login → id lookup
    cur.execute("SELECT login, id FROM contributors")
    contributor_map = {row[0]: row[1] for row in cur.fetchall()}

    while total < MAX_COMMITS:
        r = get(
            f"{BASE_URL}/repos/{REPO_OWNER}/{REPO_NAME}/commits",
            params={"per_page": 100, "page": page}
        )
        if not r:
            break

        data = r.json()
        if not data:
            break

        for c in data:
            if total >= MAX_COMMITS:
                break

            sha         = c["sha"]
            commit_info = c["commit"]
            gh_author   = c.get("author")   # GitHub user object, can be None

            contributor_id = None
            if gh_author and gh_author.get("login"):
                contributor_id = contributor_map.get(gh_author["login"])

            git_author  = commit_info.get("author") or {}
            author_name  = git_author.get("name")
            author_email = git_author.get("email")
            committed_at = git_author.get("date")
            message      = commit_info.get("message", "")

            cur.execute("""
                INSERT INTO commits
                    (sha, repo_id, contributor_id, author_name,
                     author_email, message, committed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (sha) DO NOTHING
            """, (
                sha, repo_id, contributor_id,
                author_name, author_email, message, committed_at
            ))
            total += 1

        conn.commit()
        print(f"  page {page:02d} — {total} commits so far")

        if len(data) < 100:
            break
        page += 1

    print(f"  Done. Total commits saved: {total}")
    return total


# ============================================================
# FETCH: LANGUAGES
# ============================================================
def fetch_languages(conn, repo_id):
    print(f"\nFetching languages ...")
    r = get(f"{BASE_URL}/repos/{REPO_OWNER}/{REPO_NAME}/languages")
    if not r:
        return

    data = r.json()
    cur  = conn.cursor()
    for lang, byte_count in data.items():
        cur.execute("""
            INSERT INTO languages (repo_id, language, bytes)
            VALUES (%s, %s, %s)
            ON CONFLICT (repo_id, language) DO UPDATE SET bytes = EXCLUDED.bytes
        """, (repo_id, lang, byte_count))

    conn.commit()
    print(f"  Languages: {', '.join(data.keys())}")


# ============================================================
# LOG SYNC RUN
# ============================================================
def log_sync(conn, repo_id, commits_n, contributors_n):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO sync_log (repo_id, commits_fetched, contributors_fetched)
        VALUES (%s, %s, %s)
    """, (repo_id, commits_n, contributors_n))
    conn.commit()


# ============================================================
# MAIN
# ============================================================
def main():
    if GITHUB_TOKEN == "YOUR_TOKEN_HERE":
        print("ERROR: Please paste your GitHub token into GITHUB_TOKEN before running.")
        return
    if DB_CONFIG["password"] == "YOUR_DB_PASSWORD":
        print("ERROR: Please set your PostgreSQL password in DB_CONFIG.")
        return

    print("Connecting to PostgreSQL ...")
    conn = psycopg2.connect(**DB_CONFIG)
    print("Connected!")

    repo_id       = fetch_repo(conn)
    contributors_n = fetch_contributors(conn, repo_id)
    commits_n      = fetch_commits(conn, repo_id)
    fetch_languages(conn, repo_id)
    log_sync(conn, repo_id, commits_n, contributors_n)

    conn.close()
    print("\n========================================")
    print("  Ingestion complete! Database is ready.")
    print("========================================")


if __name__ == "__main__":
    main()
