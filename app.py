"""
GitHub Analytics Dashboard — Flask App
Run: python app.py  →  open http://localhost:5000
Requirements: pip install flask psycopg2-binary
"""

from flask import Flask, render_template_string, jsonify
import psycopg2
import psycopg2.extras

app = Flask(__name__)

# ============================================================
# CONFIG — same as ingest.py
# ============================================================
DB_CONFIG = {
    "dbname":   "github_analytics",
    "user":     "postgres",
    "password": "MYPASSWORD",     # same password as ingest.py
    "host":     "localhost",
    "port":     5432
}

def get_db():
    return psycopg2.connect(**DB_CONFIG, cursor_factory=psycopg2.extras.RealDictCursor)

# ============================================================
# DASHBOARD HTML TEMPLATE
# ============================================================
TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>GitHub Analytics — Appwrite</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  body {
    font-family: 'Segoe UI', system-ui, sans-serif;
    background: #0d1117;
    color: #e6edf3;
    min-height: 100vh;
  }

  header {
    background: #161b22;
    border-bottom: 1px solid #30363d;
    padding: 18px 40px;
    display: flex;
    align-items: center;
    gap: 14px;
  }
  header svg { color: #f0883e; }
  header h1 { font-size: 1.3rem; font-weight: 600; color: #f0f6fc; }
  header span { font-size: 0.85rem; color: #8b949e; margin-left: 4px; }

  .main { padding: 32px 40px; max-width: 1400px; margin: 0 auto; }

  /* Stat cards */
  .stats-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 16px;
    margin-bottom: 32px;
  }
  .stat-card {
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 10px;
    padding: 20px 24px;
  }
  .stat-card .label { font-size: 0.78rem; color: #8b949e; text-transform: uppercase; letter-spacing: .06em; margin-bottom: 8px; }
  .stat-card .value { font-size: 2rem; font-weight: 700; color: #f0f6fc; }
  .stat-card .sub   { font-size: 0.78rem; color: #8b949e; margin-top: 4px; }

  /* Charts grid */
  .charts-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 20px;
    margin-bottom: 32px;
  }
  @media (max-width: 900px) { .charts-grid { grid-template-columns: 1fr; } }

  .card {
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 10px;
    padding: 22px;
  }
  .card h2 {
    font-size: 0.92rem;
    font-weight: 600;
    color: #8b949e;
    text-transform: uppercase;
    letter-spacing: .06em;
    margin-bottom: 18px;
  }
  .chart-wrap { position: relative; height: 260px; }

  /* Table */
  .table-card {
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 10px;
    padding: 22px;
    margin-bottom: 20px;
  }
  .table-card h2 {
    font-size: 0.92rem;
    font-weight: 600;
    color: #8b949e;
    text-transform: uppercase;
    letter-spacing: .06em;
    margin-bottom: 16px;
  }
  table { width: 100%; border-collapse: collapse; }
  th {
    text-align: left;
    font-size: 0.78rem;
    color: #8b949e;
    padding: 8px 12px;
    border-bottom: 1px solid #30363d;
    text-transform: uppercase;
    letter-spacing: .05em;
  }
  td {
    padding: 10px 12px;
    font-size: 0.88rem;
    border-bottom: 1px solid #21262d;
    color: #e6edf3;
  }
  tr:last-child td { border-bottom: none; }
  tr:hover td { background: #1c2128; }
  .rank { color: #8b949e; font-size: 0.8rem; }
  .login { color: #79c0ff; font-weight: 500; }
  .badge {
    display: inline-block;
    background: #1f6feb33;
    color: #79c0ff;
    border: 1px solid #1f6feb55;
    border-radius: 20px;
    padding: 2px 10px;
    font-size: 0.76rem;
  }

  .footer {
    text-align: center;
    padding: 20px;
    color: #484f58;
    font-size: 0.8rem;
    border-top: 1px solid #21262d;
    margin-top: 20px;
  }
</style>
</head>
<body>

<header>
  <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
    <path d="M9 19c-5 1.5-5-2.5-7-3m14 6v-3.87a3.37 3.37 0 0 0-.94-2.61c3.14-.35 6.44-1.54 6.44-7A5.44 5.44 0 0 0 20 4.77A5.07 5.07 0 0 0 19.91 1S18.73.65 16 2.48a13.38 13.38 0 0 0-7 0C6.27.65 5.09 1 5.09 1A5.07 5.07 0 0 0 5 4.77a5.44 5.44 0 0 0-1.5 3.78c0 5.42 3.3 6.61 6.44 7A3.37 3.37 0 0 0 9 18.13V22"/>
  </svg>
  <h1>GitHub Analytics Data Warehouse <span>/ appwrite/appwrite</span></h1>
</header>

<div class="main">

  <!-- STAT CARDS -->
  <div class="stats-grid">
    <div class="stat-card">
      <div class="label">Total Commits</div>
      <div class="value">{{ summary.total_commits | default('—') }}</div>
      <div class="sub">in database</div>
    </div>
    <div class="stat-card">
      <div class="label">Contributors</div>
      <div class="value">{{ summary.total_contributors | default('—') }}</div>
      <div class="sub">unique authors</div>
    </div>
    <div class="stat-card">
      <div class="label">GitHub Stars</div>
      <div class="value">{{ "{:,}".format(summary.stars) if summary.stars else '—' }}</div>
      <div class="sub">as of last sync</div>
    </div>
    <div class="stat-card">
      <div class="label">Forks</div>
      <div class="value">{{ "{:,}".format(summary.forks) if summary.forks else '—' }}</div>
      <div class="sub">public forks</div>
    </div>
    <div class="stat-card">
      <div class="label">Open Issues</div>
      <div class="value">{{ "{:,}".format(summary.open_issues) if summary.open_issues else '—' }}</div>
      <div class="sub">unresolved</div>
    </div>
    <div class="stat-card">
      <div class="label">Languages</div>
      <div class="value">{{ lang_count }}</div>
      <div class="sub">detected in repo</div>
    </div>
  </div>

  <!-- CHARTS ROW -->
  <div class="charts-grid">
    <div class="card">
      <h2>Monthly Commit Activity</h2>
      <div class="chart-wrap">
        <canvas id="commitsChart"></canvas>
      </div>
    </div>
    <div class="card">
      <h2>Language Breakdown</h2>
      <div class="chart-wrap">
        <canvas id="langChart"></canvas>
      </div>
    </div>
  </div>

  <!-- TOP CONTRIBUTORS TABLE -->
  <div class="table-card">
    <h2>Top Contributors by Commit Count</h2>
    <table>
      <thead>
        <tr>
          <th>#</th>
          <th>Username</th>
          <th>Total Commits</th>
          <th>Share</th>
          <th>First Commit</th>
          <th>Latest Commit</th>
        </tr>
      </thead>
      <tbody>
        {% for row in contributors %}
        <tr>
          <td class="rank">{{ loop.index }}</td>
          <td><span class="login">{{ row.login }}</span></td>
          <td><strong>{{ row.total_commits }}</strong></td>
          <td><span class="badge">{{ row.commit_percentage }}%</span></td>
          <td>{{ row.first_commit.strftime('%Y-%m-%d') if row.first_commit else '—' }}</td>
          <td>{{ row.latest_commit.strftime('%Y-%m-%d') if row.latest_commit else '—' }}</td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
  </div>

</div>

<div class="footer">GitHub Analytics Data Warehouse &mdash; DBMS Course Project</div>

<script>
// Monthly commits chart
const monthlyData = {{ monthly_commits | tojson }};
const months  = monthlyData.map(r => r.month).reverse();
const counts  = monthlyData.map(r => r.commit_count).reverse();

new Chart(document.getElementById('commitsChart'), {
  type: 'bar',
  data: {
    labels: months,
    datasets: [{
      label: 'Commits',
      data: counts,
      backgroundColor: '#1f6feb99',
      borderColor: '#388bfd',
      borderWidth: 1,
      borderRadius: 4,
    }]
  },
  options: {
    responsive: true, maintainAspectRatio: false,
    plugins: { legend: { display: false } },
    scales: {
      x: { ticks: { color: '#8b949e', maxTicksLimit: 8 }, grid: { color: '#21262d' } },
      y: { ticks: { color: '#8b949e' }, grid: { color: '#21262d' } }
    }
  }
});

// Language chart
const langData = {{ languages | tojson }};
const langColors = ['#388bfd','#f0883e','#3fb950','#d2a8ff','#ffa657',
                    '#79c0ff','#56d364','#ff7b72','#e3b341','#bc8cff'];

new Chart(document.getElementById('langChart'), {
  type: 'doughnut',
  data: {
    labels: langData.map(r => r.language),
    datasets: [{
      data: langData.map(r => r.percentage),
      backgroundColor: langColors,
      borderColor: '#161b22',
      borderWidth: 2,
    }]
  },
  options: {
    responsive: true, maintainAspectRatio: false,
    plugins: {
      legend: {
        position: 'right',
        labels: { color: '#8b949e', font: { size: 11 }, padding: 12 }
      }
    }
  }
});
</script>
</body>
</html>
"""

# ============================================================
# ROUTES
# ============================================================
@app.route("/")
def index():
    conn = get_db()
    cur  = conn.cursor()

    # Repo summary
    cur.execute("SELECT * FROM vw_repo_summary LIMIT 1")
    summary = cur.fetchone() or {}

    # Top 15 contributors
    cur.execute("SELECT * FROM vw_top_contributors LIMIT 15")
    contributors = cur.fetchall()

    # Monthly commits (last 18 months)
    cur.execute("SELECT * FROM vw_monthly_commits LIMIT 18")
    monthly_commits = [dict(r) for r in cur.fetchall()]

    # Language breakdown
    cur.execute("SELECT * FROM vw_language_breakdown")
    languages = [dict(r) for r in cur.fetchall()]
    lang_count = len(languages)

    conn.close()

    return render_template_string(
        TEMPLATE,
        summary=summary,
        contributors=contributors,
        monthly_commits=monthly_commits,
        languages=languages,
        lang_count=lang_count,
    )


@app.route("/api/summary")
def api_summary():
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("SELECT * FROM vw_repo_summary LIMIT 1")
    data = cur.fetchone()
    conn.close()
    return jsonify(dict(data) if data else {})


@app.route("/api/contributors")
def api_contributors():
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("SELECT * FROM vw_top_contributors LIMIT 20")
    data = [dict(r) for r in cur.fetchall()]
    conn.close()
    return jsonify(data)


if __name__ == "__main__":
    print("\n  GitHub Analytics Dashboard")
    print("  Open → http://localhost:5000\n")
    app.run(debug=True, port=5000)
