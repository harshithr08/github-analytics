-- ============================================================
-- GitHub Analytics Data Warehouse — Analytics Layer
-- Run: psql -U postgres -d github_analytics -f analytics.sql
-- ============================================================


-- ============================================================
-- SECTION 1: VIEWS
-- ============================================================

-- View 1: Top contributors by commit count
CREATE OR REPLACE VIEW vw_top_contributors AS
SELECT
    c.login,
    c.profile_url,
    COUNT(cm.sha)                                   AS total_commits,
    MIN(cm.committed_at)                            AS first_commit,
    MAX(cm.committed_at)                            AS latest_commit,
    ROUND(
        COUNT(cm.sha) * 100.0 / SUM(COUNT(cm.sha)) OVER(),
    2)                                              AS commit_percentage
FROM contributors c
JOIN commits cm ON cm.contributor_id = c.id
GROUP BY c.id, c.login, c.profile_url
ORDER BY total_commits DESC;


-- View 2: Monthly commit activity
CREATE OR REPLACE VIEW vw_monthly_commits AS
SELECT
    TO_CHAR(DATE_TRUNC('month', committed_at), 'YYYY-MM')   AS month,
    COUNT(*)                                                 AS commit_count,
    COUNT(DISTINCT contributor_id)                           AS active_contributors
FROM commits
WHERE committed_at IS NOT NULL
GROUP BY DATE_TRUNC('month', committed_at)
ORDER BY DATE_TRUNC('month', committed_at) DESC;


-- View 3: Language breakdown with percentage
CREATE OR REPLACE VIEW vw_language_breakdown AS
SELECT
    language,
    bytes,
    ROUND(bytes * 100.0 / SUM(bytes) OVER(), 2)    AS percentage
FROM languages
ORDER BY bytes DESC;


-- View 4: Contributor activity timeline (window function)
CREATE OR REPLACE VIEW vw_contributor_activity AS
SELECT
    c.login,
    DATE_TRUNC('month', cm.committed_at)            AS month,
    COUNT(cm.sha)                                   AS monthly_commits,
    SUM(COUNT(cm.sha)) OVER (
        PARTITION BY c.login
        ORDER BY DATE_TRUNC('month', cm.committed_at)
    )                                               AS running_total
FROM contributors c
JOIN commits cm ON cm.contributor_id = c.id
WHERE cm.committed_at IS NOT NULL
GROUP BY c.login, DATE_TRUNC('month', cm.committed_at)
ORDER BY c.login, month;


-- View 5: Repository summary dashboard
CREATE OR REPLACE VIEW vw_repo_summary AS
SELECT
    r.full_name,
    r.description,
    r.stars,
    r.forks,
    r.open_issues,
    r.default_branch,
    COUNT(DISTINCT cm.sha)              AS total_commits,
    COUNT(DISTINCT cm.contributor_id)   AS total_contributors,
    MIN(cm.committed_at)                AS first_commit_date,
    MAX(cm.committed_at)                AS latest_commit_date,
    r.last_synced_at
FROM repositories r
LEFT JOIN commits cm ON cm.repo_id = r.id
GROUP BY r.id, r.full_name, r.description, r.stars,
         r.forks, r.open_issues, r.default_branch, r.last_synced_at;


-- ============================================================
-- SECTION 2: STORED PROCEDURES
-- ============================================================

-- Procedure 1: Get top N contributors
CREATE OR REPLACE FUNCTION get_top_contributors(n INT DEFAULT 10)
RETURNS TABLE (
    login           VARCHAR,
    total_commits   BIGINT,
    first_commit    TIMESTAMPTZ,
    latest_commit   TIMESTAMPTZ
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        c.login,
        COUNT(cm.sha)       AS total_commits,
        MIN(cm.committed_at) AS first_commit,
        MAX(cm.committed_at) AS latest_commit
    FROM contributors c
    JOIN commits cm ON cm.contributor_id = c.id
    GROUP BY c.login
    ORDER BY total_commits DESC
    LIMIT n;
END;
$$ LANGUAGE plpgsql;

-- Usage: SELECT * FROM get_top_contributors(10);


-- Procedure 2: Commits in a date range
CREATE OR REPLACE FUNCTION get_commits_in_range(
    start_date DATE,
    end_date   DATE
)
RETURNS TABLE (
    sha             VARCHAR,
    author_name     VARCHAR,
    message         TEXT,
    committed_at    TIMESTAMPTZ
) AS $$
BEGIN
    RETURN QUERY
    SELECT cm.sha, cm.author_name, cm.message, cm.committed_at
    FROM commits cm
    WHERE cm.committed_at BETWEEN start_date AND end_date
    ORDER BY cm.committed_at DESC;
END;
$$ LANGUAGE plpgsql;

-- Usage: SELECT * FROM get_commits_in_range('2024-01-01', '2024-12-31');


-- Procedure 3: Contributor report (commits + months active)
CREATE OR REPLACE FUNCTION get_contributor_report(p_login VARCHAR)
RETURNS TABLE (
    login               VARCHAR,
    total_commits       BIGINT,
    months_active       BIGINT,
    first_commit        TIMESTAMPTZ,
    latest_commit       TIMESTAMPTZ,
    avg_commits_per_month NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        c.login,
        COUNT(cm.sha)                                       AS total_commits,
        COUNT(DISTINCT DATE_TRUNC('month', cm.committed_at)) AS months_active,
        MIN(cm.committed_at)                                AS first_commit,
        MAX(cm.committed_at)                                AS latest_commit,
        ROUND(
            COUNT(cm.sha)::NUMERIC /
            NULLIF(COUNT(DISTINCT DATE_TRUNC('month', cm.committed_at)), 0),
        2)                                                  AS avg_commits_per_month
    FROM contributors c
    JOIN commits cm ON cm.contributor_id = c.id
    WHERE c.login ILIKE p_login
    GROUP BY c.login;
END;
$$ LANGUAGE plpgsql;

-- Usage: SELECT * FROM get_contributor_report('styxnanda');


-- ============================================================
-- SECTION 3: TRIGGERS
-- ============================================================

-- Summary table to hold aggregated contributor stats
-- (auto-maintained by trigger)
CREATE TABLE IF NOT EXISTS contributor_summary (
    contributor_id  BIGINT  PRIMARY KEY REFERENCES contributors(id),
    login           VARCHAR(255),
    total_commits   INT     DEFAULT 0
);

-- Function called by the trigger
CREATE OR REPLACE FUNCTION refresh_contributor_summary()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO contributor_summary (contributor_id, login, total_commits)
    SELECT
        c.id,
        c.login,
        COUNT(cm.sha)
    FROM contributors c
    JOIN commits cm ON cm.contributor_id = c.id
    WHERE c.id = NEW.contributor_id
    GROUP BY c.id, c.login
    ON CONFLICT (contributor_id) DO UPDATE
        SET total_commits = EXCLUDED.total_commits;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger: fires on every new commit insert
DROP TRIGGER IF EXISTS trg_refresh_contributor_summary ON commits;
CREATE TRIGGER trg_refresh_contributor_summary
AFTER INSERT ON commits
FOR EACH ROW
WHEN (NEW.contributor_id IS NOT NULL)
EXECUTE FUNCTION refresh_contributor_summary();


-- Populate summary table right now for existing data
INSERT INTO contributor_summary (contributor_id, login, total_commits)
SELECT
    c.id,
    c.login,
    COUNT(cm.sha)
FROM contributors c
JOIN commits cm ON cm.contributor_id = c.id
GROUP BY c.id, c.login
ON CONFLICT (contributor_id) DO UPDATE
    SET total_commits = EXCLUDED.total_commits;


-- ============================================================
-- SECTION 4: PERFORMANCE — EXPLAIN ANALYZE
-- (run these manually to show indexing impact to evaluator)
-- ============================================================

-- Query 1: Top contributors (uses idx_commits_contributor_id)
EXPLAIN ANALYZE
SELECT contributor_id, COUNT(*) AS total
FROM commits
GROUP BY contributor_id
ORDER BY total DESC
LIMIT 10;

-- Query 2: Commits in a date range (uses idx_commits_committed_at)
EXPLAIN ANALYZE
SELECT * FROM commits
WHERE committed_at BETWEEN '2024-01-01' AND '2024-12-31'
ORDER BY committed_at DESC;

-- Query 3: Commits for a repo (uses idx_commits_repo_id)
EXPLAIN ANALYZE
SELECT * FROM commits
WHERE repo_id = (SELECT id FROM repositories LIMIT 1);


SELECT 'Analytics layer ready!' AS status;
