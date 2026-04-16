-- ============================================================
-- GitHub Analytics Data Warehouse — Schema
-- Run this ONCE to set up your database
-- ============================================================

-- Drop tables if re-running (safe to re-run)
DROP TABLE IF EXISTS sync_log CASCADE;
DROP TABLE IF EXISTS languages CASCADE;
DROP TABLE IF EXISTS commits CASCADE;
DROP TABLE IF EXISTS contributors CASCADE;
DROP TABLE IF EXISTS repositories CASCADE;

-- ============================================================
-- 1. REPOSITORIES
-- ============================================================
CREATE TABLE repositories (
    id              BIGINT PRIMARY KEY,
    name            VARCHAR(255)    NOT NULL,
    full_name       VARCHAR(255)    NOT NULL UNIQUE,
    description     TEXT,
    url             VARCHAR(500),
    stars           INT             DEFAULT 0,
    forks           INT             DEFAULT 0,
    open_issues     INT             DEFAULT 0,
    default_branch  VARCHAR(100),
    created_at      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ,
    last_synced_at  TIMESTAMPTZ     DEFAULT NOW()
);

-- ============================================================
-- 2. CONTRIBUTORS
-- ============================================================
CREATE TABLE contributors (
    id          BIGINT PRIMARY KEY,
    login       VARCHAR(255)    NOT NULL UNIQUE,
    avatar_url  VARCHAR(500),
    profile_url VARCHAR(500),
    type        VARCHAR(50)     DEFAULT 'User'
);

-- ============================================================
-- 3. COMMITS
-- ============================================================
CREATE TABLE commits (
    sha             VARCHAR(40)     PRIMARY KEY,
    repo_id         BIGINT          NOT NULL REFERENCES repositories(id) ON DELETE CASCADE,
    contributor_id  BIGINT          REFERENCES contributors(id) ON DELETE SET NULL,
    author_name     VARCHAR(255),
    author_email    VARCHAR(255),
    message         TEXT,
    committed_at    TIMESTAMPTZ,
    inserted_at     TIMESTAMPTZ     DEFAULT NOW()
);

-- ============================================================
-- 4. LANGUAGES
-- ============================================================
CREATE TABLE languages (
    id          SERIAL PRIMARY KEY,
    repo_id     BIGINT          NOT NULL REFERENCES repositories(id) ON DELETE CASCADE,
    language    VARCHAR(100)    NOT NULL,
    bytes       BIGINT          DEFAULT 0,
    UNIQUE (repo_id, language)
);

-- ============================================================
-- 5. SYNC LOG  (tracks every ingestion run)
-- ============================================================
CREATE TABLE sync_log (
    id                      SERIAL PRIMARY KEY,
    repo_id                 BIGINT      NOT NULL REFERENCES repositories(id) ON DELETE CASCADE,
    synced_at               TIMESTAMPTZ DEFAULT NOW(),
    commits_fetched         INT         DEFAULT 0,
    contributors_fetched    INT         DEFAULT 0
);

-- ============================================================
-- INDEXES  (for fast query performance)
-- ============================================================
CREATE INDEX idx_commits_repo_id         ON commits(repo_id);
CREATE INDEX idx_commits_contributor_id  ON commits(contributor_id);
CREATE INDEX idx_commits_committed_at    ON commits(committed_at);
CREATE INDEX idx_languages_repo_id       ON languages(repo_id);
CREATE INDEX idx_sync_log_repo_id        ON sync_log(repo_id);

-- ============================================================
-- TRIGGER — auto-update last_synced_at on repositories
-- whenever a new commit is inserted for that repo
-- ============================================================
CREATE OR REPLACE FUNCTION update_last_synced()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE repositories
    SET last_synced_at = NOW()
    WHERE id = NEW.repo_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_update_last_synced
AFTER INSERT ON commits
FOR EACH ROW
EXECUTE FUNCTION update_last_synced();

-- Done!
SELECT 'Schema created successfully.' AS status;
