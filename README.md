# GitHub Analytics Data Warehouse

## Overview
This project builds a data warehouse using PostgreSQL by extracting data from GitHub API and analyzing it.

## Features
- Data ingestion from GitHub API
- Normalized relational schema
- SQL views for analytics
- Dashboard using Flask

## Setup Instructions

1. Install PostgreSQL
2. Create database:
   CREATE DATABASE github_analytics;

3. Run schema:
   psql -U postgres -d github_analytics -f schema.sql

4. Add GitHub token in ingest.py

5. Run ingestion:
   python3 ingest.py

6. Run analytics:
   psql -U postgres -d github_analytics -f analytics.sql

7. Run dashboard:
   python3 app.py

8. Open browser:
   http://localhost:5000
