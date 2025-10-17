# EFO Data Pipeline - How to Run

This document provides complete instructions for running the EFO Data Pipeline project.

---

## Prerequisites

- **Docker** and **Docker Compose** installed
- **Git** (to clone/manage the repository)

That's it! Everything else runs in containers.

---

## Quick Start (Full Dataset)

### 1. Navigate to the project directory
```bash
cd /path/to/test-catherine
```

### 2. Create the `.env` configuration file
```bash
cat > .env << 'EOF'
# Database Configuration
DB_HOST=postgres
DB_PORT=5432
DB_NAME=efo_data
DB_USER=efo_user
DB_PASSWORD=efo_secure_password_2025

# OLS API Configuration
OLS_BASE_URL=https://www.ebi.ac.uk/ols4/api
OLS_REQUEST_DELAY=0.05

# Pipeline Configuration
BATCH_SIZE=250
LOG_LEVEL=INFO

# Execution Mode: FULL dataset (or 'test' for 100 records only)
EXECUTION_MODE=full

# Record Limit (only used in test mode)
RECORD_LIMIT=100
EOF
```

### 3. Run the pipeline
```bash
docker compose up --build
```

**That's it!** The pipeline will automatically:
- ✅ Start PostgreSQL database
- ✅ Initialize the database schema
- ✅ Fetch all **88,707 EFO terms** from the OLS API
- ✅ Extract **49,294 synonyms**
- ✅ Extract **104,158 parent relationships** (is_a links)
- ✅ Extract **8,736 MeSH cross-references**

**⏱️ Total time: ~7-10 minutes**

---

## Test Mode (100 Records Only)

If you want to test with just 100 records first:

```bash
# Update the .env file
sed -i '' 's/EXECUTION_MODE=full/EXECUTION_MODE=test/' .env

# Or manually edit .env and change:
# EXECUTION_MODE=test

# Run
docker compose up --build
```

**Test mode time: ~30 seconds**

---

## Accessing the Data

### Option 1: Via Command Line

```bash
# Connect to PostgreSQL
docker compose exec postgres psql -U efo_user -d efo_data

# Sample queries:
SELECT COUNT(*) FROM efo_terms;
SELECT COUNT(*) FROM efo_synonyms;
SELECT COUNT(*) FROM efo_relationships;
SELECT COUNT(*) FROM mesh_cross_references;
```

### Option 2: Via pgAdmin (GUI)

Connect with these credentials:
- **Host:** `localhost`
- **Port:** `5433`
- **Database:** `efo_data`
- **Username:** `efo_user`
- **Password:** `efo_secure_password_2025`

---

## Stopping and Cleaning Up

```bash
# Stop the containers (keeps data)
docker compose down

# Stop and remove all data (fresh start)
docker compose down -v
```

---

## Incremental Updates

To run incremental updates (only fetch changed terms):

```bash
# Update .env
# Change: EXECUTION_MODE=incremental

# Run again
docker compose up
```

The pipeline will:
- Compare content hashes of existing terms
- Only update changed terms
- Skip unchanged terms for efficiency

---

## Quick Reference Commands

| Command | Purpose |
|---------|---------|
| `docker compose up --build` | Start the pipeline |
| `docker compose down` | Stop containers |
| `docker compose down -v` | Stop and delete all data |
| `docker compose logs pipeline` | View pipeline logs |
| `docker compose logs -f pipeline` | Follow pipeline logs in real-time |
| `docker compose exec postgres psql -U efo_user -d efo_data` | Connect to database |

---

## Verification Queries

Once the pipeline completes, verify the data:

```sql
-- Count records in each table
SELECT 
  'efo_terms' as table_name, COUNT(*) as count 
FROM efo_terms
UNION ALL
SELECT 'efo_synonyms', COUNT(*) FROM efo_synonyms
UNION ALL
SELECT 'efo_relationships', COUNT(*) FROM efo_relationships
UNION ALL
SELECT 'mesh_cross_references', COUNT(*) FROM mesh_cross_references
ORDER BY table_name;

-- Sample data with parent relationships
SELECT 
  child.term_id,
  child.label,
  parent.label as parent_label
FROM efo_relationships r
JOIN efo_terms child ON r.child_id = child.id
JOIN efo_terms parent ON r.parent_id = parent.id
LIMIT 5;

-- Sample MeSH cross-references
SELECT 
  t.term_id,
  t.label,
  m.mesh_id,
  m.database
FROM mesh_cross_references m
JOIN efo_terms t ON m.term_id = t.id
LIMIT 5;

-- Sample term with all related data
SELECT 
  t.term_id,
  t.label,
  t.description,
  COUNT(DISTINCT s.synonym) as synonym_count,
  COUNT(DISTINCT r.parent_id) as parent_count,
  COUNT(DISTINCT m.mesh_id) as mesh_xref_count
FROM efo_terms t
LEFT JOIN efo_synonyms s ON t.id = s.term_id
LEFT JOIN efo_relationships r ON t.id = r.child_id
LEFT JOIN mesh_cross_references m ON t.id = m.term_id
WHERE t.term_id = 'EFO:0010642'
GROUP BY t.id, t.term_id, t.label, t.description;
```

---



---

## Troubleshooting

### Issue: Port 5432 already in use
**Solution:** The external port is mapped to 5433 in docker-compose.yml to avoid conflicts.

### Issue: Pipeline fails to connect to database
**Solution:** Wait for PostgreSQL health check to pass. The pipeline automatically waits for the database to be ready.

### Issue: Want to see real-time logs
**Solution:** Use `docker compose logs -f pipeline`

### Issue: Need to restart with fresh data
**Solution:** 
```bash
docker compose down -v
docker compose up --build
```

---

## Architecture Overview

The pipeline consists of three main phases:

1. **Phase 1**: Extract and load EFO terms and synonyms from the OLS API
   - Fetches terms in batches of 250
   - Bulk inserts into PostgreSQL
   - Collects parent URLs for phase 1.5

2. **Phase 1.5**: Asynchronous batch fetching of parent relationships
   - Uses async HTTP requests (50 concurrent connections)
   - Fetches actual parent IRIs from parent URLs
   - Optimized for performance (~7 minutes for full dataset)

3. **Phase 2**: Process relationships and MeSH cross-references
   - Builds IRI-to-ID mapping
   - Inserts parent-child relationships
   - Inserts MeSH cross-references

---

## Data Quality Features

- ✅ **Normalized database schema (3NF)** - Eliminates data duplication
- ✅ **Referential integrity** - Foreign keys enforce valid relationships
- ✅ **Idempotent pipeline** - Can run multiple times safely
- ✅ **Bulk operations** - Efficient batch inserts (250 records/batch)
- ✅ **Error handling** - Robust logging and transaction rollback
- ✅ **Change detection** - SHA-256 hashing for incremental updates
- ✅ **Data validation** - Filters invalid/empty values

---

## Performance Notes

- **Full dataset (88,707 terms)**: ~7-10 minutes
- **Test dataset (100 terms)**: ~30 seconds
- **Async parent fetching**: 50 concurrent connections for optimal speed
- **Bulk inserts**: 250 records per batch for efficiency

---

## Support

For questions or issues:
1. Check the logs: `docker compose logs pipeline`
2. Verify database connection: `docker compose exec postgres psql -U efo_user -d efo_data -c "SELECT version();"`
3. Review the README.md for detailed documentation

---



**Last Updated**: October 2025

