# Connect to PostgreSQL and drop/recreate databases
psql -h localhost -U postgres -d postgres -c "DROP DATABASE IF EXISTS \"mtg-meta-mage-db\";"
psql -h localhost -U postgres -d postgres -c "DROP DATABASE IF EXISTS \"mtg-meta-mage-db-test\";"

# Then recreate using your existing init script
uv run python src/etl/database/init_db.py