# Database Migration

This script migrates Arcadia user account production database tables to a staging database on the same system.

The script runs in the following steps:
1. Create staging database
2. Dump production database schemas
3. Load production schemas into the staging database
4. Iterate through records in the production database, redact personally identifiable information (PII) and load into staging database tables

The script accepts two required command line arguments, `--in` and `--out`, the names of the production and staging databases, respectively. Note that the code will create the staging database, and the production database must exist.  

To run the migration code:
1. Navigate to the arcadia_db_migration folder
```cd arcadia_db_migration```

2. Install dependencies
```pip install -r requirements.txt```

3. Modify the configuration ini file `config.ini` with the production database login credentials.

4. Navigate to the code directory
```cd code```

5. Finally, call the migration script with command line arguments
```python migrate_db.py --in {production db name} --out {staging db name}```