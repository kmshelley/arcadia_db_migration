"""
migrate_db.py

Creates and populates a staging PostgreSQL database

De-identifies, and migrates production data
to the staging db.

"""
__author__ = 'Katherine Shelley'

import sys
import os
import subprocess
import shlex
import psycopg2
from configparser import ConfigParser
import argparse

# read in login credentials from conf.ini
c = ConfigParser()
c.read('../config.ini')

HOST     = c['PostgreSQL'].get('host')
PORT     = c['PostgreSQL'].get('port')
USER     = c['PostgreSQL'].get('user')
PASSWORD = c['PostgreSQL'].get('password')


def call_process(cmd):
    '''
    Generic function for calling a subprocess command

    input: the command line string
    
    output: True if created without an error, False otherwise
    '''
    cmds = shlex.split(cmd)
    p = subprocess.Popen(cmds, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()

    if err:
        print(err)
        return False
    return True

def connect_to_psql(db_name, **kwargs):
    """
    Connect to a PostgreSQL database.

    input: 
    db_name - (str) name of the database
    kwargs may contain connection kwargs, otherwise defaults are used

    output: psycopg2 database connection instance

    raises: connection error
    """
    conn = psycopg2.connect(host=kwargs.get('host', HOST), 
                            port=kwargs.get('port', PORT), 
                            user=kwargs.get('user', USER), 
                            password=kwargs.get('password', PASSWORD), 
                            database=db_name)
    return conn

def redact(row, *columns):
    """
    Redacts columns from a SQL table row. Used for removing PII
    "Personally Identifying Information" is defined by this post: https://piwik.pro/blog/what-is-pii-personal-data/
    
    input:
    row - SQL table row, in the form of a tuple
    columns - list of 0-indexed columns to redact

    output:
    new SQL table row in the form of a tuple with columns replaced with 'REDACTED'
    """
    new_row = list(row)
    for i in columns:
        new_row[i] = 'REDACTED'
    return tuple(new_row)


def migrate_db(db_in, db_out):
    """
    Migrates a production PostgreSQL database to a staging db
    1. Creates new staging database 
    2. Creates identical tables in staging database
    3. Iterates through rows of produciton database, cleans and de-identifies data
    4. Loads cleaned, de-id data into staging tables

    Input:
    db_in - (str) The name of the production database
    db_out - (str) The name of the staging database
    """
    # first, create a staging database
    if not call_process('createdb {}'.format(db_out)):
        print('Error creating database {}, exiting...'.format(db_out))
        sys.exit(1)

    # next, dump the production database schemas
    if not call_process('pg_dump --schema-only --format tar -f ../data/schemas.db {}'.format(db_in)):
        print('Error dumping schemas for {}, exiting...'.format(db_in))
        sys.exit(1)

    # next, load the schemas into the staging db
    if not call_process('pg_restore -d {} --schema-only ../data/schemas.db'.format(db_out)):
        print('Error loading schemas for {}, exiting...'.format(db_out))
        sys.exit(1)

    # establish connection with staging db
    staging_conn = connect_to_psql(db_out)
    staging_cursor = staging_conn.cursor()

    # Next, connect to the production database
    prod_conn = connect_to_psql(db_in)
    prod_cursor = prod_conn.cursor()

    # generic add data string
    sql = 'INSERT INTO {} VALUES ({})'

    # clean and migrate the data
    for db in ['account', 'address', 'statement']:
        prod_cursor.execute('SELECT * FROM {}'.format(db))
    
        for row in prod_cursor.fetchall():
            # de-identify the data
            if db == 'account':
                # remove the full name
                new_row = redact(row, 1)
            if db == 'address':
                # remove the street address
                new_row = redact(row, 2)
    
            # format the generic sql command for this row and table
            cmd = sql.format(db, ', '.join(['%s'] * len(new_row)))
            staging_cursor.execute(cmd, new_row)

    staging_conn.commit()

    # close the connection to the databases
    staging_conn.close()
    prod_conn.close()

def parse_args():
    """
    Parse command line arguments for the migration script
    """
    parser = argparse.ArgumentParser(description='Migrate production database to back-up.')
    parser.add_argument('-i', '--in', dest='db_in', required=True, help='Name of the production database to migrate.')
    parser.add_argument('-o', '--out', dest='db_out', required=True, help='Name of the new back-up database to create and load.')
    
    return parser.parse_args()
    
if __name__ == '__main__':
    args = parse_args()
    migrate_db(args.db_in, args.db_out)
