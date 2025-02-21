import sqlite3
import pandas as pd
import logging
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

class DataWarehouse:
    def __init__(self, source_db_path: str, sql_script_path: str, target_db_path: str):
        """Initialize the Data Warehouse with database paths"""
        self.source_db_path = source_db_path
        self.sql_script_path = sql_script_path
        self.target_db_path = target_db_path

        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

        # Define the initial tables to create
        self.initial_tables = [
            'conversions',
            'session_costs',
            'session_sources'
        ]

    def read_sql_script(self) -> str:
        """Read and clean the SQL create statements from file"""
        try:
            with open(self.sql_script_path, 'r') as file:
                content = file.read()
                # Remove the Python-style comment header
                if content.startswith('#'):
                    content = content[content.find('CREATE TABLE'):]
                return content
        except Exception as e:
            self.logger.error(f"Error reading SQL script: {str(e)}")
            raise

    def create_initial_schema(self) -> None:
        """Create only the initial tables schema using the SQL script"""
        try:
            create_statements = self.read_sql_script()
            with sqlite3.connect(self.target_db_path) as target_conn:
                cursor = target_conn.cursor()
                statements = [
                    stmt.strip() for stmt in create_statements.split(';')
                    if 'CREATE TABLE' in stmt and
                    any(table in stmt for table in self.initial_tables) or 'attribution_customer_journey' in stmt
                ]
                for statement in statements:
                    try:
                        cursor.execute(statement)
                        table_name = statement[statement.find('EXISTS') + 7:].split()[0]
                        self.logger.info(f"Created table: {table_name}")
                    except sqlite3.Error as e:
                        self.logger.error(f"Error creating table: {str(e)}")
                        raise
                target_conn.commit()
                self.logger.info("Initial schema created successfully")
        except Exception as e:
            self.logger.error(f"Error in create_initial_schema: {str(e)}")
            raise

    def copy_initial_data(self) -> None:
        """Copy initial data from source database to target database for tables with data"""
        try:
            with sqlite3.connect(self.source_db_path) as source_conn, \
                 sqlite3.connect(self.target_db_path) as target_conn:

                for table_name in self.initial_tables:
                    try:
                        df = pd.read_sql_query(f"SELECT * FROM {table_name}", source_conn)
                        df.to_sql(table_name, target_conn, if_exists='replace', index=False)
                        self.logger.info(f"Copied {len(df)} rows to table {table_name}")
                    except Exception as e:
                        self.logger.error(f"Error copying table {table_name}: {str(e)}")
                        raise
                target_conn.commit()
        except Exception as e:
            self.logger.error(f"Error in copy_initial_data: {str(e)}")
            raise

    def verify_data(self) -> bool:
        """Verify that data was copied correctly for tables with initial data"""
        try:
            all_match = True
            with sqlite3.connect(self.source_db_path) as source_conn, \
                 sqlite3.connect(self.target_db_path) as target_conn:

                for table in self.initial_tables:
                    source_count = pd.read_sql_query(f"SELECT COUNT(*) as count FROM {table}", source_conn).iloc[0]['count']
                    target_count = pd.read_sql_query(f"SELECT COUNT(*) as count FROM {table}", target_conn).iloc[0]['count']
                    match = source_count == target_count
                    all_match &= match
                    self.logger.info(f"Table {table}: Source={source_count}, Target={target_count}, Match={'✓' if match else '✗'}")
            return all_match
        except Exception as e:
            self.logger.error(f"Error in verify_data: {str(e)}")
            raise
