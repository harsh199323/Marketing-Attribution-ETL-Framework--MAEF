import sqlite3
import logging
import pandas as pd
from datetime import datetime

class ChannelReporting:
    def __init__(self, db_path: str):
        """Initialize with database path"""
        self.db_path = db_path
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def create_reporting_table(self) -> None:
        """Create the channel_reporting table if it doesn't exist"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS channel_reporting (
                        channel_name TEXT NOT NULL,
                        date TEXT NOT NULL,
                        cost REAL NOT NULL,
                        ihc REAL NOT NULL,
                        ihc_revenue REAL NOT NULL,
                        PRIMARY KEY(channel_name, date)
                    )
                ''')
                conn.commit()
                self.logger.info("Channel reporting table created successfully")
        except Exception as e:
            self.logger.error(f"Error creating channel_reporting table: {str(e)}")
            raise

    def aggregate_channel_metrics(self) -> None:
        """
        Aggregate data from all tables to create channel-wise reporting metrics
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Check if we have attribution data
                attribution_count = conn.execute(
                    'SELECT COUNT(*) FROM attribution_customer_journey'
                ).fetchone()[0]
                
                if attribution_count == 0:
                    self.logger.warning("No attribution data found, aggregation will result in empty report")
                
                # Clear existing data
                conn.execute('DELETE FROM channel_reporting')
                conn.commit()

                # Updated query with better null handling and filtering in CTEs
                query = """
                WITH attribution_revenue AS (
                    SELECT 
                        COALESCE(NULLIF(ss.channel_name, ''), 'unknown') as channel_name,
                        COALESCE(NULLIF(ss.event_date, ''), c.conv_date) as date,
                        COALESCE(acj.ihc, 0) as ihc,
                        COALESCE(c.revenue, 0) * COALESCE(acj.ihc, 0) as attributed_revenue
                    FROM attribution_customer_journey acj
                    INNER JOIN session_sources ss 
                        ON ss.session_id = acj.session_id
                    INNER JOIN conversions c 
                        ON acj.conv_id = c.conv_id
                    WHERE acj.session_id IS NOT NULL 
                      AND acj.session_id != ''
                      AND ss.channel_name IS NOT NULL
                      AND ss.event_date IS NOT NULL
                ),
                channel_costs AS (
                    SELECT 
                        COALESCE(NULLIF(ss.channel_name, ''), 'unknown') as channel_name,
                        COALESCE(NULLIF(ss.event_date, ''), '1970-01-01') as date,
                        COALESCE(sc.cost, 0) as cost
                    FROM session_sources ss
                    LEFT JOIN session_costs sc 
                        ON ss.session_id = sc.session_id
                    WHERE ss.channel_name IS NOT NULL
                      AND ss.event_date IS NOT NULL
                )
                SELECT 
                    ar.channel_name,
                    ar.date,
                    COALESCE(SUM(cc.cost), 0) as total_cost,
                    COALESCE(SUM(ar.ihc), 0) as total_ihc,
                    COALESCE(SUM(ar.attributed_revenue), 0) as total_revenue
                FROM attribution_revenue ar
                LEFT JOIN channel_costs cc 
                    ON ar.channel_name = cc.channel_name 
                    AND ar.date = cc.date
                GROUP BY ar.channel_name, ar.date
                HAVING ar.channel_name != 'unknown' 
                   AND ar.date >= (SELECT MIN(conv_date) FROM conversions)
                """

                # Execute query and get results
                df = pd.read_sql_query(query, conn)

                # Check if aggregation produced any results
                if df.empty:
                    self.logger.error("No channel metrics could be aggregated - empty result set")
                    raise ValueError("Channel aggregation produced no results")

                # Insert aggregated data
                for _, row in df.iterrows():
                    conn.execute('''
                        INSERT INTO channel_reporting 
                        (channel_name, date, cost, ihc, ihc_revenue)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (
                        row['channel_name'],
                        row['date'],
                        row['total_cost'],
                        row['total_ihc'],
                        row['total_revenue']
                    ))
                conn.commit()

                # Log summary statistics
                summary = pd.read_sql_query('''
                    SELECT 
                        COUNT(*) as record_count,
                        COUNT(DISTINCT channel_name) as channel_count,
                        COUNT(DISTINCT date) as date_count
                    FROM channel_reporting
                ''', conn)
                
                self.logger.info(
                    f"Successfully aggregated channel metrics: "
                    f"{summary.iloc[0]['record_count']} records for "
                    f"{summary.iloc[0]['channel_count']} channels across "
                    f"{summary.iloc[0]['date_count']} dates"
                )

        except Exception as e:
            self.logger.error(f"Error aggregating channel metrics: {str(e)}")
            raise

    def export_channel_report(self, output_path: str) -> None:
        """
        Export channel reporting data to CSV with additional metrics
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Query data with computed metrics
                query = """
                SELECT 
                    channel_name,
                    date,
                    cost,
                    ihc,
                    ihc_revenue,
                    CASE 
                        WHEN ihc > 0 THEN cost / ihc 
                        ELSE 0 
                    END as cpo,
                    CASE 
                        WHEN cost > 0 THEN ihc_revenue / cost 
                        ELSE 0 
                    END as roas
                FROM channel_reporting
                ORDER BY channel_name, date
                """
                
                df = pd.read_sql_query(query, conn)
                df.to_csv(output_path, index=False)
                
                self.logger.info(f"Channel report exported successfully to {output_path}")
                
        except Exception as e:
            self.logger.error(f"Error exporting channel report: {str(e)}")
            raise
