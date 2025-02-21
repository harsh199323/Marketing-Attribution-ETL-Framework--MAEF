import os
import logging
from dotenv import load_dotenv
from src.api.ihc_api import IHCApiClient
from src.database.db_utils import DataWarehouse
from src.etl.transform import DataTransformer
from src.etl.load import DataLoader
from src.etl.reporting import ChannelReporting

# Load environment variables from .env file
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    try:
        # Define file paths
        source_db_path = 'data/input/challenge.db'
        sql_script_path = 'data/sql/challenge_db_create.sql'
        target_db_path = 'data/output/target.db'
        transformed_data_path = 'data/output/target_data.json'
        api_response_path = 'data/output/api_response.json'
        channel_report_path = 'data/output/channel_report.csv'

        # Load API configuration from environment variables
        api_endpoint = os.getenv('IHC_API_URL')
        api_key = os.getenv('IHC_API_KEY')
        conv_type_id = os.getenv('Conv_Type_Id')

        # Validate environment variables
        if not all([api_endpoint, api_key, conv_type_id]):
            raise ValueError("Missing required environment variables. Check .env file.")

        logger.info("Starting ETL pipeline...")

        # Step 1: Set up the SQLite database
        logger.info("Step 1: Setting up SQLite database...")
        warehouse = DataWarehouse(source_db_path, sql_script_path, target_db_path)
        warehouse.create_initial_schema()
        warehouse.copy_initial_data()
        if not warehouse.verify_data():
            raise Exception("Data verification failed after initial setup")
        logger.info("Database setup completed successfully")

        # Step 2: Transform data
        logger.info("Step 2: Transforming data...")
        transformer = DataTransformer(target_db_path)
        transformer.transform_data()  # This saves to target_data.json
        logger.info("Data transformation completed")

        # Step 3: Send data to IHC API
        logger.info("Step 3: Sending data to IHC API...")
        api_client = IHCApiClient(api_endpoint, api_key)
        api_response = api_client.send_transformed_data(transformed_data_path, conv_type_id)
        api_client.save_response(api_response, api_response_path)
        logger.info("API integration completed")

        # Step 4: Load attribution results
        logger.info("Step 4: Loading attribution results...")
        loader = DataLoader(target_db_path)
        loader.load_attribution_results(api_response_path)
        if not loader.verify_attribution_data():
            raise Exception("Attribution data loading failed verification")
        logger.info("Attribution results loaded successfully")

        # Step 5: Create and populate channel reporting
        logger.info("Step 5: Creating channel reporting...")
        reporting = ChannelReporting(target_db_path)
        reporting.create_reporting_table()
        reporting.aggregate_channel_metrics()
        reporting.export_channel_report(channel_report_path)
        logger.info("Channel reporting completed successfully")

        logger.info("ETL pipeline completed successfully")

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
