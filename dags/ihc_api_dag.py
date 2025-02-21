import os
import sys
import sqlite3
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

# Add project root to Python path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

# Import custom modules
from src.api.ihc_api import IHCApiClient
from src.database.db_utils import DataWarehouse
from src.etl.transform import DataTransformer
from src.etl.load import DataLoader
from src.etl.reporting import ChannelReporting

logger = logging.getLogger(__name__)

def get_airflow_data_dir():
    """Get Airflow data directory with proper permissions"""
    base_dir = os.path.expanduser('/mnt/d/github/haensel-ams-data-engineering/recruitment_challenge/Data_Engineering_202309/solution/data')
    return Path(base_dir)

def verify_path_exists(path, create=True):
    """Verify path exists and optionally create it"""
    path = Path(path)
    if not path.exists() and create:
        path.mkdir(parents=True, exist_ok=True)
        os.chmod(path, 0o755)
    return path

def get_date_range() -> tuple[datetime, datetime]:
    """Get date range from Airflow variables or default to last 30 days"""
    try:
        start_date = Variable.get("attribution_start_date")
        end_date = Variable.get("attribution_end_date")
        return (
            datetime.strptime(start_date, "%Y-%m-%d"),
            datetime.strptime(end_date, "%Y-%m-%d")
        )
    except Exception as e:
        logger.info("Using default date range (last 30 days) because of: " + str(e))
        return (
            datetime(2023, 8, 1),
            datetime(2023, 9, 30)
        )

def validate_date_range(start_date: datetime, end_date: datetime) -> None:
    """Validate the date range"""
    if start_date > end_date:
        raise ValueError("Start date cannot be after end date")
    if end_date > datetime.now():
        raise ValueError("End date cannot be in the future")
    if (end_date - start_date).days > 365:
        raise ValueError("Date range cannot exceed 1 year")

def setup_database(**context):
    """Initialize database and set up directories with date filtering"""
    start_date, end_date = get_date_range()
    validate_date_range(start_date, end_date)
    
    ti = context['task_instance']
    ti.xcom_push(key='start_date', value=start_date.strftime("%Y-%m-%d"))
    ti.xcom_push(key='end_date', value=end_date.strftime("%Y-%m-%d"))
    
    data_dir = get_airflow_data_dir()
    directories = {
        'input': data_dir / 'input',
        'output': data_dir / 'output',
        'sql': data_dir / 'sql'
    }
    
    for dir_name, dir_path in directories.items():
        verify_path_exists(dir_path)
        logger.info(f"Verified {dir_name} directory at {dir_path}")
    
    source_db_path = str(directories['input'] / 'challenge.db')
    sql_script_path = str(directories['sql'] / 'challenge_db_create.sql')
    target_db_path = str(directories['output'] / f"target_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.db")
    
    ti.xcom_push(key='target_db_path', value=target_db_path)
    
    warehouse = DataWarehouse(source_db_path, sql_script_path, target_db_path)
    warehouse.create_initial_schema()
    warehouse.copy_initial_data()
    
    if not warehouse.verify_data():
        raise Exception("Data verification failed after initial setup")
    
    logger.info(f"Database setup completed for date range {start_date} to {end_date}. Target DB path: {target_db_path}")

def transform_data(**context):
    """Transform data with date filtering and prepare for API submission"""
    ti = context['task_instance']
    
    target_db_path = ti.xcom_pull(task_ids='setup_database', key='target_db_path')
    if not target_db_path:
        raise ValueError("Missing target_db_path from setup_database task")
    
    start_date = ti.xcom_pull(task_ids='setup_database', key='start_date')
    end_date = ti.xcom_pull(task_ids='setup_database', key='end_date')
    
    if not os.path.exists(target_db_path):
        raise FileNotFoundError(f"Target database not found at: {target_db_path}")
    
    data_dir = get_airflow_data_dir()
    target_data_path = str(data_dir / 'output' / f"transformed_data_{start_date}_{end_date}.json")
    
    transformer = DataTransformer(target_db_path)
    transformer.transform_data(
        output_path=target_data_path,
        start_date=start_date,
        end_date=end_date
    )
    
    if not os.path.exists(target_data_path):
        raise FileNotFoundError(f"Transformed data file was not created at: {target_data_path}")
    
    ti.xcom_push(key='transformed_data_path', value=target_data_path)
    logger.info(f"Data transformed and saved to {target_data_path}")

def send_data_to_api(**context):
    """Send transformed data to API using configuration from Airflow Variables"""
    ti = context['task_instance']
    
    transformed_data_path = ti.xcom_pull(task_ids='transform_data', key='transformed_data_path')
    if not transformed_data_path:
        raise ValueError("Missing transformed_data_path from transform_data task")
    
    # Get config from environment variables
    api_endpoint = os.getenv("IHC_API_URL")
    api_key = os.getenv("IHC_API_KEY")
    conv_type_id = os.getenv("CONV_TYPE_ID")
    
    if not all([api_endpoint, api_key, conv_type_id]):
        missing = [var for var in ['IHC_API_URL', 'IHC_API_KEY', 'CONV_TYPE_ID'] 
                  if not os.getenv(var)]
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    
    # Create API client
    client = IHCApiClient(api_endpoint, api_key)
    
    # Generate response file path
    response_dir = os.path.dirname(transformed_data_path)
    response_filename = f"api_response_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    api_response_path = os.path.join(response_dir, response_filename)
    
    try:
        # Send data to API
        responses = client.send_transformed_data(transformed_data_path, conv_type_id)
        
        # Save API response to file
        with open(api_response_path, 'w') as f:
            json.dump(responses, f, indent=2)
            
        # Verify file was created and has content
        if not os.path.exists(api_response_path):
            raise FileNotFoundError(f"Failed to create API response file at {api_response_path}")
            
        if os.path.getsize(api_response_path) == 0:
            raise ValueError("API response file was created but is empty")
            
        # Push path to XCom
        ti.xcom_push(key='api_response_path', value=api_response_path)
        logger.info(f"API response saved to {api_response_path}")
        
    except Exception as e:
        logger.error(f"Error in send_data_to_api: {str(e)}")
        raise
    
    return api_response_path

def load_attribution_results(**context):
    """Load API attribution results into database"""
    ti = context['task_instance']
    
    # Get paths with detailed error messages
    target_db_path = ti.xcom_pull(task_ids='setup_database', key='target_db_path')
    if not target_db_path:
        raise ValueError("Missing target_db_path from setup_database task")
    
    # Get API response path and validate
    api_response_path = ti.xcom_pull(task_ids='send_data_to_api', key='api_response_path')
    logger.info(f"Retrieved api_response_path from XCom: {api_response_path}")
    
    if not api_response_path:
        # Check what was actually pushed to XCom
        all_xcoms = ti.xcom_pull(task_ids='send_data_to_api')
        logger.error(f"Available XComs from send_data_to_api: {all_xcoms}")
        raise ValueError("Missing api_response_path from send_data_to_api task")
    
    if not os.path.exists(api_response_path):
        raise FileNotFoundError(f"API response file not found at: {api_response_path}")
    
    if os.path.getsize(api_response_path) == 0:
        raise ValueError(f"API response file is empty: {api_response_path}")
    
    # Load and validate response data
    try:
        with open(api_response_path, 'r') as f:
            response_data = json.load(f)
        
        if not isinstance(response_data, list):
            raise ValueError("API response data is not in expected format (list)")
            
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in API response file: {str(e)}")
    
    # Process the data
    loader = DataLoader(target_db_path)
    try:
        loader.load_attribution_results(api_response_path)
        if not loader.verify_attribution_data():
            raise Exception("Attribution data loading failed verification")
    except Exception as e:
        logger.error(f"Error loading attribution results: {str(e)}")
        raise
    
    logger.info("Attribution results loaded successfully")

def create_channel_report(**context):
    """Create and export the final channel report with date-specific naming"""
    ti = context['task_instance']
    
    target_db_path = ti.xcom_pull(task_ids='setup_database', key='target_db_path')
    if not target_db_path:
        raise ValueError("Missing target_db_path from setup_database task")
    
    data_dir = get_airflow_data_dir()
    start_date = ti.xcom_pull(task_ids='setup_database', key='start_date')
    end_date = ti.xcom_pull(task_ids='setup_database', key='end_date')
    
    output_csv = str(data_dir / 'output' / f"channel_report_{start_date}_{end_date}.csv")
    
    reporter = ChannelReporting(target_db_path)
    reporter.create_reporting_table()
    reporter.aggregate_channel_metrics()
    reporter.export_channel_report(output_csv)
    
    logger.info(f"Channel report created at {output_csv}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ihc_api_etl',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    params={
        'start_date': {
            'type': 'string',
            'format': 'date',
            'default': "2023-08-01"
        },
        'end_date': {
            'type': 'string',
            'format': 'date',
            'default': "2023-09-30"
        }
    }
) as dag:
    
    task_setup_database = PythonOperator(
        task_id='setup_database',
        python_callable=setup_database,
        provide_context=True
    )
    
    task_transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True
    )
    
    task_send_data_to_api = PythonOperator(
        task_id='send_data_to_api',
        python_callable=send_data_to_api,
        provide_context=True
    )
    
    task_load_attribution_results = PythonOperator(
        task_id='load_attribution_results',
        python_callable=load_attribution_results,
        provide_context=True
    )
    
    task_create_channel_report = PythonOperator(
        task_id='create_channel_report',
        python_callable=create_channel_report,
        provide_context=True
    )
    
    # Set up task dependencies
    task_setup_database >> task_transform_data >> task_send_data_to_api >> task_load_attribution_results >> task_create_channel_report