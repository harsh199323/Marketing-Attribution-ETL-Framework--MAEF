# import sqlite3
# import logging
# import json
# import pandas as pd
# import numpy as np
# import math
# import os
# from datetime import datetime, timedelta
# from typing import Optional, List, Dict, Union

# class DataTransformer:
#     def __init__(self, db_path: str):
#         """Initialize the transformer with the target database path"""
#         self.db_path = db_path
#         self.transformed_data: List[Dict[str, Union[str, int]]] = []
        
#         logging.basicConfig(
#             level=logging.INFO,
#             format='%(asctime)s - %(levelname)s - %(message)s'
#         )
#         self.logger = logging.getLogger(__name__)

#     def clean_float(self, value: Union[float, np.float64, int]) -> Union[float, int]:
#         """Clean float values to ensure JSON compatibility"""
#         if isinstance(value, (float, np.float64)):
#             if math.isnan(value) or math.isinf(value):
#                 return 0.0
#             return float(value)
#         return value

#     def clean_dict(self, d: Dict) -> Dict:
#         """Clean all float values in a dictionary"""
#         return {k: self.clean_float(v) for k, v in d.items()}

#     def transform_data(self, output_path: str, start_date: Optional[str] = None, 
#                       end_date: Optional[str] = None) -> list:
#         """
#         Perform data transformation with optional date range filtering
        
#         Args:
#             output_path (str): Path where to save the transformed data
#             start_date (str, optional): Start date in YYYY-MM-DD format
#             end_date (str, optional): End date in YYYY-MM-DD format
            
#         Returns:
#             list: The transformed data
#         """
#         try:
#             with sqlite3.connect(self.db_path) as conn:
#                 # Initialize filter variables
#                 conv_filter = ""
#                 conv_params = []
                
#                 # Add buffer to conversion date range
#                 if start_date and end_date:
#                     conv_start = (datetime.strptime(start_date, '%Y-%m-%d') 
#                                 - timedelta(days=45)).strftime('%Y-%m-%d')
#                     conv_end = (datetime.strptime(end_date, '%Y-%m-%d') 
#                               + timedelta(days=1)).strftime('%Y-%m-%d')
#                     conv_filter = "WHERE conv_date BETWEEN ? AND ?"
#                     conv_params = [conv_start, conv_end]

#                 # Load session sources first
#                 session_sources_df = pd.read_sql_query(
#                     "SELECT * FROM session_sources", 
#                     conn
#                 )
                
#                 # Then validate session data
#                 session_sources_df = session_sources_df[
#                     (session_sources_df['channel_name'].notnull()) &
#                     (session_sources_df['event_date'].notnull())
#                 ]

#                 # For conversions, include buffer period for attribution
#                 conv_filter = ""
#                 conv_params: List[str] = []
#                 if start_date and end_date:
#                     buffer_days = 7  # Attribution window
#                     conv_start = (
#                         datetime.strptime(start_date, '%Y-%m-%d') - 
#                         timedelta(days=buffer_days)
#                     ).strftime('%Y-%m-%d')
#                     conv_end = (
#                         datetime.strptime(end_date, '%Y-%m-%d') + 
#                         timedelta(days=1)
#                     ).strftime('%Y-%m-%d')
#                     conv_filter = "WHERE conv_date BETWEEN ? AND ?"
#                     conv_params = [conv_start, conv_end]

#                 # Load conversion data with date filter
#                 conv_query = f"SELECT * FROM conversions {conv_filter}"
#                 conversions_df = pd.read_sql_query(
#                     conv_query, conn, params=conv_params
#                 )

#                 self.logger.info(
#                     f"Loaded {len(session_sources_df)} sessions and "
#                     f"{len(conversions_df)} conversions for date range: "
#                     f"{start_date or 'ALL'} to {end_date or 'ALL'}"
#                 )

#                 # Reset transformed data for new processing
#                 self.transformed_data = []

#                 # Process each conversion
#                 for _, conversion in conversions_df.iterrows():
#                     conv_id = conversion['conv_id']
#                     user_id = conversion['user_id']
#                     conv_timestamp = f"{conversion['conv_date']} {conversion['conv_time']}"

#                     # Get user sessions before conversion with 30-day lookback
#                     user_sessions = session_sources_df[
#                         (session_sources_df['user_id'] == user_id) &
#                         (pd.to_datetime(session_sources_df['event_date']) >= 
#                          pd.to_datetime(conversion['conv_date']) - pd.DateOffset(days=30)) &
#                         (pd.to_datetime(session_sources_df['event_date']) <= 
#                          pd.to_datetime(conversion['conv_date']))
#                     ]

#                     # Create journey entries for each session
#                     for _, session in user_sessions.iterrows():
#                         session_timestamp = (f"{session['event_date']} "
#                                              f"{session['event_time']}")
                        
#                         # Check if session led to conversion
#                         is_conversion = int(session_timestamp == conv_timestamp)
                        
#                         journey_entry = {
#                             "conversion_id": str(conv_id),
#                             "session_id": str(session['session_id']),
#                             "timestamp": session_timestamp,
#                             "channel_label": str(session['channel_name']),
#                             "holder_engagement": int(session['holder_engagement']),
#                             "closer_engagement": int(session['closer_engagement']),
#                             "conversion": is_conversion,
#                             "impression_interaction": int(session['impression_interaction'])
#                         }
                        
#                         # Clean and add journey entry
#                         self.transformed_data.append(self.clean_dict(journey_entry))

#                 self.logger.info(f"Created {len(self.transformed_data)} journey entries")
                
#                 if not self.transformed_data:
#                     raise ValueError("No journey entries were created")
                
#                 # Save the transformed data
#                 self.save_transformed_data(output_path)
                
#                 return self.transformed_data

#         except Exception as e:
#             self.logger.error(f"Error in transform_data: {str(e)}")
#             raise

#     def save_transformed_data(self, output_path: str) -> None:
#         """Save transformed data to a JSON file."""
#         try:
#             # Ensure the directory exists
#             os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
#             self.logger.info(f"Saving transformed data to {output_path}")
#             with open(output_path, 'w') as json_file:
#                 json.dump(self.transformed_data, json_file, indent=4)
#             self.logger.info(f"Transformed data saved successfully to {output_path}")
#         except Exception as e:
#             self.logger.error(f"Error in save_transformed_data: {str(e)}")
#             raise



import sqlite3
import logging
import json
import pandas as pd
import numpy as np
import math
import os
from typing import Optional

class DataTransformer:
    def __init__(self, db_path: str):
        """Initialize the transformer with the target database path"""
        self.db_path = db_path
        self.transformed_data = []
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def clean_float(self, value):
        """Clean float values to ensure JSON compatibility"""
        if isinstance(value, (float, np.float64)):
            if math.isnan(value) or math.isinf(value):
                return 0.0
            return float(value)
        return value

    def clean_dict(self, d):
        """Clean all float values in a dictionary"""
        return {k: self.clean_float(v) for k, v in d.items()}

    def transform_data(self, output_path: str, 
                       start_date: Optional[str] = None, 
                       end_date: Optional[str] = None) -> list:
            """
        Perform data transformation to match the required format exactly
        
        Args:
            output_path (str): Path where to save the transformed data
            start_date (str, optional): Start date in YYYY-MM-DD format
            end_date (str, optional): End date in YYYY-MM-DD format
            
        Returns:
            
            """
            try:
                with sqlite3.connect(self.db_path) as conn:
                    self.logger.info("Loading data from database...")
                    session_sources_df = pd.read_sql_query("SELECT * FROM session_sources", conn)
                    conversions_df = pd.read_sql_query("SELECT * FROM conversions", conn)

                    self.logger.info(f"Loaded {len(session_sources_df)} sessions and {len(conversions_df)} conversions")

                    # Process each conversion
                    for _, conversion in conversions_df.iterrows():
                        conv_id = conversion['conv_id']
                        user_id = conversion['user_id']
                        conv_timestamp = f"{conversion['conv_date']} {conversion['conv_time']}"

                        # Get all sessions for the given user_id that happened before the conversion timestamp
                        user_sessions = session_sources_df[
                            (session_sources_df['user_id'] == user_id) &
                            (session_sources_df['event_date'] + ' ' + session_sources_df['event_time'] < conv_timestamp)
                        ]

                        # For each session in the user's journey
                        for _, session in user_sessions.iterrows():
                            session_timestamp = f"{session['event_date']} {session['event_time']}"
                            
                            # Determine if this session resulted in conversion
                            is_conversion = 1 if session_timestamp == conv_timestamp else 0
                            
                            journey_entry = {
                                "conversion_id": str(conv_id),
                                "session_id": str(session['session_id']),
                                "timestamp": session_timestamp,
                                "channel_label": str(session['channel_name']),
                                "holder_engagement": int(session['holder_engagement']),
                                "closer_engagement": int(session['closer_engagement']),
                                "conversion": is_conversion,
                                "impression_interaction": int(session['impression_interaction'])
                            }
                            
                            # Clean any potential problematic float values
                            journey_entry = self.clean_dict(journey_entry)
                            self.transformed_data.append(journey_entry)

                    self.logger.info(f"Created {len(self.transformed_data)} journey entries")
                    
                    if not self.transformed_data:
                        raise ValueError("No journey entries were created")
                    
                    # Save the transformed data
                    self.save_transformed_data(output_path)
                    
                    return self.transformed_data

            except Exception as e:
                self.logger.error(f"Error in transform_data: {str(e)}")
                raise

    def save_transformed_data(self, output_path: str) -> None:
        """Save transformed data to a JSON file."""
        try:
            # Ensure the directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            self.logger.info(f"Saving transformed data to {output_path}")
            with open(output_path, 'w') as json_file:
                json.dump(self.transformed_data, json_file, indent=4)
            self.logger.info(f"Transformed data saved successfully to {output_path}")
        except Exception as e:
            self.logger.error(f"Error in save_transformed_data: {str(e)}")
            raise