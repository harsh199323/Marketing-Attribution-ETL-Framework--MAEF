import sqlite3
import json
import logging
import os
from collections import defaultdict

class DataLoader:
    def __init__(self, db_path: str):
        self.db_path = db_path
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def load_attribution_results(self, api_response_path: str) -> None:
        try:
            # Check if the file exists
            if not os.path.exists(api_response_path):
                raise FileNotFoundError(f"API response file not found at {api_response_path}")
            
            # Load API responses from file
            with open(api_response_path, 'r') as file:
                api_responses = json.load(file)
            
            # Log the type and length of api_responses for debugging
            self.logger.info(f"Loaded API responses: {type(api_responses)} with {len(api_responses)} elements")
            
            valid_responses = []
            error_counts = defaultdict(int)
            structure_samples = []  # Track samples of invalid structures
            
            # Process each response
            for idx, response in enumerate(api_responses):
                if isinstance(response, dict):
                    # Check for responses with statusCode and value (new structure)
                    if 'statusCode' in response and response['statusCode'] == 200:
                        if 'value' in response and isinstance(response['value'], list):
                            valid_responses.extend(response['value'])
                            # Check for partial failures
                            if 'partialFailureErrors' in response and response['partialFailureErrors']:
                                for err in response['partialFailureErrors']:
                                    error_counts['partial_failure'] += 1
                                    self.logger.warning(f"Partial failure in response {idx}: {err}")
                        else:
                            error_counts['missing_value'] += 1
                            self.logger.warning(f"Response {idx} has status 200 but no 'value' list")
                            if len(structure_samples) < 3:
                                structure_samples.append(response)
                    # Existing checks for 'data' or 'results'
                    elif 'data' in response:
                        valid_responses.extend(response['data'])
                    elif 'results' in response:
                        valid_responses.extend(response['results'])
                    elif 'error' in response:
                        error_counts[response.get('error_code', 'unknown_error')] += 1
                        self.logger.error(f"API error in response {idx}: {response['error']}")
                    else:
                        error_counts['invalid_structure'] += 1
                        if len(structure_samples) < 3:
                            structure_samples.append(response)
                elif isinstance(response, list):
                    valid_responses.extend(response)
                else:
                    error_counts['invalid_type'] += 1
                    if len(structure_samples) < 3:
                        structure_samples.append(response)
            
            # Enhanced error reporting
            if error_counts:
                self.logger.warning(f"API response contained {sum(error_counts.values())} errors:")
                for code, count in error_counts.items():
                    self.logger.warning(f"- {code}: {count} occurrences")
                if structure_samples:
                    sample_msg = "\nInvalid structure examples:\n" + "\n".join(
                        [f"Response {i+1}: {json.dumps(sample)}" 
                         for i, sample in enumerate(structure_samples)]
                    )
                    self.logger.warning(sample_msg)
            
            if not valid_responses:
                raise ValueError(
                    "API response contained no valid attribution data. "
                    f"Diagnostics: {dict(error_counts)}"
                )
            
            # Process valid responses
            attribution_data = []
            seen = set()
            for result in valid_responses:
                # Enhanced validation
                if not all(key in result for key in ('conversion_id', 'session_id', 'ihc')):
                    self.logger.warning("Skipping incomplete entry")
                    continue
                
                # Type enforcement
                entry = {
                    'conv_id': str(result['conversion_id']),
                    'session_id': str(result['session_id']),
                    'ihc': float(result.get('ihc', 0.0))
                }
                
                # Skip entries with invalid IDs
                if not entry['conv_id'] or not entry['session_id']:
                    self.logger.warning("Skipping entry with empty conversion/session ID")
                    continue
                
                key = (entry['conv_id'], entry['session_id'])
                if key in seen:
                    self.logger.warning(f"Duplicate entry detected: {key}")
                    continue
                
                seen.add(key)
                attribution_data.append(entry)
            
            if len(attribution_data) == 0:
                self.logger.error("No valid attribution data to load")
                return
            
            total_ihc = sum(entry['ihc'] for entry in attribution_data)
            if total_ihc <= 0:
                self.logger.error(f"Invalid total IHC value: {total_ihc}")
                raise ValueError("Attribution data contains no valid IHC values")
            
            # Insert into database
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS attribution_customer_journey (
                        conv_id TEXT,
                        session_id TEXT,
                        ihc REAL,
                        UNIQUE(conv_id, session_id) ON CONFLICT REPLACE
                    )
                ''')
                
                conn.executemany(
                    '''INSERT OR REPLACE INTO attribution_customer_journey 
                    (conv_id, session_id, ihc) 
                    VALUES (?, ?, ?)''',
                    [(entry['conv_id'], entry['session_id'], entry['ihc']) 
                     for entry in attribution_data]
                )
                conn.commit()
        
        except Exception as e:
            self.logger.error(f"Error loading attribution results: {str(e)}")
            raise

    def verify_attribution_data(self) -> bool:
        """Verify attribution data was loaded correctly"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute('''
                    SELECT COUNT(*) 
                    FROM attribution_customer_journey 
                    WHERE ihc > 0
                ''')
                valid_records = cursor.fetchone()[0]
                
                if valid_records == 0:
                    self.logger.error("No valid attribution records found")
                    return False
                    
                self.logger.info(f"Found {valid_records} valid attribution records")
                return True
                
        except Exception as e:
            self.logger.error(f"Verification failed: {str(e)}")
            return False