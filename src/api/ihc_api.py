import json
import logging
import requests
import time
import os
from typing import List, Dict

class IHCApiClient:
    def __init__(self, api_endpoint: str, api_key: str):
        self.api_endpoint = api_endpoint
        self.api_key = api_key
        self.logger = logging.getLogger(__name__)
        # API limits from documentation
        self.MAX_JOURNEYS_PER_REQUEST = 85
        self.MAX_SESSIONS_PER_REQUEST = 2750

    def group_by_conversion_id(self, data: List[Dict]) -> Dict[str, List[Dict]]:
        """Group sessions by conversion_id to maintain journey integrity"""
        journeys = {}
        for session in data:
            conv_id = session['conversion_id']
            journeys.setdefault(conv_id, []).append(session)
        return journeys

    def create_chunks(self, data: List[Dict]) -> List[List[Dict]]:
        """Create chunks respecting both journey and session limits"""
        journeys = self.group_by_conversion_id(data)
        chunks = []
        current_chunk = []
        current_sessions = 0
        current_journeys = 0

        for conv_id, sessions in journeys.items():
            # Check if adding this journey would exceed either limit
            if (current_journeys + 1 > self.MAX_JOURNEYS_PER_REQUEST or 
                current_sessions + len(sessions) > self.MAX_SESSIONS_PER_REQUEST):
                if current_chunk:
                    chunks.append(current_chunk)
                current_chunk = []
                current_sessions = 0
                current_journeys = 0

            # Add journey to current chunk
            current_chunk.extend(sessions)
            current_sessions += len(sessions)
            current_journeys += 1

        # Add the last chunk if not empty
        if current_chunk:
            chunks.append(current_chunk)

        return chunks

    def get_redistribution_parameters(self) -> Dict:
        """Default redistribution parameters as per API documentation"""
        return {
            'initializer': {
                'direction': 'earlier_sessions_only',
                'receive_threshold': 0,
                'redistribution_channel_labels': ['Direct', 'Email_NewsLetter']
            },
            'holder': {
                'direction': 'any_session',
                'receive_threshold': 0,
                'redistribution_channel_labels': ['Direct', 'Email_NewsLetter']
            },
            'closer': {
                'direction': 'later_sessions_only',
                'receive_threshold': 0.1,
                'redistribution_channel_labels': ['Direct']
            }
        }

    def save_response(self, responses: List[Dict], output_path: str) -> None:
        """Save API responses to a JSON file"""
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            self.logger.info(f"Saving API responses to {output_path}")
            with open(output_path, 'w') as json_file:
                json.dump(responses, json_file, indent=4)
            self.logger.info(f"API responses saved successfully to {output_path}")
        except Exception as e:
            self.logger.error(f"Error saving API responses: {str(e)}")
            raise

    def send_transformed_data(self, transformed_data_path: str, conv_type_id: str):
        try:
            with open(transformed_data_path, 'r') as file:
                data = json.load(file)

            # Add data validation before processing
            if not data or not isinstance(data, list):
                raise ValueError("Transformed data is empty or invalid format")
                
            chunks = self.create_chunks(data)
            total_chunks = len(chunks)
            self.logger.info(f"Split data into {total_chunks} chunks")
            
            responses = []
            successful_chunks = 0
            max_attempts = 3  # Add max attempts constant
            backoff_base = 2  # Exponential backoff base
            
            for i, chunk in enumerate(chunks, 1):
                # Add chunk validation
                if not chunk or not isinstance(chunk, list):
                    self.logger.error(f"Skipping invalid chunk {i}/{total_chunks}")
                    continue

                # Initialize retry variables
                attempt = 0
                success = False
                
                while attempt < max_attempts and not success:
                    try:
                        # Move validation outside request context
                        if not conv_type_id:
                            raise ValueError("conv_type_id cannot be empty")
                            
                        url = f"{self.api_endpoint}?conv_type_id={conv_type_id}"
                        headers = {
                            'Content-Type': 'application/json',
                            'x-api-key': self.api_key
                        }
                        body = {
                            'customer_journeys': chunk,
                            'redistribution_parameter': self.get_redistribution_parameters()
                        }
                        
                        response = requests.post(url, json=body, headers=headers)
                        response.raise_for_status()
                        response_data = response.json()

                        if response.status_code in [200, 206]:
                            self.logger.info(f"Chunk {i}/{total_chunks} succeeded")
                            responses.append(response_data)
                            successful_chunks += 1
                            success = True
                            
                            # Add delay between successful chunks
                            if i < total_chunks:
                                time.sleep(1)
                        else:
                            raise requests.exceptions.HTTPError(
                                f"API returned {response.status_code}: {response.text}"
                            )

                    except Exception as e:
                        attempt += 1
                        wait_time = backoff_base ** attempt
                        self.logger.warning(
                            f"Chunk {i}/{total_chunks} attempt {attempt} failed: {str(e)}. "
                            f"Retrying in {wait_time}s..."
                        )
                        time.sleep(wait_time)
                        
                        if attempt >= max_attempts:
                            self.logger.error(
                                f"Chunk {i}/{total_chunks} failed after {max_attempts} attempts"
                            )
            self.logger.info(f"Completed sending data: {successful_chunks}/{total_chunks} chunks successful")
            
            if not responses:
                raise ValueError("No valid API responses received")
            
            output_path = os.path.join(os.path.dirname(transformed_data_path), "api_response.json")
            self.save_response(responses, output_path)
            return responses
                
        except Exception as e:
            self.logger.error(f"Fatal error in send_transformed_data: {str(e)}")
            raise