from datetime import datetime, timedelta
from typing import Tuple

class DateRangeHandler:
    @staticmethod
    def validate_date_range(start_date: str, end_date: str) -> Tuple[datetime, datetime]:
        """
        Validate and parse date range strings into datetime objects
        
        Args:
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            
        Returns:
            Tuple[datetime, datetime]: Validated start and end datetime objects
        """
        try:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            
            if end_dt < start_dt:
                raise ValueError("End date must be after start date")
                
            if end_dt > datetime.now():
                raise ValueError("End date cannot be in the future")
                
            return start_dt, end_dt
            
        except ValueError as e:
            raise ValueError(f"Invalid date format or range: {str(e)}")
            
    @staticmethod
    def get_default_date_range() -> Tuple[str, str]:
        """
        Get default date range (last 30 days)
        
        Returns:
            Tuple[str, str]: Default start and end dates in YYYY-MM-DD format
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')