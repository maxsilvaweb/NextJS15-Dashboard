import os
import json
import re
import pandas as pd
from datetime import datetime
import logging
import glob

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='data_processing.log'
)

def is_valid_email(email):
    """Check if email is valid using regex pattern"""
    if email == "invalid-email":
        return False
    pattern = r'^[\w\.-]+@[\w\.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))

def is_valid_date(date_str):
    """Check if date string is valid ISO format"""
    if date_str == "not-a-date":
        return False
    try:
        datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        return True
    except (ValueError, TypeError):
        return False

def is_valid_url(url):
    """Check if URL is valid"""
    if url == "broken_link":
        return False
    pattern = r'^https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+'
    return bool(re.match(pattern, url))

def is_valid_handle(handle):
    """Check if social media handle is valid"""
    if handle in ["#error_handle", ""]:
        return False
    return True

def clean_numeric(value):
    """Convert numeric values, handling NaN strings"""
    if value == "NaN" or value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def get_database_row_count():
    """Get the current number of rows in the database"""
    try:
        uploader = HerokuPostgreSQLUploader()
        with uploader.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM processed_data")
                count = cursor.fetchone()[0]
                logging.info(f"Database currently has {count} rows")
                return count
    except Exception as e:
        logging.error(f"Error checking database row count: {e}")
        return 0

def process_json_file(file_path, user_id):
    """Process a single JSON file and return normalized data"""
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        # Extract user data
        user_data = data.get('user', {})
        
        # Extract program data (tasks)
        program_data = data.get('program', {})
        tasks = program_data.get('tasks', [])
        
        # Normalize the data into a flat structure
        normalized_record = {
            'user_id': user_id,
            'email': user_data.get('email', ''),
            'name': user_data.get('name', ''),
            'platform': user_data.get('platform', ''),
            'url': user_data.get('url', ''),
            'followers': user_data.get('followers', 0),
            'engagement_rate': user_data.get('engagement_rate', 0.0),
            'total_sales_attributed': sum(task.get('sales_attributed', 0) for task in tasks),
            'likes': sum(task.get('likes', 0) for task in tasks),
            'comments': sum(task.get('comments', 0) for task in tasks),
            'shares': sum(task.get('shares', 0) for task in tasks),
            'reach': sum(task.get('reach', 0) for task in tasks)
        }
        
        return normalized_record
        
    except Exception as e:
        logging.error(f"Error processing {file_path}: {e}")
        return None

def get_next_file_to_process(mixed_dir, start_from):
    """Get the next file to process based on the starting number"""
    file_pattern = os.path.join(mixed_dir, f"user_{start_from}.json")
    if os.path.exists(file_pattern):
        return file_pattern
    return None

def main():
    # Get current database row count
    current_row_count = get_database_row_count()
    
    # The next file to process should be user_{current_row_count}.json
    next_file_number = current_row_count
    
    # Directory containing JSON files
    mixed_dir = os.path.join(os.path.dirname(__file__), "mixed")
    
    # Get the next file to process
    next_file = get_next_file_to_process(mixed_dir, next_file_number)
    
    if not next_file:
        logging.info(f"No new file to process. Looking for user_{next_file_number}.json")
        return
    
    logging.info(f"Processing file: {os.path.basename(next_file)}")
    
    # Process the file
    # User ID should be current_row_count + 1 (1-indexed)
    user_id = current_row_count + 1
    normalized_data = process_json_file(next_file, user_id)
    
    if not normalized_data:
        logging.error("Failed to process file")
        return
    
    # Upload to database
    try:
        uploader = HerokuPostgreSQLUploader()
        uploader.upload_data([normalized_data])
        logging.info(f"Successfully uploaded data for user_id {user_id}")
        
        # Verify the upload
        new_row_count = get_database_row_count()
        logging.info(f"Database now has {new_row_count} rows (was {current_row_count})")
        
    except Exception as e:
        logging.error(f"Error uploading data: {e}")

if __name__ == "__main__":
    main()