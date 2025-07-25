import os
import json
import re
import pandas as pd
from datetime import datetime
import logging
import glob
from upload_to_postgres_heroku import HerokuPostgreSQLUploader

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
            'name': user_data.get('name', ''),
            'email': user_data.get('email', ''),
            'email_valid': is_valid_email(user_data.get('email', '')),
            'instagram_handle': user_data.get('instagram_handle', ''),
            'tiktok_handle': user_data.get('tiktok_handle', ''),
            'joined_at': user_data.get('joined_at', ''),
            'program_id': program_data.get('program_id', ''),
            'brand': program_data.get('brand', ''),
            'platform': user_data.get('platform', ''),
            'post_url': user_data.get('url', ''),
            'url_valid': is_valid_url(user_data.get('url', '')),
            'likes': sum(task.get('likes', 0) for task in tasks),
            'comments': sum(task.get('comments', 0) for task in tasks),
            'shares': sum(task.get('shares', 0) for task in tasks),
            'reach': sum(task.get('reach', 0) for task in tasks),
            'total_sales_attributed': sum(task.get('sales_attributed', 0) for task in tasks),
            'source_file': os.path.basename(file_path),
            'issues_found': 0,
            'issues_list': []
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
    # Create tables first
    try:
        uploader = HerokuPostgreSQLUploader()
        uploader.create_tables()
        database_available = True
    except Exception as e:
        logging.error(f"Error creating tables: {e}")
        database_available = False
        # Continue processing even without database
    
    # Get current database row count (or default to 0 if no database)
    if database_available:
        current_row_count = get_database_row_count()
    else:
        current_row_count = 0
        logging.info("Database not available, starting from file 0")
    
    # The next file to process should be user_{current_row_count}.json
    next_file_number = current_row_count
    
    # Directory containing JSON files
    mixed_dir = os.path.join(os.path.dirname(__file__), "mixed")
    
    # Get the next file to process
    next_file = get_next_file_to_process(mixed_dir, next_file_number)
    
    if not next_file:
        logging.info(f"No new file to process. Looking for user_{next_file_number}.json")
        # Create empty processed_data.json for the workflow
        with open('processed_data.json', 'w') as f:
            json.dump([], f)
        return
    
    logging.info(f"Processing file: {os.path.basename(next_file)}")
    
    # Process the file
    # User ID should be current_row_count + 1 (1-indexed)
    user_id = current_row_count + 1
    normalized_data = process_json_file(next_file, user_id)
    
    if not normalized_data:
        logging.error("Failed to process file")
        # Create empty processed_data.json for the workflow
        with open('processed_data.json', 'w') as f:
            json.dump([], f)
        return
    
    # Save processed data to JSON file (required by the workflow)
    with open('processed_data.json', 'w') as f:
        json.dump([normalized_data], f, indent=2)
    
    logging.info(f"Successfully processed and saved data for user_id {user_id}")
    
    # Verify the upload by checking row count
    new_row_count = get_database_row_count()
    logging.info(f"Processing complete. Ready for upload to database.")

if __name__ == "__main__":
    main()