import os
import json
import re
import pandas as pd
import logging
from datetime import datetime
from uploader import HerokuPostgreSQLUploader

def is_valid_email(email):
    """Check if email is valid using regex pattern"""
    if email == "invalid-email" or not email:
        return False
    pattern = r'^[\w\.-]+@[\w\.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))

def is_valid_date(date_str):
    """Check if date string is valid ISO format"""
    if date_str == "not-a-date" or not date_str:
        return False
    try:
        datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        return True
    except (ValueError, TypeError):
        return False

def is_valid_url(url):
    """Check if URL is valid"""
    if url == "broken_link" or not url:
        return False
    pattern = r'^https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+'
    return bool(re.match(pattern, url))

def is_valid_handle(handle):
    """Check if social media handle is valid"""
    if handle in ["#error_handle", "", None]:
        return False
    return True

def clean_numeric(value):
    """Convert numeric values, handling NaN strings"""
    if value == "NaN" or value == "no-data" or value is None:
        return 0
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0

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
        
        # Extract user data (root level)
        user_name = data.get('name', '')
        user_email = data.get('email', '')
        instagram_handle = data.get('instagram_handle', '')
        tiktok_handle = data.get('tiktok_handle', '')
        joined_at = data.get('joined_at', '')
        
        # Extract advocacy programs data
        advocacy_programs = data.get('advocacy_programs', [])
        
        processed_records = []
        
        # Process each advocacy program
        for program_idx, program in enumerate(advocacy_programs):
            program_id = program.get('program_id', '')
            brand = str(program.get('brand', ''))  # Convert to string since it might be a number
            total_sales_attributed = clean_numeric(program.get('total_sales_attributed', 0))
            
            # Process each task in the program
            tasks_completed = program.get('tasks_completed', [])
            
            for task_idx, task in enumerate(tasks_completed):
                # Create a unique task_id if none exists
                task_id = task.get('task_id') or f"task_{user_id}_{program_idx}_{task_idx}"
                
                # Normalize the data into a flat structure
                normalized_record = {
                    'user_id': user_id,
                    'name': user_name,
                    'email': user_email,
                    'email_valid': is_valid_email(user_email),
                    'instagram_handle': instagram_handle,
                    'tiktok_handle': tiktok_handle,
                    'joined_at': joined_at if is_valid_date(joined_at) else None,
                    'program_id': program_id,
                    'brand': brand,
                    'task_id': str(task_id),
                    'platform': task.get('platform', ''),
                    'post_url': task.get('post_url', ''),
                    'url_valid': is_valid_url(task.get('post_url', '')),
                    'likes': int(clean_numeric(task.get('likes', 0))),
                    'comments': int(clean_numeric(task.get('comments', 0))),
                    'shares': int(clean_numeric(task.get('shares', 0))),
                    'reach': int(clean_numeric(task.get('reach', 0))),
                    'total_sales_attributed': total_sales_attributed,
                    'source_file': os.path.basename(file_path),
                    'issues_found': 0,
                    'issues_list': []
                }
                
                processed_records.append(normalized_record)
        
        # If no advocacy programs or tasks, create a basic record
        if not processed_records:
            normalized_record = {
                'user_id': user_id,
                'name': user_name,
                'email': user_email,
                'email_valid': is_valid_email(user_email),
                'instagram_handle': instagram_handle,
                'tiktok_handle': tiktok_handle,
                'joined_at': joined_at if is_valid_date(joined_at) else None,
                'program_id': '',
                'brand': '',
                'task_id': f"task_{user_id}_0_0",
                'platform': '',
                'post_url': '',
                'url_valid': False,
                'likes': 0,
                'comments': 0,
                'shares': 0,
                'reach': 0,
                'total_sales_attributed': 0,
                'source_file': os.path.basename(file_path),
                'issues_found': 0,
                'issues_list': []
            }
            processed_records.append(normalized_record)
        
        return processed_records
        
    except Exception as e:
        logging.error(f"Error processing {file_path}: {e}")
        return []

def main():
    # 1. Create tables first if they don't exist
    try:
        uploader = HerokuPostgreSQLUploader()
        uploader.create_tables()
        logging.info("Tables created or already exist")
        database_available = True
    except Exception as e:
        logging.error(f"Error creating tables: {e}")
        database_available = False
        # Create empty processed_data.json and exit
        with open('processed_data.json', 'w') as f:
            json.dump([], f)
        return
    
    # 2. Check current row count in database
    current_row_count = get_database_row_count()
    
    # Directory containing JSON files
    mixed_dir = os.path.join(os.path.dirname(__file__), "mixed")
    
    # 3. Determine which files to process
    if current_row_count == 0:
        # Process ALL files from user_0.json to user_10042.json (10043 files total)
        logging.info("Database is empty. Processing all files from user_0.json to user_10042.json")
        start_file = 0
        end_file = 10042  # This gives us 10043 files (0 to 10042 inclusive)
    else:
        # Process only the next file: user_{current_row_count}.json
        logging.info(f"Database has {current_row_count} rows. Processing next file: user_{current_row_count}.json")
        start_file = current_row_count
        end_file = current_row_count
    
    # Process files
    all_processed_data = []
    files_processed = 0
    
    for file_number in range(start_file, end_file + 1):
        file_path = os.path.join(mixed_dir, f"user_{file_number}.json")
        
        if not os.path.exists(file_path):
            logging.warning(f"File not found: user_{file_number}.json")
            continue
        
        # User ID should be file_number + 1 (1-indexed)
        user_id = file_number + 1
        normalized_records = process_json_file(file_path, user_id)
        
        if normalized_records:
            all_processed_data.extend(normalized_records)
            files_processed += 1
            if files_processed % 100 == 0:  # Log progress every 100 files
                logging.info(f"Processed {files_processed} files so far...")
        else:
            logging.error(f"Failed to process {file_path}")
    
    # Save all processed data to JSON file (required by the workflow)
    with open('processed_data.json', 'w') as f:
        json.dump(all_processed_data, f, indent=2)
    
    logging.info(f"Successfully processed {files_processed} files. Created {len(all_processed_data)} records. Data saved to processed_data.json")
    
    if files_processed == 0:
        logging.warning("No files were processed")
    else:
        logging.info(f"Ready to upload {len(all_processed_data)} records to database")

if __name__ == "__main__":
    main()