import os
import json
import re
import pandas as pd
from datetime import datetime
import logging

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
    """Check the current row count in the database"""
    try:
        from upload_to_postgres_heroku import HerokuPostgreSQLUploader
        uploader = HerokuPostgreSQLUploader()
        with uploader.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM processed_data")
                count = cursor.fetchone()[0]
                return count
    except Exception as e:
        logging.error(f"Error checking database row count: {e}")
        return 0

def get_highest_numbered_file(directory):
    """Get the highest numbered user file in the directory"""
    import glob
    json_pattern = os.path.join(directory, "user_*.json")
    json_files = glob.glob(json_pattern)
    
    if not json_files:
        return None
    
    # Extract numbers and find the highest
    def extract_number(filename):
        match = re.search(r'user_(\d+)\.json', os.path.basename(filename))
        return int(match.group(1)) if match else -1
    
    highest_file = max(json_files, key=extract_number)
    return highest_file

def process_json_file(file_path):
    """Process a single JSON file and return cleaned data records"""
    filename = os.path.basename(file_path)
    
    # Extract sequential ID from filename (e.g., user_0.json -> 1, user_125.json -> 126)
    sequential_id = None
    match = re.match(r'user_(\d+)\.json', filename)
    if match:
        sequential_id = int(match.group(1)) + 1  # Add 1 so user_0.json becomes ID 1
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logging.error(f"Error reading {filename}: {e}")
        return []
    
    # Initialize the list to store all records for this file
    all_records = []
    
    # Extract user data
    user_id = data.get('user_id')
    name = data.get('name', '')
    email = data.get('email', '')
    instagram_handle = data.get('instagram_handle', '')
    tiktok_handle = data.get('tiktok_handle', '')
    joined_at = data.get('joined_at', '')
    advocacy_programs = data.get('advocacy_programs', [])
    
    # Handle invalid emails by replacing with bootstrap email
    if email == "invalid-email" or not is_valid_email(email):
        # Extract username from name or use default
        if name and name != "???":
            username = name.lower().replace(" ", "_").replace(".", "")
            # Remove any non-alphanumeric characters except underscore
            username = re.sub(r'[^a-z0-9_]', '', username)
        else:
            username = f"user_{sequential_id if sequential_id else 'unknown'}"
        email = f"{username}@domain.com"
    
    # Handle user_id: Always use sequential ID for consistency
    # If user_id is a UUID or any non-integer, replace with sequential ID
    if isinstance(user_id, str) and len(user_id) == 36 and '-' in user_id:
        # This is likely a UUID, replace with sequential ID
        user_id = sequential_id
    elif user_id is None or not isinstance(user_id, int):
        # Use sequential ID for null or non-integer user_ids
        user_id = sequential_id
    
    # If we still don't have a valid user_id, generate fallback
    if user_id is None:
        user_id = abs(hash(filename)) % 1000000
    
    if not advocacy_programs:
        # If no advocacy programs, create one record with user data only
        issues = []
        cleaned_data = {}
        
        # Add user data
        cleaned_data['user_id'] = user_id
        cleaned_data['name'] = name
        cleaned_data['email'] = email
        cleaned_data['email_valid'] = is_valid_email(email)
        # Handle social media handles safely
        cleaned_data['instagram_handle'] = instagram_handle.lstrip('@') if instagram_handle and is_valid_handle(instagram_handle) else None
        cleaned_data['tiktok_handle'] = tiktok_handle.lstrip('@') if tiktok_handle and is_valid_handle(tiktok_handle) else None
        
        # Handle joined_at date
        if is_valid_date(joined_at):
            date_obj = datetime.fromisoformat(joined_at.replace('Z', '+00:00'))
            cleaned_data['joined_at'] = date_obj.isoformat()
        else:
            cleaned_data['joined_at'] = None
            if joined_at:
                issues.append("Invalid date format")
        
        # Set empty program/task data
        cleaned_data['program_id'] = None
        cleaned_data['brand'] = None
        cleaned_data['task_id'] = None
        cleaned_data['platform'] = None
        cleaned_data['post_url'] = None
        cleaned_data['url_valid'] = False
        cleaned_data['likes'] = None
        cleaned_data['comments'] = None
        cleaned_data['shares'] = None
        cleaned_data['reach'] = None
        cleaned_data['total_sales_attributed'] = None
        
        # Add metadata
        cleaned_data['source_file'] = filename
        cleaned_data['issues_found'] = len(issues)
        cleaned_data['issues_list'] = issues
        
        all_records.append(cleaned_data)
    
    else:
        # Process each advocacy program
        for program in advocacy_programs:
            program_id = program.get('program_id')
            brand = program.get('brand')
            total_sales_attributed = program.get('total_sales_attributed')
            tasks_completed = program.get('tasks_completed', [])
            
            if not tasks_completed:
                # If no tasks, create one record with program data but no task data
                issues = []
                cleaned_data = {}
                
                # Add user data
                cleaned_data['user_id'] = user_id
                cleaned_data['name'] = name
                cleaned_data['email'] = email
                cleaned_data['email_valid'] = is_valid_email(email)
                # Handle social media handles safely
                cleaned_data['instagram_handle'] = instagram_handle.lstrip('@') if instagram_handle and is_valid_handle(instagram_handle) else None
                cleaned_data['tiktok_handle'] = tiktok_handle.lstrip('@') if tiktok_handle and is_valid_handle(tiktok_handle) else None
                
                # Handle joined_at date
                if is_valid_date(joined_at):
                    date_obj = datetime.fromisoformat(joined_at.replace('Z', '+00:00'))
                    cleaned_data['joined_at'] = date_obj.isoformat()
                else:
                    cleaned_data['joined_at'] = None
                    if joined_at:
                        issues.append("Invalid date format")
                
                # Add program data
                cleaned_data['program_id'] = program_id
                cleaned_data['brand'] = brand
                cleaned_data['total_sales_attributed'] = clean_numeric(total_sales_attributed)
                
                # Set empty task data
                cleaned_data['task_id'] = None
                cleaned_data['platform'] = None
                cleaned_data['post_url'] = None
                cleaned_data['url_valid'] = False
                cleaned_data['likes'] = None
                cleaned_data['comments'] = None
                cleaned_data['shares'] = None
                cleaned_data['reach'] = None
                
                # Add metadata
                cleaned_data['source_file'] = filename
                cleaned_data['issues_found'] = len(issues)
                cleaned_data['issues_list'] = issues
                
                all_records.append(cleaned_data)
            
            else:
                # Process each task in the program
                for task in tasks_completed:
                    issues = []
                    cleaned_data = {}
                    
                    # Add user data
                    cleaned_data['user_id'] = user_id
                    cleaned_data['name'] = name
                    cleaned_data['email'] = email
                    cleaned_data['email_valid'] = is_valid_email(email)
                    # Handle social media handles safely
                    cleaned_data['instagram_handle'] = instagram_handle.lstrip('@') if instagram_handle and is_valid_handle(instagram_handle) else None
                    cleaned_data['tiktok_handle'] = tiktok_handle.lstrip('@') if tiktok_handle and is_valid_handle(tiktok_handle) else None
                    
                    # Handle joined_at date
                    if is_valid_date(joined_at):
                        date_obj = datetime.fromisoformat(joined_at.replace('Z', '+00:00'))
                        cleaned_data['joined_at'] = date_obj.isoformat()
                    else:
                        cleaned_data['joined_at'] = None
                        if joined_at:
                            issues.append("Invalid date format")
                    
                    # Add program data
                    cleaned_data['program_id'] = program_id
                    cleaned_data['brand'] = brand
                    cleaned_data['total_sales_attributed'] = clean_numeric(total_sales_attributed)
                    
                    # Add task data
                    cleaned_data['task_id'] = task.get('task_id')
                    
                    # Handle platform
                    platform = task.get('platform')
                    if isinstance(platform, int):
                        issues.append("Platform is numeric instead of string")
                        cleaned_data['platform'] = str(platform)
                    else:
                        cleaned_data['platform'] = platform
                    
                    # Handle post_url
                    post_url = task.get('post_url', '')
                    if not is_valid_url(post_url):
                        if post_url:
                            issues.append("Invalid post URL")
                        cleaned_data['post_url'] = None
                        cleaned_data['url_valid'] = False
                    else:
                        cleaned_data['post_url'] = post_url
                        cleaned_data['url_valid'] = True
                    
                    # Handle engagement metrics
                    for field in ['likes', 'comments', 'shares', 'reach']:
                        value = task.get(field)
                        cleaned_value = clean_numeric(value)
                        cleaned_data[field] = cleaned_value
                        if cleaned_value is None and value is not None:
                            issues.append(f"Invalid {field} value: {value}")
                    
                    # Add metadata
                    cleaned_data['source_file'] = filename
                    cleaned_data['issues_found'] = len(issues)
                    cleaned_data['issues_list'] = issues
                    
                    all_records.append(cleaned_data)
    
    # Log issues if any
    total_issues = sum(record['issues_found'] for record in all_records)
    if total_issues > 0:
        all_issues = []
        for record in all_records:
            all_issues.extend(record['issues_list'])
        logging.warning(f"Issues in {filename}: {', '.join(set(all_issues))}")
    
    return all_records

def get_last_processed_file_number():
    """Get the last processed file number from tracking file"""
    tracking_file = os.path.join(os.path.dirname(__file__), "last_processed_file.txt")
    try:
        if os.path.exists(tracking_file):
            with open(tracking_file, 'r') as f:
                return int(f.read().strip())
        return -1  # No files processed yet
    except Exception as e:
        logging.error(f"Error reading last processed file: {e}")
        return -1

def update_last_processed_file_number(file_number):
    """Update the last processed file number in tracking file"""
    tracking_file = os.path.join(os.path.dirname(__file__), "last_processed_file.txt")
    try:
        with open(tracking_file, 'w') as f:
            f.write(str(file_number))
        logging.info(f"Updated last processed file number to: {file_number}")
    except Exception as e:
        logging.error(f"Error updating last processed file: {e}")

def process_all_files(directory):
    """Process all JSON files in the directory"""
    all_data = []
    file_count = 0
    error_count = 0
    last_processed_number = -1
    
    # Get the last processed file number
    start_from = get_last_processed_file_number()
    logging.info(f"Starting from file number: {start_from + 1}")
    
    # Get all JSON files using glob for better file discovery
    import glob
    json_pattern = os.path.join(directory, "user_*.json")
    json_files = glob.glob(json_pattern)
    
    # Sort files numerically by extracting the number from filename
    def extract_number(filename):
        match = re.search(r'user_(\d+)\.json', os.path.basename(filename))
        return int(match.group(1)) if match else 0
    
    json_files.sort(key=extract_number)
    
    # Filter to only process files after the last processed one
    files_to_process = [f for f in json_files if extract_number(f) > start_from]
    total_files = len(files_to_process)
    
    if total_files == 0:
        logging.info("No new files to process")
        return []
    
    logging.info(f"Starting to process {total_files} new JSON files")
    
    for file_path in files_to_process:
        filename = os.path.basename(file_path)
        file_number = extract_number(file_path)
        file_count += 1
        
        # Log progress every 100 files
        if file_count % 100 == 0:
            logging.info(f"Processed {file_count}/{total_files} files")
        
        try:
            result = process_json_file(file_path)
            if result:
                all_data.extend(result)
                last_processed_number = file_number  # Track the last successfully processed file
            else:
                error_count += 1
                logging.error(f"No data returned from {filename}")
        except Exception as e:
            error_count += 1
            logging.error(f"Error processing {filename}: {e}")
    
    # Update the tracking file with the last processed file number
    if last_processed_number > start_from:
        update_last_processed_file_number(last_processed_number)
    
    logging.info(f"Completed processing. Processed {file_count} files with {error_count} errors")
    logging.info(f"Total records generated: {len(all_data)}")
    return all_data

def analyze_data(data):
    """Analyze the cleaned data and generate statistics"""
    df = pd.DataFrame(data)
    
    # Basic statistics
    total_records = len(df)
    valid_emails = int(df['email_valid'].sum())
    valid_urls = int(df['url_valid'].sum()) if 'url_valid' in df.columns else 0
    missing_user_ids = int(df['issues_list'].apply(lambda x: 'Missing user_id' in x).sum())
    
    # Platform distribution
    platform_counts = df['platform'].value_counts(dropna=False)
    
    # Issues statistics
    common_issues = {}
    for issues in df['issues_list']:
        for issue in issues:
            common_issues[issue] = common_issues.get(issue, 0) + 1
    
    # Engagement metrics - convert to native Python types
    engagement_stats = {
        'likes': {
            'mean': float(df['likes'].mean()) if not pd.isna(df['likes'].mean()) else None,
            'median': float(df['likes'].median()) if not pd.isna(df['likes'].median()) else None,
            'null_count': int(df['likes'].isna().sum())
        },
        'comments': {
            'mean': float(df['comments'].mean()) if not pd.isna(df['comments'].mean()) else None,
            'median': float(df['comments'].median()) if not pd.isna(df['comments'].median()) else None,
            'null_count': int(df['comments'].isna().sum())
        },
        'shares': {
            'mean': float(df['shares'].mean()) if not pd.isna(df['shares'].mean()) else None,
            'median': float(df['shares'].median()) if not pd.isna(df['shares'].median()) else None,
            'null_count': int(df['shares'].isna().sum())
        },
        'reach': {
            'mean': float(df['reach'].mean()) if not pd.isna(df['reach'].mean()) else None,
            'median': float(df['reach'].median()) if not pd.isna(df['reach'].median()) else None,
            'null_count': int(df['reach'].isna().sum())
        }
    }
    
    # Sales statistics - convert to native Python types
    sales_stats = {
        'mean': float(df['total_sales_attributed'].mean()) if not pd.isna(df['total_sales_attributed'].mean()) else None,
        'median': float(df['total_sales_attributed'].median()) if not pd.isna(df['total_sales_attributed'].median()) else None,
        'null_count': int(df['total_sales_attributed'].isna().sum())
    }
    
    # Create summary - convert all values to JSON-serializable types
    summary = {
        'total_records': int(total_records),
        'valid_emails_percentage': float((valid_emails / total_records) * 100),
        'valid_urls_percentage': float((valid_urls / total_records) * 100),
        'missing_user_ids_percentage': float((missing_user_ids / total_records) * 100),
        'platform_distribution': {str(k): int(v) for k, v in platform_counts.to_dict().items()},
        'common_issues': common_issues,
        'engagement_stats': engagement_stats,
        'sales_stats': sales_stats
    }
    
    return summary

def get_last_processed_file_number():
    """Get the highest file number that was already processed based on database count"""
    try:
        from upload_to_postgres_heroku import HerokuPostgreSQLUploader
        uploader = HerokuPostgreSQLUploader()
        with uploader.get_connection() as conn:
            with conn.cursor() as cursor:
                # Get the highest user_id from the database
                cursor.execute("SELECT MAX(user_id) FROM processed_data")
                result = cursor.fetchone()[0]
                if result is not None:
                    # Convert back to file number (user_id = file_number + 1)
                    return result - 1
                return -1  # No files processed yet
    except Exception as e:
        logging.error(f"Error checking last processed file: {e}")
        return -1

def process_new_files(directory):
    """Process only new files that haven't been processed yet"""
    all_data = []
    file_count = 0
    error_count = 0
    
    # Get the last processed file number
    last_processed = get_last_processed_file_number()
    logging.info(f"Last processed file number: {last_processed}")
    
    # Get all JSON files
    import glob
    json_pattern = os.path.join(directory, "user_*.json")
    json_files = glob.glob(json_pattern)
    
    # Filter to only new files
    def extract_number(filename):
        match = re.search(r'user_(\d+)\.json', os.path.basename(filename))
        return int(match.group(1)) if match else 0
    
    # Only process files with numbers greater than last_processed
    new_files = [f for f in json_files if extract_number(f) > last_processed]
    new_files.sort(key=extract_number)
    
    total_files = len(new_files)
    logging.info(f"Found {total_files} new files to process")
    
    if total_files == 0:
        logging.info("No new files to process")
        return []
    
    for file_path in new_files:
        filename = os.path.basename(file_path)
        file_count += 1
        
        logging.info(f"Processing new file {file_count}/{total_files}: {filename}")
        
        try:
            result = process_json_file(file_path)
            if result:
                all_data.extend(result)
            else:
                error_count += 1
                logging.error(f"No data returned from {filename}")
        except Exception as e:
            error_count += 1
            logging.error(f"Error processing {filename}: {e}")
    
    logging.info(f"Completed processing. Processed {file_count} new files with {error_count} errors")
    logging.info(f"Total new records generated: {len(all_data)}")
    return all_data

def main():
    # Directory containing JSON files - use relative path
    json_dir = os.path.join(os.path.dirname(__file__), "mixed")
    
    # Always use the incremental processing approach
    logging.info("Processing files incrementally")
    data = process_all_files(json_dir)
    
    if not data:
        logging.info("No new data to process or upload")
        return
    
    # Save processed data to JSON (preserving lists) - also make relative
    output_json = os.path.join(os.path.dirname(__file__), "processed_data.json")
    with open(output_json, 'w') as f:
        json.dump(data, f, indent=2)
    logging.info(f"Saved {len(data)} records to {output_json}")
    
    # Analyze and save statistics
    stats = analyze_data(data)
    stats_file = os.path.join(os.path.dirname(__file__), "data_statistics.json")
    with open(stats_file, 'w') as f:
        json.dump(stats, f, indent=2)
    logging.info(f"Saved statistics to {stats_file}")
    
    # Print summary
    print(f"\nProcessed {len(data)} new records")
    last_processed = get_last_processed_file_number()
    print(f"Last processed file number: {last_processed}")
    
    if last_processed == -1:
        print("This was the first run (no tracking file existed)")
    else:
        print(f"Processed only new files since file number {last_processed}")

if __name__ == "__main__":
    main()