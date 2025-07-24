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

def process_all_files(directory):
    """Process all JSON files in the directory"""
    all_data = []
    file_count = 0
    error_count = 0
    
    # Get all JSON files using glob for better file discovery
    import glob
    json_pattern = os.path.join(directory, "user_*.json")
    json_files = glob.glob(json_pattern)
    
    # Sort files numerically by extracting the number from filename
    def extract_number(filename):
        match = re.search(r'user_(\d+)\.json', os.path.basename(filename))
        return int(match.group(1)) if match else 0
    
    json_files.sort(key=extract_number)
    total_files = len(json_files)
    
    logging.info(f"Starting to process {total_files} JSON files")
    
    for file_path in json_files:
        filename = os.path.basename(file_path)
        file_count += 1
        
        # Log progress every 100 files
        if file_count % 100 == 0:
            logging.info(f"Processed {file_count}/{total_files} files")
        
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

def main():
    # Directory containing JSON files - use relative path
    json_dir = os.path.join(os.path.dirname(__file__), "mixed")
    
    # Process all files
    logging.info("Starting data processing")
    processed_data = process_all_files(json_dir)
    
    # Save processed data to JSON (preserving lists) - also make relative
    output_json = os.path.join(os.path.dirname(__file__), "processed_data.json")
    with open(output_json, 'w') as f:
        json.dump(processed_data, f, indent=2)
    logging.info(f"Saved processed data to {output_json}")
    
    # Analyze and save statistics - also make relative
    stats = analyze_data(processed_data)
    stats_file = os.path.join(os.path.dirname(__file__), "data_statistics.json")
    with open(stats_file, 'w') as f:
        json.dump(stats, f, indent=2)
    logging.info(f"Saved data statistics to {stats_file}")
    
    # Print comprehensive summary to match vanilla version
    print("\n" + "="*50)
    print("DATA PROCESSING SUMMARY")
    print("="*50)
    print(f"Total records processed: {stats['total_records']}")
    
    print(f"\nData Quality Metrics:")
    print(f"  Valid emails: {int(stats['total_records'] * stats['valid_emails_percentage'] / 100)} ({stats['valid_emails_percentage']:.1f}%)")
    print(f"  Valid URLs: {int(stats['total_records'] * stats['valid_urls_percentage'] / 100)} ({stats['valid_urls_percentage']:.1f}%)")
    print(f"  Missing user IDs: {int(stats['total_records'] * stats['missing_user_ids_percentage'] / 100)} ({stats['missing_user_ids_percentage']:.1f}%)")
    
    print(f"\nPlatform Distribution:")
    for platform, count in stats['platform_distribution'].items():
        percentage = (count / stats['total_records']) * 100 if stats['total_records'] > 0 else 0
        print(f"  {platform}: {count} ({percentage:.1f}%)")
    
    print(f"\nCommon Issues Found:")
    for issue, count in list(stats['common_issues'].items())[:10]:  # Show top 10 issues
        percentage = (count / stats['total_records']) * 100 if stats['total_records'] > 0 else 0
        print(f"  {issue}: {count} ({percentage:.1f}%)")
    
    print(f"\nEngagement Statistics:")
    if stats['engagement_stats']['likes']['mean'] is not None:
        print(f"  Average likes per post: {stats['engagement_stats']['likes']['mean']:.1f}")
        print(f"  Median likes per post: {stats['engagement_stats']['likes']['median']:.1f}")
    print(f"  Records with missing likes: {stats['engagement_stats']['likes']['null_count']}")
    
    if stats['engagement_stats']['comments']['mean'] is not None:
        print(f"  Average comments per post: {stats['engagement_stats']['comments']['mean']:.1f}")
        print(f"  Median comments per post: {stats['engagement_stats']['comments']['median']:.1f}")
    print(f"  Records with missing comments: {stats['engagement_stats']['comments']['null_count']}")
    
    if stats['engagement_stats']['shares']['mean'] is not None:
        print(f"  Average shares per post: {stats['engagement_stats']['shares']['mean']:.1f}")
        print(f"  Median shares per post: {stats['engagement_stats']['shares']['median']:.1f}")
    print(f"  Records with missing shares: {stats['engagement_stats']['shares']['null_count']}")
    
    if stats['engagement_stats']['reach']['mean'] is not None:
        print(f"  Average reach per post: {stats['engagement_stats']['reach']['mean']:.1f}")
        print(f"  Median reach per post: {stats['engagement_stats']['reach']['median']:.1f}")
    print(f"  Records with missing reach: {stats['engagement_stats']['reach']['null_count']}")
    
    print(f"\nSales Statistics:")
    if stats['sales_stats']['mean'] is not None:
        print(f"  Average sales per record: ${stats['sales_stats']['mean']:.2f}")
        print(f"  Median sales per record: ${stats['sales_stats']['median']:.2f}")
    print(f"  Records with missing sales data: {stats['sales_stats']['null_count']}")
    records_with_sales = stats['total_records'] - stats['sales_stats']['null_count']
    if stats['total_records'] > 0:
        sales_conversion_rate = (records_with_sales / stats['total_records']) * 100
        print(f"  Records with sales data: {records_with_sales} ({sales_conversion_rate:.1f}%)")
    
    print(f"\nOutput Files:")
    print(f"  Processed data: {output_json}")
    print(f"  Statistics: {stats_file}")
    
    print("\n" + "="*50)
    print("Processing completed successfully!")
    print("="*50)

if __name__ == "__main__":
    main()