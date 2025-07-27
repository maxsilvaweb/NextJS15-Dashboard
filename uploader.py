import os
import json
import glob
import logging
from datetime import datetime
from upload_to_postgres_heroku_backup import HerokuPostgreSQLUploader
import psycopg2
from psycopg2.extras import execute_batch

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('upload.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class EnhancedHerokuUploader(HerokuPostgreSQLUploader):
    def __init__(self, mixed_dir="mixed"):
        super().__init__()
        self.mixed_dir = mixed_dir
        
    def check_tables_exist(self):
        """Check if tables exist in the database"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_schema = 'public' 
                            AND table_name = 'processed_data'
                        );
                    """)
                    tables_exist = cur.fetchone()[0]
                    logger.info(f"Tables exist: {tables_exist}")
                    return tables_exist
        except Exception as e:
            logger.error(f"Error checking if tables exist: {e}")
            return False
    
    def get_last_uploaded_user_number(self):
        """Get the highest user_id (file number) from the database"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT MAX(user_id) FROM processed_data")
                    result = cur.fetchone()[0]
                    last_number = result if result is not None else -1
                    logger.info(f"Last uploaded user number: {last_number}")
                    return last_number
        except Exception as e:
            logger.error(f"Error getting last uploaded user number: {e}")
            return -1
    
    def get_available_user_files(self):
        """Get list of available user files and their numbers"""
        pattern = os.path.join(self.mixed_dir, "user_*.json")
        files = glob.glob(pattern)
        
        file_numbers = []
        for file_path in files:
            filename = os.path.basename(file_path)
            try:
                # Extract number from filename like "user_123.json"
                number = int(filename.replace("user_", "").replace(".json", ""))
                file_numbers.append((number, file_path))
            except ValueError:
                logger.warning(f"Skipping file with invalid format: {filename}")
        
        # Sort by number
        file_numbers.sort(key=lambda x: x[0])
        logger.info(f"Found {len(file_numbers)} user files")
        return file_numbers
    
    def process_user_file(self, file_path, user_number):
        """Process a single user file and return processed data"""
        try:
            with open(file_path, 'r') as f:
                user_data = json.load(f)
            
            processed_records = []
            
            # Extract user info
            user_info = {
                'user_id': user_number,
                'name': user_data.get('name'),
                'email': user_data.get('email'),
                'instagram_handle': user_data.get('instagram_handle'),
                'tiktok_handle': user_data.get('tiktok_handle'),
                'joined_at': user_data.get('joined_at'),
                'source_file': os.path.basename(file_path)
            }
            
            # Process advocacy programs
            advocacy_programs = user_data.get('advocacy_programs', [])
            
            for program in advocacy_programs:
                program_id = program.get('program_id')
                brand = program.get('brand')
                total_sales = program.get('total_sales_attributed', 0)
                
                tasks = program.get('tasks_completed', [])
                
                for task in tasks:
                    record = user_info.copy()
                    record.update({
                        'program_id': program_id,
                        'brand': brand,
                        'task_id': task.get('task_id'),
                        'platform': task.get('platform'),
                        'post_url': task.get('post_url'),
                        'likes': task.get('likes', 0),
                        'comments': task.get('comments', 0),
                        'shares': task.get('shares', 0),
                        'reach': task.get('reach', 0),
                        'total_sales_attributed': total_sales
                    })
                    processed_records.append(record)
            
            return processed_records
            
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
            return []
    
    def batch_upload_records(self, records, batch_size=1000):
        """Upload records in batches to avoid memory issues"""
        if not records:
            logger.warning("No records to upload")
            return 0
        
        total_uploaded = 0
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Prepare insert SQL
                    insert_sql = """
                        INSERT INTO processed_data 
                        (user_id, name, email, instagram_handle, tiktok_handle, joined_at,
                         program_id, brand, task_id, platform, post_url, likes, comments, 
                         shares, reach, total_sales_attributed, source_file, processed_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (user_id, email, post_url, task_id) DO NOTHING
                    """
                    
                    # Process in batches
                    for i in range(0, len(records), batch_size):
                        batch = records[i:i + batch_size]
                        
                        # Prepare batch data
                        batch_data = []
                        for record in batch:
                            # Parse joined_at if it exists
                            joined_at = None
                            if record.get('joined_at'):
                                try:
                                    joined_at = datetime.fromisoformat(record['joined_at'].replace('Z', '+00:00'))
                                except:
                                    joined_at = None
                            
                            batch_data.append((
                                record.get('user_id'),
                                record.get('name'),
                                record.get('email'),
                                record.get('instagram_handle'),
                                record.get('tiktok_handle'),
                                joined_at,
                                record.get('program_id'),
                                record.get('brand'),
                                record.get('task_id'),
                                record.get('platform'),
                                record.get('post_url'),
                                record.get('likes', 0),
                                record.get('comments', 0),
                                record.get('shares', 0),
                                record.get('reach', 0),
                                record.get('total_sales_attributed', 0),
                                record.get('source_file'),
                                datetime.now()
                            ))
                        
                        # Execute batch
                        execute_batch(cur, insert_sql, batch_data, page_size=batch_size)
                        batch_uploaded = cur.rowcount
                        total_uploaded += batch_uploaded
                        
                        logger.info(f"Uploaded batch {i//batch_size + 1}: {batch_uploaded} records")
                    
                    conn.commit()
                    logger.info(f"Total records uploaded: {total_uploaded}")
                    return total_uploaded
                    
        except Exception as e:
            logger.error(f"Error during batch upload: {e}")
            return 0
    
    def run_upload_process(self, start_file=None, end_file=None):
        """Main upload process with your requirements"""
        logger.info("Starting enhanced upload process...")
        
        # Step 1: Check if tables exist, create if not
        tables_exist = self.check_tables_exist()
        
        if not tables_exist:
            logger.info("Tables don't exist. Creating tables...")
            self.create_tables()
            logger.info("Tables created successfully")
        
        # Step 2: Get available files
        available_files = self.get_available_user_files()
        
        if not available_files:
            logger.warning(f"No user files found in {self.mixed_dir} directory")
            return
        
        # Step 3: Determine which files to process
        if not tables_exist:
            # No tables existed, so database is empty - upload all files
            logger.info("Database was empty. Processing all available files...")
            files_to_process = available_files
        else:
            # Tables exist, check last uploaded file
            last_uploaded = self.get_last_uploaded_user_number()
            
            if last_uploaded == -1:
                # No data in tables, upload all
                logger.info("Tables exist but no data found. Processing all files...")
                files_to_process = available_files
            else:
                # Upload only files after the last uploaded
                logger.info(f"Last uploaded file: user_{last_uploaded}.json")
                files_to_process = [(num, path) for num, path in available_files if num > last_uploaded]
                logger.info(f"Found {len(files_to_process)} new files to process")
        
        # Apply custom range if specified
        if start_file is not None or end_file is not None:
            start = start_file if start_file is not None else 0
            end = end_file if end_file is not None else 9999
            files_to_process = [(num, path) for num, path in files_to_process if start <= num <= end]
            logger.info(f"Custom range applied: {len(files_to_process)} files to process")
        
        if not files_to_process:
            logger.info("No new files to process")
            return
        
        # Step 4: Process and upload files
        all_records = []
        processed_count = 0
        
        for user_number, file_path in files_to_process:
            logger.info(f"Processing file: {os.path.basename(file_path)}")
            
            records = self.process_user_file(file_path, user_number)
            all_records.extend(records)
            processed_count += 1
            
            # Upload in batches of files to avoid memory issues
            if len(all_records) >= 5000:  # Upload every 5000 records
                uploaded = self.batch_upload_records(all_records)
                logger.info(f"Uploaded {uploaded} records from {processed_count} files")
                all_records = []  # Clear memory
        
        # Upload remaining records
        if all_records:
            uploaded = self.batch_upload_records(all_records)
            logger.info(f"Uploaded final batch: {uploaded} records")
        
        logger.info(f"Upload process completed. Processed {processed_count} files.")

def main():
    """Main function to run the enhanced uploader"""
    # Change from "mixed_backup" to "mixed" to match your file location
    uploader = EnhancedHerokuUploader(mixed_dir="mixed")
    
    try:
        # Run the upload process
        # You can specify custom ranges like: uploader.run_upload_process(start_file=0, end_file=9999)
        uploader.run_upload_process()
        
    except Exception as e:
        logger.error(f"Upload process failed: {e}")
        exit(1)

if __name__ == "__main__":
    main()