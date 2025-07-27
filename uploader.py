import json
import os
import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch
from datetime import datetime
import logging
from dotenv import load_dotenv
from urllib.parse import urlparse
import glob
from pathlib import Path

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MixedDataUploader:
    def __init__(self):
        # Try both environment variable names for flexibility
        database_url = os.getenv('HEROKU_DATABASE_URL') or os.getenv('DATABASE_URL')
        if not database_url:
            raise ValueError("HEROKU_DATABASE_URL or DATABASE_URL environment variable is required")
        
        # Parse the DATABASE_URL
        url = urlparse(database_url)
        self.connection_params = {
            'host': url.hostname,
            'port': url.port or 5432,
            'database': url.path[1:],  # Remove leading '/'
            'user': url.username,
            'password': url.password,
            'sslmode': 'require'  # Heroku requires SSL
        }
    
    def get_connection(self):
        """Create and return a database connection"""
        try:
            conn = psycopg2.connect(**self.connection_params)
            logger.info("Successfully connected to Heroku PostgreSQL")
            return conn
        except psycopg2.Error as e:
            logger.error(f"Error connecting to Heroku PostgreSQL: {e}")
            raise
    
    def create_tables(self):
        """Create necessary tables if they don't exist"""
        create_processed_data_table = """
        CREATE TABLE IF NOT EXISTS processed_data (
            id SERIAL PRIMARY KEY,
            user_id VARCHAR(255),  -- UUID from JSON files
            name VARCHAR(255),
            email VARCHAR(255),
            email_valid BOOLEAN DEFAULT TRUE,
            instagram_handle VARCHAR(255),
            tiktok_handle VARCHAR(255),
            joined_at TIMESTAMP,
            program_id VARCHAR(255),
            brand VARCHAR(255),
            task_id VARCHAR(255),
            platform VARCHAR(100),
            post_url TEXT,
            url_valid BOOLEAN DEFAULT TRUE,
            likes INTEGER,
            comments INTEGER,
            shares INTEGER,
            reach INTEGER,
            total_sales_attributed DECIMAL(10,2),
            source_file VARCHAR(255),
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, task_id, post_url)
        );
        """
        
        create_indexes = [
            "CREATE INDEX IF NOT EXISTS idx_processed_data_user_id ON processed_data(user_id);",
            "CREATE INDEX IF NOT EXISTS idx_processed_data_email ON processed_data(email);",
            "CREATE INDEX IF NOT EXISTS idx_processed_data_platform ON processed_data(platform);",
            "CREATE INDEX IF NOT EXISTS idx_processed_data_task_id ON processed_data(task_id);",
            "CREATE INDEX IF NOT EXISTS idx_processed_data_program_id ON processed_data(program_id);",
            "CREATE INDEX IF NOT EXISTS idx_processed_data_created_at ON processed_data(created_at);"
        ]
        
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(create_processed_data_table)
                
                for index_sql in create_indexes:
                    cur.execute(index_sql)
                
                conn.commit()
                logger.info("Tables and indexes created successfully")

    def extract_user_number_from_filename(self, filename):
        """Extract user number from filename like 'user_123.json'"""
        try:
            # Extract number from filename like 'user_123.json'
            base_name = Path(filename).stem  # Gets 'user_123'
            if base_name.startswith('user_'):
                return int(base_name.split('_')[1])
            return None
        except (ValueError, IndexError):
            return None

    def process_json_file(self, file_path):
        """Process a single JSON file and extract records"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if not data:
                logger.warning(f"Empty data in file: {file_path}")
                return []
            
            records = []
            filename = os.path.basename(file_path)
            user_number = self.extract_user_number_from_filename(filename)
            
            # Parse joined_at if it exists
            joined_at = None
            if data.get('joined_at'):
                try:
                    joined_at = datetime.fromisoformat(data['joined_at'].replace('Z', '+00:00'))
                except:
                    joined_at = None
            
            # Process advocacy programs
            advocacy_programs = data.get('advocacy_programs', [])
            
            if not advocacy_programs:
                # If no advocacy programs, create a basic record
                records.append({
                    'user_id': data.get('user_id'),
                    'name': data.get('name'),
                    'email': data.get('email'),
                    'instagram_handle': data.get('instagram_handle'),
                    'tiktok_handle': data.get('tiktok_handle'),
                    'joined_at': joined_at,
                    'program_id': None,
                    'brand': None,
                    'task_id': None,
                    'platform': None,
                    'post_url': None,
                    'likes': None,
                    'comments': None,
                    'shares': None,
                    'reach': None,
                    'total_sales_attributed': None,
                    'source_file': filename
                })
            else:
                # Process each advocacy program
                for program in advocacy_programs:
                    program_id = program.get('program_id')
                    brand = program.get('brand')
                    total_sales = program.get('total_sales_attributed')
                    
                    tasks_completed = program.get('tasks_completed', [])
                    
                    if not tasks_completed:
                        # Program with no tasks
                        records.append({
                            'user_id': data.get('user_id'),
                            'name': data.get('name'),
                            'email': data.get('email'),
                            'instagram_handle': data.get('instagram_handle'),
                            'tiktok_handle': data.get('tiktok_handle'),
                            'joined_at': joined_at,
                            'program_id': program_id,
                            'brand': brand,
                            'task_id': None,
                            'platform': None,
                            'post_url': None,
                            'likes': None,
                            'comments': None,
                            'shares': None,
                            'reach': None,
                            'total_sales_attributed': total_sales,
                            'source_file': filename
                        })
                    else:
                        # Process each task
                        for task in tasks_completed:
                            records.append({
                                'user_id': data.get('user_id'),
                                'name': data.get('name'),
                                'email': data.get('email'),
                                'instagram_handle': data.get('instagram_handle'),
                                'tiktok_handle': data.get('tiktok_handle'),
                                'joined_at': joined_at,
                                'program_id': program_id,
                                'brand': brand,
                                'task_id': task.get('task_id'),
                                'platform': task.get('platform'),
                                'post_url': task.get('post_url'),
                                'likes': task.get('likes'),
                                'comments': task.get('comments'),
                                'shares': task.get('shares'),
                                'reach': task.get('reach'),
                                'total_sales_attributed': total_sales,
                                'source_file': filename
                            })
            
            return records
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error in file {file_path}: {e}")
            return []
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
            return []

    def upload_mixed_data(self, mixed_dir='mixed'):
        """Upload all JSON files from the mixed directory"""
        if not os.path.exists(mixed_dir):
            logger.error(f"Directory {mixed_dir} not found")
            return False
        
        # Get all JSON files in the mixed directory
        json_files = glob.glob(os.path.join(mixed_dir, '*.json'))
        
        if not json_files:
            logger.warning(f"No JSON files found in {mixed_dir}")
            return True
        
        logger.info(f"Found {len(json_files)} JSON files to process")
        
        all_records = []
        processed_files = 0
        failed_files = 0
        
        # Process each JSON file
        for file_path in sorted(json_files):
            try:
                records = self.process_json_file(file_path)
                all_records.extend(records)
                processed_files += 1
                
                if processed_files % 100 == 0:
                    logger.info(f"Processed {processed_files}/{len(json_files)} files...")
                    
            except Exception as e:
                logger.error(f"Failed to process {file_path}: {e}")
                failed_files += 1
        
        logger.info(f"Processed {processed_files} files successfully, {failed_files} failed")
        logger.info(f"Total records extracted: {len(all_records)}")
        
        if not all_records:
            logger.warning("No records to upload")
            return True
        
        # Prepare data for batch insert
        insert_data = []
        for record in all_records:
            insert_data.append((
                record.get('user_id'),
                record.get('name'),
                record.get('email'),
                True,  # email_valid - assuming emails are valid
                record.get('instagram_handle'),
                record.get('tiktok_handle'),
                record.get('joined_at'),
                record.get('program_id'),
                record.get('brand'),
                record.get('task_id'),
                record.get('platform'),
                record.get('post_url'),
                True,  # url_valid - assuming URLs are valid
                record.get('likes'),
                record.get('comments'),
                record.get('shares'),
                record.get('reach'),
                record.get('total_sales_attributed'),
                record.get('source_file'),
                datetime.now()
            ))
        
        # Upload to database
        insert_sql = """
            INSERT INTO processed_data 
            (user_id, name, email, email_valid, instagram_handle, tiktok_handle, joined_at,
             program_id, brand, task_id, platform, post_url, url_valid, likes, comments, 
             shares, reach, total_sales_attributed, source_file, processed_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id, task_id, post_url) DO NOTHING
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    execute_batch(cur, insert_sql, insert_data, page_size=1000)
                    inserted_count = cur.rowcount
                    conn.commit()
                    
                    logger.info(f"Successfully uploaded {inserted_count} records to Heroku PostgreSQL")
                    return True
                    
        except Exception as e:
            logger.error(f"Error uploading data: {e}")
            return False

    def get_upload_stats(self):
        """Get statistics about uploaded data"""
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    # Get total records
                    cur.execute("SELECT COUNT(*) as total_records FROM processed_data")
                    total_records = cur.fetchone()['total_records']
                    
                    # Get unique users
                    cur.execute("SELECT COUNT(DISTINCT user_id) as unique_users FROM processed_data")
                    unique_users = cur.fetchone()['unique_users']
                    
                    # Get platform breakdown
                    cur.execute("""
                        SELECT platform, COUNT(*) as count 
                        FROM processed_data 
                        WHERE platform IS NOT NULL 
                        GROUP BY platform 
                        ORDER BY count DESC
                    """)
                    platform_stats = cur.fetchall()
                    
                    logger.info(f"Upload Statistics:")
                    logger.info(f"  Total records: {total_records}")
                    logger.info(f"  Unique users: {unique_users}")
                    logger.info(f"  Platform breakdown:")
                    for stat in platform_stats:
                        logger.info(f"    {stat['platform']}: {stat['count']}")
                        
        except Exception as e:
            logger.error(f"Error getting upload stats: {e}")

def main():
    """Main function to upload mixed data to Heroku PostgreSQL"""
    uploader = MixedDataUploader()
    
    try:
        logger.info("Starting upload process...")
        
        # Create tables
        uploader.create_tables()
        
        # Upload mixed data
        success = uploader.upload_mixed_data()
        
        if success:
            # Show upload statistics
            uploader.get_upload_stats()
            logger.info("Data upload to Heroku PostgreSQL completed successfully")
        else:
            logger.error("Data upload failed")
            exit(1)
            
    except Exception as e:
        logger.error(f"Upload process failed: {e}")
        exit(1)

if __name__ == "__main__":
    main()