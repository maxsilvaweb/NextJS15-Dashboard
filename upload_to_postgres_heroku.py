import json
import os
import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch
from datetime import datetime
import logging
from dotenv import load_dotenv
from urllib.parse import urlparse

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HerokuPostgreSQLUploader:
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
            user_id INTEGER,  -- Sequential integer based on filename
            original_user_id VARCHAR(255),  -- Store original user_id from JSON
            name VARCHAR(255),
            email VARCHAR(255),
            email_valid BOOLEAN DEFAULT FALSE,
            instagram_handle VARCHAR(255),
            tiktok_handle VARCHAR(255),
            joined_at TIMESTAMP,
            program_id VARCHAR(255),
            brand VARCHAR(255),
            task_id VARCHAR(255),
            platform VARCHAR(100),
            post_url TEXT,
            url_valid BOOLEAN DEFAULT FALSE,
            likes INTEGER,
            comments INTEGER,
            shares INTEGER,
            reach INTEGER,
            total_sales_attributed DECIMAL(10,2),
            source_file VARCHAR(255),
            issues_found INTEGER DEFAULT 0,
            issues_list TEXT,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, email, post_url, task_id)
        );
        """
        
        create_processing_log_table = """
        CREATE TABLE IF NOT EXISTS processing_log (
            id SERIAL PRIMARY KEY,
            total_records INTEGER,
            valid_emails_count INTEGER,
            valid_urls_count INTEGER,
            missing_user_ids_count INTEGER,
            github_run_id VARCHAR(255),
            commit_sha VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
                cur.execute(create_processing_log_table)
                
                for index_sql in create_indexes:
                    cur.execute(index_sql)
                
                conn.commit()
                logger.info("Tables and indexes created successfully")

    def upload_processed_data(self, json_file_path='processed_data.json'):
        """Upload processed data to PostgreSQL"""
        if not os.path.exists(json_file_path):
            logger.error(f"File {json_file_path} not found")
            return False
        
        try:
            with open(json_file_path, 'r') as f:
                data = json.load(f)
            
            if not data:
                logger.warning("No data to upload")
                return True
            
            # Prepare data for batch insert
            insert_data = []
            for record in data:
                # Convert issues_list to JSON string for storage
                issues_list_str = json.dumps(record.get('issues_list', [])) if record.get('issues_list') else None
                
                # Parse joined_at if it exists
                joined_at = None
                if record.get('joined_at'):
                    try:
                        joined_at = datetime.fromisoformat(record['joined_at'].replace('Z', '+00:00'))
                    except:
                        joined_at = None
                
                insert_data.append((
                    record.get('user_id'),  # Now an integer
                    record.get('original_user_id'),  # Original UUID string
                    record.get('name'),
                    record.get('email'),
                    record.get('email_valid', False),
                    record.get('instagram_handle'),
                    record.get('tiktok_handle'),
                    joined_at,
                    record.get('program_id'),
                    record.get('brand'),
                    record.get('task_id'),
                    record.get('platform'),
                    record.get('post_url'),
                    record.get('url_valid', False),
                    record.get('likes'),
                    record.get('comments'),
                    record.get('shares'),
                    record.get('reach'),
                    record.get('total_sales_attributed'),
                    record.get('source_file'),
                    record.get('issues_found', 0),
                    issues_list_str,
                    datetime.now()
                ))
            
            insert_sql = """
                INSERT INTO processed_data 
                (user_id, original_user_id, name, email, email_valid, instagram_handle, tiktok_handle, joined_at,
                 program_id, brand, task_id, platform, post_url, url_valid, likes, comments, 
                 shares, reach, total_sales_attributed, source_file, issues_found, issues_list, processed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id, email, post_url, task_id) DO NOTHING
            """
            
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
    
    def log_processing_stats(self, stats_file_path='data_statistics.json'):
        """Log processing statistics"""
        if not os.path.exists(stats_file_path):
            logger.warning(f"Statistics file {stats_file_path} not found")
            return
        
        try:
            with open(stats_file_path, 'r') as f:
                stats = json.load(f)
            
            insert_sql = """
                INSERT INTO processing_log 
                (total_records, valid_emails_count, valid_urls_count, missing_user_ids_count, github_run_id, commit_sha)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(insert_sql, (
                        stats.get('total_records', 0),
                        stats.get('valid_emails_count', 0),
                        stats.get('valid_urls_count', 0),
                        stats.get('missing_user_ids_count', 0),
                        os.getenv('GITHUB_RUN_ID'),
                        os.getenv('GITHUB_SHA')
                    ))
                    conn.commit()
                    logger.info("Processing statistics logged successfully")
                    
        except Exception as e:
            logger.error(f"Error logging statistics: {e}")

def main():
    """Main function to upload data to Heroku PostgreSQL"""
    uploader = HerokuPostgreSQLUploader()
    
    try:
        # Create tables
        uploader.create_tables()
        
        # Upload processed data
        success = uploader.upload_processed_data()
        
        if success:
            # Log processing statistics
            uploader.log_processing_stats()
            logger.info("Data upload to Heroku PostgreSQL completed successfully")
        else:
            logger.error("Data upload failed")
            exit(1)
            
    except Exception as e:
        logger.error(f"Upload process failed: {e}")
        exit(1)

if __name__ == "__main__":
    main()