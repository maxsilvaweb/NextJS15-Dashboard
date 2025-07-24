#!/usr/bin/env python3
"""
Initial bulk upload script - processes all files in mixed directory
Run this once to upload all existing data
"""

import os
import json
import logging
from datetime import datetime
from normalise_data_panda import process_all_files, analyze_data
from upload_to_postgres_heroku import HerokuPostgreSQLUploader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_file_tracking_record():
    """Create initial tracking record of all processed files"""
    import glob
    import hashlib
    
    json_dir = os.path.join(os.path.dirname(__file__), "mixed")
    json_pattern = os.path.join(json_dir, "user_*.json")
    json_files = glob.glob(json_pattern)
    
    tracking_data = {
        "last_run": datetime.now().isoformat(),
        "files_processed": {},
        "total_files": len(json_files)
    }
    
    logger.info(f"Creating tracking record for {len(json_files)} files")
    
    for file_path in json_files:
        filename = os.path.basename(file_path)
        file_stat = os.stat(file_path)
        
        # Generate file hash for change detection
        with open(file_path, 'rb') as f:
            file_hash = hashlib.md5(f.read()).hexdigest()
        
        tracking_data["files_processed"][filename] = {
            "size": file_stat.st_size,
            "mtime": file_stat.st_mtime,
            "hash": file_hash,
            "processed_at": datetime.now().isoformat()
        }
    
    # Save tracking data
    tracking_file = os.path.join(os.path.dirname(__file__), "file_tracking.json")
    with open(tracking_file, 'w') as f:
        json.dump(tracking_data, f, indent=2)
    
    logger.info(f"Tracking data saved to {tracking_file}")
    return tracking_data

def main():
    """Main function for initial bulk upload"""
    logger.info("Starting initial bulk upload process")
    
    # Directory containing JSON files
    json_dir = os.path.join(os.path.dirname(__file__), "mixed")
    
    if not os.path.exists(json_dir):
        logger.error(f"Directory {json_dir} not found")
        return False
    
    try:
        # Process all files
        logger.info("Processing all JSON files")
        processed_data = process_all_files(json_dir)
        
        if not processed_data:
            logger.warning("No data processed")
            return False
        
        # Save processed data
        output_json = os.path.join(os.path.dirname(__file__), "processed_data.json")
        with open(output_json, 'w') as f:
            json.dump(processed_data, f, indent=2)
        logger.info(f"Saved {len(processed_data)} records to {output_json}")
        
        # Generate statistics
        stats = analyze_data(processed_data)
        stats_file = os.path.join(os.path.dirname(__file__), "data_statistics.json")
        with open(stats_file, 'w') as f:
            json.dump(stats, f, indent=2)
        logger.info(f"Statistics saved to {stats_file}")
        
        # Upload to database
        logger.info("Uploading to database")
        uploader = HerokuPostgreSQLUploader()
        uploader.create_tables()
        
        success = uploader.upload_processed_data(output_json)
        if success:
            uploader.log_processing_stats(stats_file)
            logger.info("Database upload completed successfully")
        else:
            logger.error("Database upload failed")
            return False
        
        # Create file tracking record for future incremental runs
        create_file_tracking_record()
        
        logger.info("Initial bulk upload completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Initial upload failed: {e}")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)