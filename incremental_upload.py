#!/usr/bin/env python3
"""
Incremental upload script - detects and processes only new/modified files
Designed for GitHub Actions workflow
"""

import os
import json
import glob
import hashlib
import logging
from datetime import datetime
from typing import List, Dict
from normalise_data_panda import process_json_file, analyze_data
from upload_to_postgres_heroku import HerokuPostgreSQLUploader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class IncrementalProcessor:
    def __init__(self, tracking_file='file_tracking.json'):
        self.tracking_file = tracking_file
        self.tracking_data = self.load_tracking_data()
    
    def load_tracking_data(self) -> Dict:
        """Load existing file tracking data"""
        if os.path.exists(self.tracking_file):
            with open(self.tracking_file, 'r') as f:
                return json.load(f)
        return {
            "last_run": None,
            "files_processed": {},
            "total_files": 0
        }
    
    def save_tracking_data(self):
        """Save updated tracking data"""
        self.tracking_data["last_run"] = datetime.now().isoformat()
        with open(self.tracking_file, 'w') as f:
            json.dump(self.tracking_data, f, indent=2)
    
    def get_file_hash(self, file_path: str) -> str:
        """Generate MD5 hash of file content"""
        with open(file_path, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()
    
    def detect_new_or_modified_files(self, directory: str) -> List[str]:
        """Detect files that are new or have been modified"""
        json_pattern = os.path.join(directory, "user_*.json")
        all_files = glob.glob(json_pattern)
        
        new_or_modified = []
        processed_files = self.tracking_data.get("files_processed", {})
        
        logger.info(f"Checking {len(all_files)} files for changes")
        
        for file_path in all_files:
            filename = os.path.basename(file_path)
            file_stat = os.stat(file_path)
            current_hash = self.get_file_hash(file_path)
            
            # Check if file is new or modified
            if filename not in processed_files:
                logger.info(f"New file detected: {filename}")
                new_or_modified.append(file_path)
            elif (
                processed_files[filename].get("hash") != current_hash or
                processed_files[filename].get("size") != file_stat.st_size
            ):
                logger.info(f"Modified file detected: {filename}")
                new_or_modified.append(file_path)
            
            # Update tracking info
            processed_files[filename] = {
                "size": file_stat.st_size,
                "mtime": file_stat.st_mtime,
                "hash": current_hash,
                "processed_at": datetime.now().isoformat()
            }
        
        self.tracking_data["files_processed"] = processed_files
        self.tracking_data["total_files"] = len(all_files)
        
        return new_or_modified
    
    def process_files(self, files: List[str]) -> List[Dict]:
        """Process a list of files and return processed data"""
        all_data = []
        
        for file_path in files:
            filename = os.path.basename(file_path)
            try:
                logger.info(f"Processing {filename}")
                result = process_json_file(file_path)
                if result:
                    all_data.extend(result)
                    logger.info(f"Processed {filename}: {len(result)} records")
                else:
                    logger.warning(f"No data returned from {filename}")
            except Exception as e:
                logger.error(f"Error processing {filename}: {e}")
        
        return all_data

def main():
    """Main function for incremental processing"""
    logger.info("Starting incremental file detection and upload")
    
    json_dir = os.path.join(os.path.dirname(__file__), "mixed")
    
    if not os.path.exists(json_dir):
        logger.error(f"Directory {json_dir} not found")
        return False
    
    try:
        # Initialize processor
        processor = IncrementalProcessor()
        
        # Detect new or modified files
        files_to_process = processor.detect_new_or_modified_files(json_dir)
        
        if not files_to_process:
            logger.info("No new or modified files detected")
            # Update tracking data even if no files to process
            processor.save_tracking_data()
            return True
        
        logger.info(f"Found {len(files_to_process)} files to process")
        
        # Process the detected files
        processed_data = processor.process_files(files_to_process)
        
        if not processed_data:
            logger.warning("No valid data processed from detected files")
            processor.save_tracking_data()
            return True
        
        # Save incremental processed data
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_json = f"incremental_data_{timestamp}.json"
        with open(output_json, 'w') as f:
            json.dump(processed_data, f, indent=2)
        logger.info(f"Saved {len(processed_data)} records to {output_json}")
        
        # Generate statistics for incremental data
        stats = analyze_data(processed_data)
        stats['files_processed'] = len(files_to_process)
        stats['processing_type'] = 'incremental'
        stats['timestamp'] = timestamp
        
        stats_file = f"incremental_stats_{timestamp}.json"
        with open(stats_file, 'w') as f:
            json.dump(stats, f, indent=2)
        
        # Upload to database
        logger.info("Uploading incremental data to database")
        uploader = HerokuPostgreSQLUploader()
        
        success = uploader.upload_processed_data(output_json)
        if success:
            logger.info(f"Successfully uploaded {len(processed_data)} records")
            
            # Update tracking data
            processor.save_tracking_data()
            
            # Clean up temporary files
            os.remove(output_json)
            os.remove(stats_file)
            
            return True
        else:
            logger.error("Database upload failed")
            return False
            
    except Exception as e:
        logger.error(f"Incremental processing failed: {e}")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)