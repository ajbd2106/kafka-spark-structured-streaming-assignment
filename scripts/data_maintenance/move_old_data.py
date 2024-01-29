# move_old_data.py
import logging
import yaml
from datetime import datetime, timedelta
import shutil
import os

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def read_batch_config():
    with open('conf/batch_config.yaml', 'r') as file:
        return yaml.safe_load(file)

def move_old_data(raw_zone_path, archive_path, retention_days):
    try:
        current_date = datetime.now()
        cutoff_date = current_date - timedelta(days=retention_days)

        # Move files older than the retention period to the archive location
        for file_name in os.listdir(raw_zone_path):
            file_path = os.path.join(raw_zone_path, file_name)

            if os.path.isfile(file_path):
                file_creation_time = datetime.fromtimestamp(os.path.getctime(file_path))

                if file_creation_time < cutoff_date:
                    shutil.move(file_path, os.path.join(archive_path, file_name))

        logger.info("Old data moved successfully.")
    except Exception as e:
        logger.error(f"Error moving old data: {e}")
        raise

# Example usage
if __name__ == "__main__":
    batch_config = read_batch_config()

    raw_zone_path = batch_config['hdfs']['raw_zone_path']
    archive_path = '/path/to/archive'
    retention_days = 30

    try:
        move_old_data(raw_zone_path, archive_path, retention_days)
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
