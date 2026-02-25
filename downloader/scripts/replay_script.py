from datetime import datetime
import re

from common.db.queries import create_file_metadata
from common.config.config import minio_config as mc

# python -m scripts.replay_script
# play this script manually to register bufr files downloaded before postgres implementation
def replay_index_bufr_file_creation():

    for file in mc.minio_client.list_objects(mc.bufr_bucket,prefix="files-pagb",recursive=True):

        file_name = file.object_name
        num_station  = re.search(r"PAGB(\d+)", file_name).group(1)
        match = re.search(r"_(\d{14})\.bufr\.gz$", file_name)

        timestamp = datetime.strptime(match.group(1), "%Y%m%d%H%M%S")

        create_file_metadata(file.object_name, num_station, timestamp)

        print("Replay script done")


def main():
    replay_index_bufr_file_creation()


if __name__ == "__main__":
    main()

