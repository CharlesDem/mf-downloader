from datetime import datetime
import time
import schedule
from file_helper import dl_file_to_s3, URL

def job() -> None:
    minute = datetime.now().minute
    if minute % 5 != 0:
        return

    dl_file_to_s3(URL)

def main() -> None:
    schedule.every(1).minutes.do(job)

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()