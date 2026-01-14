import time
import schedule
from file_helper import dl_file, URL

def main():

    schedule.every(5).minutes.do(dl_file, URL)

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__=="__main__":
    main()