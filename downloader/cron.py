from datetime import datetime
import time
import schedule
import structlog
from file_helper import dl_pagb_all

OFFSET_MINUTES = 5   #avoid 'quart d'heure pile poil'

log = structlog.get_logger()


def job() -> None:
    now = datetime.now()
    minute = now.minute

    if (minute - OFFSET_MINUTES) % 15 != 0:
        return

    dl_pagb_all("files-pagb")

def main() -> None:

    try :
        schedule.every(1).minutes.do(job)

        while True:
            schedule.run_pending()
            time.sleep(1)
    except Exception as e:
        log.error(f"Unmanaged error, {e}")

if __name__ == "__main__":
    main()