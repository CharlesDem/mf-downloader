import os
from dotenv import load_dotenv
import requests
import json
from typing import Any, Dict, List

load_dotenv()

WEBHOOK_URL = os.environ["WEBHOOK_DISCORD"]

def discord_error(
    message: str,
    title: str = "Error dowloading MF files",
    level: str = "ERROR",
) -> None:
    payload: Dict[str, List[Dict[str, Any]]] = {
        "content": "@everyone", # type: ignore
        "embeds": [
            {
                "title": title,
                "description": message,
                "color": 15158332,
                "fields": [
                    {
                        "name": "Level",
                        "value": level,
                        "inline": True,
                    }
                ],
            }
        ]
    }

    try:
        requests.post(
            WEBHOOK_URL,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
    except Exception:
        pass #don't crash for that