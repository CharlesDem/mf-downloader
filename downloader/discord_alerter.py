import requests
import json
from typing import Any, Dict, List
from common.config.config import discord_config as dc

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
            dc.web_hook,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
    except Exception:
        pass #don't crash for that