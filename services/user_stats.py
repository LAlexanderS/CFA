import json
import os
from datetime import datetime
from typing import Dict, Optional

from aiogram.types import Message

USERS_STATS_FILE = "users_stats.json"


def load_user_stats() -> Dict:
    if os.path.exists(USERS_STATS_FILE):
        try:
            with open(USERS_STATS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_user_stats(stats: Dict) -> None:
    with open(USERS_STATS_FILE, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)


def record_user_activity(user_id, username: Optional[str] = None, opened_mini_app: bool = False) -> None:
    stats = load_user_stats()
    user_key = str(user_id)
    now_iso = datetime.utcnow().isoformat()

    entry = stats.get(
        user_key,
        {"user_id": user_id, "username": username, "first_seen": now_iso, "opened_mini_app": False},
    )

    entry["username"] = username or entry.get("username") or "Пользователь"
    entry["last_seen"] = now_iso
    if opened_mini_app:
        entry["opened_mini_app"] = True

    stats[user_key] = entry
    save_user_stats(stats)


def record_user_activity_from_message(message: Message, opened_mini_app: bool = False) -> None:
    if not message or not message.from_user:
        return
    username = message.from_user.username or message.from_user.full_name or "Пользователь"
    record_user_activity(message.from_user.id, username, opened_mini_app)

