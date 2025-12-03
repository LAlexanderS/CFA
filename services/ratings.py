import json
import os
from typing import Dict, Optional

RATINGS_FILE = "question_ratings.json"
MARKER_HASH_FILE = "marker_hash_map.json"


def load_ratings() -> Dict:
    if os.path.exists(RATINGS_FILE):
        try:
            with open(RATINGS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_ratings(ratings: Dict) -> None:
    with open(RATINGS_FILE, "w", encoding="utf-8") as f:
        json.dump(ratings, f, ensure_ascii=False, indent=2)


def load_hash_map() -> Dict:
    if os.path.exists(MARKER_HASH_FILE):
        try:
            with open(MARKER_HASH_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_hash_map(hash_map: Dict) -> None:
    with open(MARKER_HASH_FILE, "w", encoding="utf-8") as f:
        json.dump(hash_map, f, ensure_ascii=False, indent=2)


def get_marker_hash(marker: str) -> str:
    import hashlib

    hash_obj = hashlib.md5(marker.encode("utf-8"))
    return hash_obj.hexdigest()[:16]


def get_marker_from_hash(hash_value: str) -> Optional[str]:
    hash_map = load_hash_map()
    return hash_map.get(hash_value)


def store_marker_hash(marker: str) -> str:
    hash_map = load_hash_map()
    hash_value = get_marker_hash(marker)
    hash_map[hash_value] = marker
    save_hash_map(hash_map)
    return hash_value


def add_rating(marker: str, rating_type: str, user_id, username: Optional[str] = None) -> bool:
    ratings = load_ratings()
    if marker not in ratings:
        ratings[marker] = {"up": 0, "down": 0, "users": {}}

    user_id_str = str(user_id)
    username = username or "Пользователь"

    user_entry = ratings[marker]["users"].get(user_id_str)
    if user_entry:
        previous_rating = user_entry.get("value") if isinstance(user_entry, dict) else user_entry
        if previous_rating == rating_type:
            return True
        ratings[marker][previous_rating] = max(0, ratings[marker][previous_rating] - 1)

    ratings[marker][rating_type] += 1
    ratings[marker]["users"][user_id_str] = {"value": rating_type, "username": username}
    save_ratings(ratings)
    return True


def get_ratings(marker: str) -> Dict:
    ratings = load_ratings()
    if marker not in ratings:
        ratings[marker] = {"up": 0, "down": 0, "users": {}}

    users = ratings[marker].get("users", {})
    normalized_users = {}
    for user_id, data in users.items():
        if isinstance(data, dict):
            value = data.get("value")
            username = data.get("username")
        else:
            value = data
            username = None
        normalized_users[user_id] = {"value": value, "username": username}

    ratings[marker]["users"] = normalized_users
    return {"up": ratings[marker].get("up", 0), "down": ratings[marker].get("down", 0), "users": normalized_users}


def has_user_rated(marker: str, user_id) -> bool:
    ratings = load_ratings()
    user_entry = ratings.get(marker, {}).get("users", {}).get(str(user_id))
    if user_entry is None:
        return False
    if isinstance(user_entry, dict):
        return user_entry.get("value") is not None
    return True


def get_user_rating(marker: str, user_id):
    ratings = load_ratings()
    user_entry = ratings.get(marker, {}).get("users", {}).get(str(user_id))
    if isinstance(user_entry, dict):
        return user_entry.get("value")
    return user_entry


def get_all_ratings() -> Dict:
    return load_ratings()

