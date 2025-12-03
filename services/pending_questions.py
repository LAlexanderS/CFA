import json
import os
import time
from typing import Dict, Optional, Tuple

PENDING_QUESTIONS_FILE = "pending_questions.json"


def load_pending_questions() -> Dict:
    if os.path.exists(PENDING_QUESTIONS_FILE):
        try:
            with open(PENDING_QUESTIONS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_pending_questions(questions: Dict) -> None:
    with open(PENDING_QUESTIONS_FILE, "w", encoding="utf-8") as f:
        json.dump(questions, f, ensure_ascii=False, indent=2)


def add_pending_question(user_id, question_text, group_message_id) -> str:
    questions = load_pending_questions()
    question_id = f"{user_id}_{group_message_id}_{int(time.time())}"
    questions[question_id] = {
        "user_id": user_id,
        "question_text": question_text,
        "group_message_id": group_message_id,
        "answered": False,
    }
    save_pending_questions(questions)
    return question_id


def get_pending_question_by_message(group_message_id) -> Tuple[Optional[str], Optional[Dict]]:
    questions = load_pending_questions()
    for question_id, question_data in questions.items():
        if (
            question_data["group_message_id"] == group_message_id
            and not question_data.get("answered", False)
        ):
            return question_id, question_data
    return None, None


def mark_question_answered(question_id: str) -> None:
    questions = load_pending_questions()
    if question_id in questions:
        questions[question_id]["answered"] = True
        save_pending_questions(questions)

