import re
import tempfile
from datetime import datetime
from typing import Dict, Optional


def parse_tildaforms_message(text: str) -> dict:
    data = {}
    lines = text.strip().split("\n")
    collecting_quiz_comments = False
    quiz_comment_lines = []

    for line in lines:
        if collecting_quiz_comments:
            quiz_comment_lines.append(line)
            continue

        if ":" not in line:
            continue

        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()

        if key == "quiz_comments":
            collecting_quiz_comments = True
            if value:
                quiz_comment_lines.append(value)
            continue

        data[key] = value

    if collecting_quiz_comments:
        data["quiz_comments"] = "\n".join(quiz_comment_lines).strip()

    return data


def _normalize_question_text(text: str) -> str:
    return re.sub(r"[^a-zа-я0-9]+", "", text.lower(), flags=re.IGNORECASE)


def parse_quiz_comments(raw_comments: Optional[str]) -> Dict[str, str]:
    if not raw_comments:
        return {}

    comments_map: dict[str, str] = {}
    current_question = None
    comment_lines: list[str] = []
    in_comment = False

    def flush():
        if current_question and comment_lines:
            normalized = _normalize_question_text(current_question)
            comment_text = "\n".join(comment_lines).strip().strip('"')
            if normalized:
                comments_map[normalized] = comment_text

    for line in raw_comments.splitlines():
        stripped = line.strip()
        if not stripped:
            flush()
            current_question = None
            comment_lines = []
            in_comment = False
            continue

        lower = stripped.lower()
        if lower.startswith("вопрос:"):
            flush()
            current_question = stripped.split(":", 1)[1].strip()
            comment_lines = []
            in_comment = False
            continue

        if lower.startswith("комментарий:"):
            in_comment = True
            comment_lines = [stripped.split(":", 1)[1].strip()]
            continue

        if in_comment:
            comment_lines.append(stripped)

    flush()
    return comments_map


def create_html_from_tildaforms_data(data: dict) -> str:
    def format_key(key: str) -> str:
        formatted = key.replace("_", " ").title()
        replacements = {
            "Tg User Id": "ID пользователя Telegram",
            "Tg Username": "Username Telegram",
            "Tg First Name": "Имя",
            "Tg Last Name": "Фамилия",
        }
        return replacements.get(formatted, formatted)

    user_id = data.get("tg_user_id", "Не указан")
    username = data.get("tg_username", "Не указан")
    first_name = data.get("tg_first_name", "Не указано")
    quiz_comments_map = parse_quiz_comments(data.get("quiz_comments"))

    html_content = f"""<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Заявка на выпуск ЦФА</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
            min-height: 100vh;
        }}
        .container {{
            max-width: 800px;
            margin: 0 auto;
            background: white;
            border-radius: 20px;
            box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
            overflow: hidden;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }}
        .header h1 {{
            font-size: 28px;
            font-weight: 600;
            margin-bottom: 10px;
        }}
        .content {{
            padding: 40px;
        }}
        .user-info {{
            background: #f8f9fa;
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 30px;
        }}
        .user-details {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
        }}
        .form-data {{
            margin-top: 30px;
        }}
        .form-item {{
            border-bottom: 1px solid #e9ecef;
            padding: 20px 0;
        }}
        .badge {{
            display: inline-block;
            padding: 6px 12px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border-radius: 6px;
            font-size: 14px;
            font-weight: 500;
        }}
        .form-item .value {{
            font-size: 16px;
            color: #333;
            padding: 10px 15px;
            background: #f8f9fa;
            border-radius: 8px;
            margin-top: 5px;
        }}
        .comment-block {{
            margin-top: 10px;
            padding: 12px 15px;
            background: #fff9e6;
            border-radius: 8px;
            border-left: 4px solid #f6c343;
        }}
        .comment-title {{
            font-size: 13px;
            text-transform: uppercase;
            letter-spacing: 0.04em;
            color: #b38400;
            margin-bottom: 6px;
        }}
        .comment-text {{
            font-size: 15px;
            color: #7a5c00;
            line-height: 1.4;
        }}
        .footer {{
            background: #f8f9fa;
            padding: 20px 40px;
            text-align: center;
            color: #666;
            font-size: 12px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🎉 Путеводитель по ЦФА</h1>
            <p>Чек-лист сформирован из формы ответов</p>
        </div>
        <div class="content">
            <div class="user-info">
                <h2>Информация о пользователе</h2>
                <div class="user-details"> 
                    <div class="user-detail">
                        <label>ID пользователя</label>
                        <span>{user_id}</span>
                    </div>
                    <div class="user-detail">
                        <label>Username</label>
                        <span>@{username}</span>
                    </div>
                    <div class="user-detail">
                        <label>Имя</label>
                        <span>{first_name}</span>
                    </div>
                </div>
            </div>
            <div class="form-data">
                <h2>Данные заявки</h2>"""

    excluded_fields = {"tg_user_id", "tg_username", "tg_first_name", "tg_last_name", "quiz_comments"}
    for key, value in data.items():
        if key in excluded_fields:
            continue
        formatted_key = format_key(key)
        value_str = value.strip() if isinstance(value, str) else str(value)
        value_lower = value_str.lower()
        if value_lower == "да":
            value_html = '<span class="badge">✓ Да</span>'
        elif value_lower == "нет":
            value_html = '<span class="badge" style="background: #dc3545;">✗ Нет</span>'
        else:
            value_html = f'<div class="value">{value_str}</div>'

        normalized_question = _normalize_question_text(formatted_key)
        comment_text = quiz_comments_map.get(normalized_question)
        comment_html = ""
        if comment_text:
            comment_text_html = comment_text.replace("\n", "<br>")
            comment_html = f"""
                    <div class="comment-block">
                        <div class="comment-title">Комментарий к вопросу</div>
                        <div class="comment-text">{comment_text_html}</div>
                    </div>"""

        html_content += f"""
                <div class="form-item">
                    <label>{formatted_key}</label>
                    {value_html}
                    {comment_html}
                </div>"""

    html_content += f"""
            </div>
        </div>
        <div class="footer">
            <p>Заявка создана: {datetime.now().strftime('%d.%m.%Y %H:%M')}</p>
        </div>
    </div>
</body>
</html>"""
    return html_content


def save_html_temp(html_content: str) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    temp_file = tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".html",
        prefix=f"tildaforms_{timestamp}_",
        delete=False,
        encoding="utf-8",
    )
    temp_file.write(html_content)
    temp_file.close()
    return temp_file.name

