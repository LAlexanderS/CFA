import tempfile
from datetime import datetime


def parse_tildaforms_message(text: str) -> dict:
    data = {}
    lines = text.strip().split("\n")
    for line in lines:
        if ":" in line:
            key, value = line.split(":", 1)
            data[key.strip()] = value.strip()
    return data


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
            <h1>🎉 Заявка на выпуск ЦФА</h1>
            <p>Новая заявка поступила через форму</p>
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

    excluded_fields = {"tg_user_id", "tg_username", "tg_first_name", "tg_last_name"}
    for key, value in data.items():
        if key in excluded_fields:
            continue
        formatted_key = format_key(key)
        if value.lower() == "да":
            value_html = '<span class="badge">✓ Да</span>'
        elif value.lower() == "нет":
            value_html = '<span class="badge" style="background: #dc3545;">✗ Нет</span>'
        else:
            value_html = f'<div class="value">{value}</div>'

        html_content += f"""
                <div class="form-item">
                    <label>{formatted_key}</label>
                    {value_html}
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

