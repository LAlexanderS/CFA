# Импорт библиотек
import pandas as pd
import numpy as np
import sys
import logging
import asyncio
import os
import re
import json
import hashlib
import time
from datetime import datetime
from aiogram import Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import CommandStart
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder
from aiogram.enums import ParseMode
from aiogram import Router, F, Bot
from aiogram.types import FSInputFile
from aiogram.filters import Command
from dotenv import load_dotenv
from aiogram.types import WebAppInfo

load_dotenv()

TOKEN = os.getenv("TOKEN")
TOKEN_GROUP = os.getenv("TOKEN_GROUP")

# # Подключение к боту
# with open('token.txt', 'r') as file:
#     token_value = file.read().strip()
# os.environ['TELEGRAM_BOT_TOKEN'] = token_value
# TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')

# Определяем диспетчер и перехватчик
dp = Dispatcher()
router = Router()
dp.include_router(router)

global structure

# Определяем ф-ию инициализации
async def main() -> None:
    bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    print("hi")
    await dp.start_polling(bot)


# --------------------------------Словари-------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
# async def read_dict():

global key_buttons_1rang, key_buttons_text, key_buttons_termins, key_all_opros

# Функция для инициализации/обновления словарей
def update_dictionaries():
    global key_buttons_1rang, key_buttons_text, key_buttons_termins, key_all_opros

    structure_f = pd.read_excel(os.path.abspath('structure.xlsx'), engine='openpyxl')

    # Инициализация key_buttons_1rang
    level, marker, text_messege, buttns = structure_f[structure_f['Уровень'] == 1].iloc[0]
    buttns = [btn.strip() for btn in buttns.strip('[]').split(']\n[')]
    key_buttons_1rang = {}
    for i in buttns:
        key_buttons_1rang[i] = 2

    # Инициализация остальных словарей
    key_buttons_text = structure_f.set_index('Маркер')['Уровень'].to_dict()

    key_buttons_termins = structure_f[structure_f['Маркер'] == list(key_buttons_1rang.keys())[0]]['Кнопки'].iloc[0]
    key_buttons_termins = [btn.strip() for btn in key_buttons_termins.strip('[]').split(']\n[')]

    key_all_opros = list(key_buttons_1rang.keys()) + key_buttons_termins


# Вызов функции при старте для инициализации
update_dictionaries()


user_context={}

# --------------------------------Система ответов администраторов--------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
PENDING_QUESTIONS_FILE = 'pending_questions.json'

def load_pending_questions():
    """Загружает список ожидающих ответа вопросов"""
    if os.path.exists(PENDING_QUESTIONS_FILE):
        try:
            with open(PENDING_QUESTIONS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_pending_questions(questions):
    """Сохраняет список ожидающих ответа вопросов"""
    with open(PENDING_QUESTIONS_FILE, 'w', encoding='utf-8') as f:
        json.dump(questions, f, ensure_ascii=False, indent=2)

def add_pending_question(user_id, question_text, group_message_id):
    """Добавляет вопрос в список ожидающих ответа"""
    questions = load_pending_questions()
    question_id = f"{user_id}_{group_message_id}_{int(time.time())}"
    questions[question_id] = {
        'user_id': user_id,
        'question_text': question_text,
        'group_message_id': group_message_id,
        'answered': False
    }
    save_pending_questions(questions)
    return question_id

def get_pending_question_by_message(group_message_id):
    """Получает вопрос по ID сообщения в группе"""
    questions = load_pending_questions()
    for question_id, question_data in questions.items():
        if question_data['group_message_id'] == group_message_id and not question_data.get('answered', False):
            return question_id, question_data
    return None, None

def mark_question_answered(question_id):
    """Отмечает вопрос как отвеченный"""
    questions = load_pending_questions()
    if question_id in questions:
        questions[question_id]['answered'] = True
        save_pending_questions(questions)

# --------------------------------Система оценок------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
RATINGS_FILE = 'question_ratings.json'
MARKER_HASH_FILE = 'marker_hash_map.json'
USERS_STATS_FILE = 'users_stats.json'

def load_ratings():
    """Загружает оценки из файла"""
    if os.path.exists(RATINGS_FILE):
        try:
            with open(RATINGS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_ratings(ratings):
    """Сохраняет оценки в файл"""
    with open(RATINGS_FILE, 'w', encoding='utf-8') as f:
        json.dump(ratings, f, ensure_ascii=False, indent=2)

def load_hash_map():
    """Загружает маппинг хеш -> маркер"""
    if os.path.exists(MARKER_HASH_FILE):
        try:
            with open(MARKER_HASH_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_hash_map(hash_map):
    """Сохраняет маппинг хеш -> маркер"""
    with open(MARKER_HASH_FILE, 'w', encoding='utf-8') as f:
        json.dump(hash_map, f, ensure_ascii=False, indent=2)

def get_marker_hash(marker):
    """Получает хеш маркера (короткий идентификатор для callback_data)"""
    # Используем MD5 хеш и берем первые 16 символов (достаточно для уникальности)
    hash_obj = hashlib.md5(marker.encode('utf-8'))
    return hash_obj.hexdigest()[:16]

def get_marker_from_hash(hash_value):
    """Восстанавливает маркер по хешу"""
    hash_map = load_hash_map()
    return hash_map.get(hash_value, None)

def store_marker_hash(marker):
    """Сохраняет маппинг маркер -> хеш"""
    hash_map = load_hash_map()
    hash_value = get_marker_hash(marker)
    hash_map[hash_value] = marker
    save_hash_map(hash_map)
    return hash_value

def add_rating(marker, rating_type, user_id, username=None):
    """Добавляет или изменяет оценку к вопросу (rating_type: 'up' или 'down')"""
    ratings = load_ratings()
    if marker not in ratings:
        ratings[marker] = {'up': 0, 'down': 0, 'users': {}}
    
    user_id_str = str(user_id)
    username = username or "Пользователь"
    
    # Проверяем, оценивал ли уже этот пользователь
    if user_id_str in ratings[marker].get('users', {}):
        # Пользователь уже оценил - меняем оценку
        previous_entry = ratings[marker]['users'][user_id_str]
        if isinstance(previous_entry, dict):
            previous_rating = previous_entry.get('value')
        else:
            previous_rating = previous_entry
        
        # Если пользователь нажимает на ту же кнопку, ничего не делаем
        if previous_rating == rating_type:
            return True
        
        # Убираем предыдущую оценку
        ratings[marker][previous_rating] -= 1
        if ratings[marker][previous_rating] < 0:
            ratings[marker][previous_rating] = 0
    
    # Добавляем новую оценку
    ratings[marker][rating_type] += 1
    if 'users' not in ratings[marker]:
        ratings[marker]['users'] = {}
    ratings[marker]['users'][user_id_str] = {
        'value': rating_type,
        'username': username
    }
    save_ratings(ratings)
    return True

def get_ratings(marker):
    """Получает оценки для вопроса"""
    ratings = load_ratings()
    if marker not in ratings:
        ratings[marker] = {'up': 0, 'down': 0, 'users': {}}

    # Нормализуем структуру users
    users = ratings[marker].get('users', {})
    normalized_users = {}
    for user_id, data in users.items():
        if isinstance(data, dict):
            value = data.get('value')
            username = data.get('username')
        else:
            value = data
            username = None
        normalized_users[user_id] = {
            'value': value,
            'username': username
        }
    ratings[marker]['users'] = normalized_users

    return {
        'up': ratings[marker].get('up', 0),
        'down': ratings[marker].get('down', 0),
        'users': normalized_users
    }

def has_user_rated(marker, user_id):
    """Проверяет, оценивал ли пользователь этот вопрос"""
    ratings = load_ratings()
    if marker not in ratings:
        return False
    user_id_str = str(user_id)
    user_entry = ratings[marker].get('users', {}).get(user_id_str)
    if user_entry is None:
        return False
    if isinstance(user_entry, dict):
        return user_entry.get('value') is not None
    return True

def get_user_rating(marker, user_id):
    """Получает оценку пользователя для вопроса"""
    ratings = load_ratings()
    if marker not in ratings:
        return None
    user_id_str = str(user_id)
    user_entry = ratings[marker].get('users', {}).get(user_id_str)
    if isinstance(user_entry, dict):
        return user_entry.get('value')
    return user_entry

def get_all_ratings():
    """Получает все оценки для администратора"""
    return load_ratings()

# --------------------------------Учет пользователей--------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
def load_user_stats():
    """Загружает статистику пользователей"""
    if os.path.exists(USERS_STATS_FILE):
        try:
            with open(USERS_STATS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_user_stats(stats):
    """Сохраняет статистику пользователей"""
    with open(USERS_STATS_FILE, 'w', encoding='utf-8') as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

def record_user_activity(user_id, username=None, opened_mini_app=False):
    """Фиксирует активность пользователя"""
    stats = load_user_stats()
    user_key = str(user_id)
    now_iso = datetime.utcnow().isoformat()

    entry = stats.get(user_key, {
        'user_id': user_id,
        'username': username,
        'first_seen': now_iso,
        'opened_mini_app': False
    })

    entry['username'] = username or entry.get('username') or "Пользователь"
    entry['last_seen'] = now_iso
    if opened_mini_app:
        entry['opened_mini_app'] = True

    stats[user_key] = entry
    save_user_stats(stats)

def record_user_activity_from_message(message: Message, opened_mini_app: bool = False):
    """Удобный хелпер для записи активности из объекта Message"""
    if not message or not message.from_user:
        return
    username = message.from_user.username or message.from_user.full_name or "Пользователь"
    record_user_activity(message.from_user.id, username, opened_mini_app)

# --------------------------------Функции-------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
async def read_table(type_z, message):
    structure_f = pd.read_excel(os.path.abspath('structure.xlsx'), engine='openpyxl')
    if type_z == 1:
        level, marker, text_messege, buttns = structure_f[structure_f['Уровень'] == 1].iloc[0]
        return level, marker, text_messege, buttns
    elif type_z == 2:
        level, marker, text_messege, buttns = \
            structure_f[(structure_f['Уровень'] == 2) & (structure_f['Маркер'] == message.text)].iloc[0]
        return level, marker, text_messege, buttns
    elif type_z == 3:
        level, marker, text_messege, buttns = \
            structure_f[structure_f['Маркер'] == 'Задать свой вопрос о ЦФА'].iloc[0]
        return level, marker, text_messege, buttns
    elif type_z == 4:
        level, marker, text_messege, buttns = structure_f[structure_f['Маркер'] == message.text].iloc[0]
        return level, marker, text_messege, buttns
    elif type_z == 5:
        structure_f = structure_f[~structure_f['Маркер'].isin(key_all_opros)]
        level, marker, text_messege, buttns = structure_f[structure_f['Маркер'] == message.text].iloc[0]
        return level, marker, text_messege, buttns



# --------------------------------Загрузка структуры--------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
@dp.message(Command("update_table"))  # Начать обновление таблицы
async def send_me_message(message: Message, bot: Bot):
    await bot.send_message(
        chat_id=message.from_user.id,
        text="Отправьте файл в формате xlsx! Не меняйте его название!")


@dp.message(F.document)
async def doc_message(message: Message, bot: Bot):
    document = message.document
    if document.file_name and document.file_name.endswith('.xlsx'):
        try:
            destination = f"{document.file_name}"
            await bot.download(document, destination=destination)
            new_structure = pd.read_excel(destination, engine='openpyxl')
            structure = new_structure.copy()

            # Обновляем словари после загрузки нового файла
            update_dictionaries()

            await bot.send_message(chat_id=message.from_user.id, text="Таблица успешно обновлена!")
        except Exception as e:
            await bot.send_message(chat_id=message.from_user.id, text=f"Ошибка при обработке файла: {str(e)}")


# Используйте Command вместо F.text
@dp.message(Command("download"))
async def download_command(message: Message, bot: Bot):
    try:
        await bot.send_document(
            chat_id=message.from_user.id,
            document=FSInputFile(os.path.abspath('structure.xlsx'))
        )
    except Exception as e:
        await message.answer(f"Ошибка при отправке файла: {str(e)}")



# --------------------------------Обработчики---------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
@dp.message(CommandStart())  # Первый уровень
async def cmd_start1(message: Message, bot: Bot):
    record_user_activity_from_message(message)
    level, marker, text_messege, buttns = await read_table(1, message)
    builder = ReplyKeyboardBuilder()
    buttns = [btn.strip() for btn in buttns.strip('[]').split(']\n[')]
    for button_text in buttns:
        builder.add(KeyboardButton(text=button_text))
    builder.adjust(1)

    # Добавляем кнопку, которая откроет ссылку в браузере Telegram
    builder.add(KeyboardButton(
        text="Хочу выпустить ЦФА",
        web_app=WebAppInfo(url="https://easycfa.tilda.ws/")
    ))
    builder.adjust(1)

    await bot.send_message(
        chat_id=message.chat.id,
        text=text_messege,
        reply_markup=builder.as_markup(), parse_mode=ParseMode.HTML
    )

# @router.message(F.text == 'Главная')  # Первый уровень
# async def cmd_start1(message: Message, bot: Bot):
#     level, marker, text_messege, buttns = await read_table(1, message)
#     builder = ReplyKeyboardBuilder()
#     buttns = [btn.strip() for btn in buttns.strip('[]').split(']\n[')]
#     for button_text in buttns:
#         builder.add(KeyboardButton(text=button_text))
#     builder.adjust(1)
#
#     await bot.send_message(
#         chat_id=message.chat.id,
#         text="Что вас интересует?",
#         reply_markup=builder.as_markup(), parse_mode=ParseMode.HTML
#     )

@router.message(F.text == 'Главная')
async def cmd_start1(message: Message, bot: Bot):
    record_user_activity_from_message(message)
    level, marker, text_messege, buttns = await read_table(1, message)
    builder = ReplyKeyboardBuilder()

    buttns = [btn.strip() for btn in buttns.strip('[]').split(']\n[')]
    for button_text in buttns:
        builder.add(KeyboardButton(text=button_text))

    # Добавляем кнопку, которая откроет ссылку в браузере Telegram
    builder.add(KeyboardButton(
        text="Хочу выпустить ЦФА",
        web_app=WebAppInfo(url="https://easycfa.tilda.ws/")
    ))
    builder.adjust(1)

    await message.answer(
        text='Что вас интересует?',
        reply_markup=builder.as_markup(),
        parse_mode="HTML"
    )





@router.message(lambda message: message.text not in key_buttons_text.keys()) # Второй уровень - работа с текстом
async def fn_text(message: Message, bot: Bot):
    # with open('token_group.txt', 'r') as file:
    #     token_group = file.read().strip()

    record_user_activity_from_message(message)
    user_id = message.from_user.id
    # Проверяем, ожидаем ли мы вопрос от пользователя
    if user_id in user_context and user_context[user_id].get('waiting_for_question', False):
        context = user_context[user_id]
        last_bot_message = context.get('last_bot_message')

        # Получаем ожидаемый текст приглашения
        level, marker, expected_text, buttns = await read_table(3, message)

        if last_bot_message == expected_text:
            username = message.from_user.username or "Пользователь"
            user_id = message.from_user.id
            question_text = message.text
            
            # Формируем сообщение для группы администраторов
            text_messege = f"Добрый день! \nВам поступил новый запрос по ЦФА от @{username} (ID: {user_id})\nТекст запроса: <pre>{question_text}</pre>\n\n<i>Ответьте на это сообщение (reply), чтобы отправить ответ пользователю.</i>"
            
            # Отправляем вопрос в группу администраторов
            sent_message = await bot.send_message(
                chat_id=TOKEN_GROUP,
                text=text_messege, 
                parse_mode=ParseMode.HTML
            )
            
            # Сохраняем вопрос в список ожидающих ответа
            add_pending_question(user_id, question_text, sent_message.message_id)
            
            # Отправляем подтверждение пользователю
            level, marker, text_messege, buttns = await read_table(3, message)
            await bot.send_message(
                chat_id=message.chat.id,
                text=buttns, 
                parse_mode=ParseMode.HTML
            )

        # Очищаем контекст
        del user_context[user_id]
        return


@router.message(lambda message: message.text in key_buttons_1rang.keys()) # Второй уровень
async def fn_1(message: Message, bot: Bot):
    record_user_activity_from_message(message)
    internal_command = key_buttons_1rang[message.text]
    # print(message.text)

    level, marker, text_messege, buttns = await read_table(1, message)
    buttns_list = [btn.strip() for btn in buttns.strip('[]').split(']\n[')]

    if internal_command == 2 and message.text != 'Задать свой вопрос о ЦФА':
        level, marker, text_messege, buttns = await read_table(2, message)
        builder = ReplyKeyboardBuilder()
        buttns = [btn.strip() for btn in buttns.strip('[]').split(']\n[')]
        for button_text in buttns:
            builder.add(KeyboardButton(text=button_text))
        #builder.add(KeyboardButton(text='/start'))
        builder.adjust(1)

        await bot.send_message(
            chat_id=message.chat.id,
            text=text_messege,
            reply_markup=builder.as_markup(), parse_mode=ParseMode.HTML)
    elif internal_command == 2 and message.text == 'Задать свой вопрос о ЦФА':
        level, marker, text_messege, buttns = await read_table(3, message)

        # Устанавливаем контекст для пользователя
        user_context[message.from_user.id] = {
            'waiting_for_question': True,
            'last_bot_message': text_messege
        }

        await bot.send_message(
            chat_id=message.chat.id,
            text=text_messege,
            parse_mode=ParseMode.HTML
        )


@router.message(lambda message: message.text in key_buttons_termins) # Третий уровень ответы
async def fn_2(message: Message, bot: Bot):
    record_user_activity_from_message(message)
    # print('key_buttons_termins = ', message.text)
    level, marker, text_messege, buttns = await read_table(4, message)
    # print(marker)
    
    # Создаем обычную клавиатуру для навигации
    builder = ReplyKeyboardBuilder()
    buttns = [btn.strip() for btn in buttns.strip('[]').split(']\n[')]
    for button_text in buttns:
        builder.add(KeyboardButton(text=button_text))
    #builder.add(KeyboardButton(text='/start'))
    builder.adjust(1)
    
    # Создаем inline клавиатуру для оценки вопроса
    # Используем хеш маркера для callback_data (короткий и безопасный)
    ratings = get_ratings(marker)
    user_id = message.from_user.id
    user_rated = has_user_rated(marker, user_id)
    user_rating = get_user_rating(marker, user_id) if user_rated else None
    
    marker_hash = store_marker_hash(marker)  # Сохраняем маппинг и получаем хеш
    inline_builder = InlineKeyboardBuilder()
    
    # Всегда используем up: и down: в callback_data, чтобы можно было изменять оценку
    if user_rated and user_rating == 'up':
        inline_builder.add(InlineKeyboardButton(
            text=f"👍 {ratings['up']} ✓",
            callback_data=f"up:{marker_hash}"
        ))
        inline_builder.add(InlineKeyboardButton(
            text=f"👎 {ratings['down']}",
            callback_data=f"down:{marker_hash}"
        ))
    elif user_rated and user_rating == 'down':
        inline_builder.add(InlineKeyboardButton(
            text=f"👍 {ratings['up']}",
            callback_data=f"up:{marker_hash}"
        ))
        inline_builder.add(InlineKeyboardButton(
            text=f"👎 {ratings['down']} ✓",
            callback_data=f"down:{marker_hash}"
        ))
    else:
        inline_builder.add(InlineKeyboardButton(
            text=f"👍 {ratings['up']}",
            callback_data=f"up:{marker_hash}"
        ))
        inline_builder.add(InlineKeyboardButton(
            text=f"👎 {ratings['down']}",
            callback_data=f"down:{marker_hash}"
        ))
    inline_builder.adjust(2)
    
    # Отправляем сообщение с ответом и кнопками оценки в одном сообщении
    # Inline клавиатура будет в том же сообщении с ответом
    await bot.send_message(
        chat_id=message.chat.id,
        text=text_messege,
        reply_markup=inline_builder.as_markup(), 
        parse_mode=ParseMode.HTML
    )


@router.message(lambda message: message.text not in key_all_opros) # Третий уровень опрос
async def fn_3(message: Message, bot: Bot):
    record_user_activity_from_message(message)
    # print('key_all_opros = ', message.text)
    level, marker, text_messege, buttns = await read_table(5, message)

    # Создаем обычную клавиатуру для навигации
    builder = ReplyKeyboardBuilder()
    buttns = [btn.strip() for btn in buttns.strip('[]').split(']\n[')]
    for button_text in buttns:
        builder.add(KeyboardButton(text=button_text))
    #builder.add(KeyboardButton(text='/start'))
    builder.adjust(1)

    # Создаем inline клавиатуру для оценки вопроса
    # Используем хеш маркера для callback_data (короткий и безопасный)
    ratings = get_ratings(marker)
    user_id = message.from_user.id
    user_rated = has_user_rated(marker, user_id)
    user_rating = get_user_rating(marker, user_id) if user_rated else None
    
    marker_hash = store_marker_hash(marker)  # Сохраняем маппинг и получаем хеш
    inline_builder = InlineKeyboardBuilder()
    
    # Всегда используем up: и down: в callback_data, чтобы можно было изменять оценку
    if user_rated and user_rating == 'up':
        inline_builder.add(InlineKeyboardButton(
            text=f"👍 {ratings['up']} ✓",
            callback_data=f"up:{marker_hash}"
        ))
        inline_builder.add(InlineKeyboardButton(
            text=f"👎 {ratings['down']}",
            callback_data=f"down:{marker_hash}"
        ))
    elif user_rated and user_rating == 'down':
        inline_builder.add(InlineKeyboardButton(
            text=f"👍 {ratings['up']}",
            callback_data=f"up:{marker_hash}"
        ))
        inline_builder.add(InlineKeyboardButton(
            text=f"👎 {ratings['down']} ✓",
            callback_data=f"down:{marker_hash}"
        ))
    else:
        inline_builder.add(InlineKeyboardButton(
            text=f"👍 {ratings['up']}",
            callback_data=f"up:{marker_hash}"
        ))
        inline_builder.add(InlineKeyboardButton(
            text=f"👎 {ratings['down']}",
            callback_data=f"down:{marker_hash}"
        ))
    inline_builder.adjust(2)

    # Отправляем сообщение с ответом и кнопками оценки в одном сообщении
    # Inline клавиатура будет в том же сообщении с ответом
    await bot.send_message(
        chat_id=message.chat.id,
        text=text_messege,
        reply_markup=inline_builder.as_markup(), 
        parse_mode=ParseMode.HTML
    )

# --------------------------------Обработчик WebApp---------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
@router.message(F.web_app_data)
async def handle_web_app_data(message: Message, bot: Bot):
    """Регистрирует факт открытия мини-приложения"""
    record_user_activity_from_message(message, opened_mini_app=True)
    data_preview = message.web_app_data.data if message.web_app_data else ""
    await message.answer("Спасибо! Данные мини-приложения получены.", parse_mode=ParseMode.HTML)
    logging.info(f"Получены данные из mini app от пользователя {message.from_user.id}: {data_preview}")


# --------------------------------Обработчик оценок--------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
@router.callback_query(F.data.startswith("up:") | F.data.startswith("down:"))
async def handle_rating(callback: CallbackQuery, bot: Bot):
    """Обработчик нажатий на кнопки оценки"""
    data = callback.data
    user_id = callback.from_user.id
    
    parts = data.split(":", 1)
    if len(parts) != 2:
        await callback.answer("Ошибка обработки оценки")
        return
    
    rating_type = parts[0]  # "up" или "down"
    marker_hash = parts[1]  # Хеш маркера
    
    # Восстанавливаем маркер по хешу
    marker = get_marker_from_hash(marker_hash)
    if marker is None:
        await callback.answer("Ошибка: маркер не найден")
        return
    
    # Получаем текущую оценку пользователя
    current_rating = get_user_rating(marker, user_id)
    
    # Если пользователь нажимает на ту же кнопку, что уже выбрал - ничего не делаем
    if current_rating == rating_type:
        await callback.answer("Вы уже выбрали эту оценку")
        return
    
    # Добавляем или изменяем оценку
    username = callback.from_user.username or callback.from_user.full_name or "Пользователь"
    add_rating(marker, rating_type, user_id, username)
    
    # Показываем подтверждение
    if current_rating is None:
        # Первая оценка
        if rating_type == "up":
            await callback.answer("Спасибо за положительную оценку! 👍")
        else:
            await callback.answer("Спасибо за обратную связь!")
    else:
        # Изменение оценки
        if rating_type == "up":
            await callback.answer("Оценка изменена на 👍")
        else:
            await callback.answer("Оценка изменена на 👎")
    
    # Обновляем кнопки с новыми значениями
    ratings = get_ratings(marker)
    user_rating = get_user_rating(marker, user_id)
    
    inline_builder = InlineKeyboardBuilder()
    # Показываем оценку пользователя с галочкой
    if user_rating == 'up':
        inline_builder.add(InlineKeyboardButton(
            text=f"👍 {ratings['up']} ✓",
            callback_data=f"up:{marker_hash}"
        ))
        inline_builder.add(InlineKeyboardButton(
            text=f"👎 {ratings['down']}",
            callback_data=f"down:{marker_hash}"
        ))
    else:
        inline_builder.add(InlineKeyboardButton(
            text=f"👍 {ratings['up']}",
            callback_data=f"up:{marker_hash}"
        ))
        inline_builder.add(InlineKeyboardButton(
            text=f"👎 {ratings['down']} ✓",
            callback_data=f"down:{marker_hash}"
        ))
    inline_builder.adjust(2)
    
    # Обновляем сообщение с кнопками
    try:
        await callback.message.edit_reply_markup(reply_markup=inline_builder.as_markup())
    except:
        pass  # Если не удалось обновить (например, сообщение уже изменено), просто игнорируем


# --------------------------------Команда для просмотра рейтинга--------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
@dp.message(Command("ratings"))
async def show_ratings(message: Message, bot: Bot):
    """Показывает рейтинг всех вопросов для администратора"""
    ratings = get_all_ratings()
    
    if not ratings:
        await bot.send_message(
            chat_id=message.chat.id,
            text="Пока нет оценок."
        )
        return
    
    # Загружаем структуру для получения текста вопросов по маркерам
    try:
        structure_f = pd.read_excel(os.path.abspath('structure.xlsx'), engine='openpyxl')
        marker_to_text = structure_f.set_index('Маркер')['Текст сообщения'].to_dict()
    except:
        marker_to_text = {}
    
    # Формируем сообщение с рейтингом
    text = "📊 <b>Рейтинг вопросов:</b>\n\n"
    
    # Сортируем по общему количеству оценок (вниз + вверх)
    sorted_ratings = sorted(
        ratings.items(),
        key=lambda x: x[1]['up'] + x[1]['down'],
        reverse=True
    )
    
    for marker, rating_data in sorted_ratings:
        up_count = rating_data['up']
        down_count = rating_data['down']
        total = up_count + down_count
        
        # Вычисляем процент положительных оценок
        if total > 0:
            positive_percent = round((up_count / total) * 100, 1)
        else:
            positive_percent = 0
        
        # Пытаемся получить текст вопроса, если есть в таблице
        question_display = marker_to_text.get(marker, marker)
        # Ограничиваем длину для читаемости
        if len(question_display) > 100:
            question_display = question_display[:100] + "..."
        
        text += f"<b>{question_display}</b>\n"
        text += f"Маркер: {marker}\n"
        text += f"👍 {up_count} | 👎 {down_count} | Всего: {total} | Положительных: {positive_percent}%\n\n"
    
    await bot.send_message(
        chat_id=message.chat.id,
        text=text,
        parse_mode=ParseMode.HTML
    )











# --------------------------------Обработчик ответов администраторов----------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
# Используем dp.message вместо router.message, чтобы этот обработчик работал независимо
@dp.message(F.reply_to_message)
async def handle_admin_reply(message: Message, bot: Bot):
    """Обработчик ответов администраторов на вопросы пользователей в группе"""
    # Проверяем, что сообщение пришло из группы администраторов
    try:
        group_id = int(TOKEN_GROUP) if TOKEN_GROUP.lstrip('-').isdigit() else None
        if group_id is not None:
            if message.chat.id != group_id:
                return
        else:
            # Если TOKEN_GROUP не число, сравниваем как строки
            if str(message.chat.id) != str(TOKEN_GROUP):
                return
    except Exception as e:
        # Если не удалось сравнить, пропускаем
        logging.debug(f"Ошибка проверки группы: {e}, chat_id={message.chat.id}, TOKEN_GROUP={TOKEN_GROUP}")
        return
    
    logging.info(f"Получено сообщение в группе администраторов от {message.from_user.id}, reply_to={message.reply_to_message.message_id if message.reply_to_message else None}")
    
    # Получаем ID сообщения, на которое отвечают
    replied_message_id = message.reply_to_message.message_id
    
    # Проверяем, есть ли ожидающий ответа вопрос с таким ID сообщения
    question_id, question_data = get_pending_question_by_message(replied_message_id)
    
    if question_id is None or question_data is None:
        # Это не ответ на ожидающий вопрос
        logging.debug(f"Не найден вопрос для сообщения {replied_message_id}")
        return
    
    logging.info(f"Найден вопрос {question_id} для ответа администратора, user_id={question_data.get('user_id')}")
    
    # Проверяем, не отвечен ли уже вопрос
    if question_data.get('answered', False):
        await bot.send_message(
            chat_id=message.chat.id,
            reply_to_message_id=message.message_id,
            text="⚠️ Этот вопрос уже был отвечен другим администратором."
        )
        return
    
    # Получаем данные вопроса
    user_id = question_data['user_id']
    # Убеждаемся, что user_id - это число
    if isinstance(user_id, str):
        try:
            user_id = int(user_id)
        except:
            pass
    
    question_text = question_data['question_text']
    admin_answer = message.text or message.caption or "Ответ администратора"
    admin_username = message.from_user.username or message.from_user.first_name or "Администратор"
    
    # Отмечаем вопрос как отвеченный
    mark_question_answered(question_id)
    
    # Отправляем ответ пользователю
    try:
        answer_text = f"Ответ от администратора на ваш вопрос:\n\n<i>{question_text}</i>\n\n<b>Ответ:</b>\n{admin_answer}"
        logging.info(f"Попытка отправить ответ пользователю {user_id} (тип: {type(user_id)})")
        await bot.send_message(
            chat_id=int(user_id),
            text=answer_text,
            parse_mode=ParseMode.HTML
        )
        logging.info(f"Ответ успешно отправлен пользователю {user_id}")
        
        # Уведомляем в группе, что ответ отправлен
        await bot.send_message(
            chat_id=message.chat.id,
            reply_to_message_id=message.message_id,
            text=f"✅ Ответ отправлен пользователю (ID: {user_id})"
        )
        
        # Обновляем исходное сообщение с вопросом, чтобы показать, что он отвечен
        try:
            original_text = message.reply_to_message.text or message.reply_to_message.caption or ""
            updated_text = f"{original_text}\n\n✅ <b>Отвечено администратором @{admin_username}</b>"
            await bot.edit_message_text(
                chat_id=message.chat.id,
                message_id=replied_message_id,
                text=updated_text,
                parse_mode=ParseMode.HTML
            )
        except Exception as edit_error:
            # Если не удалось отредактировать сообщение, просто игнорируем
            logging.warning(f"Не удалось отредактировать сообщение: {edit_error}")
            pass
            
    except Exception as e:
        # Если не удалось отправить сообщение пользователю (например, он заблокировал бота)
        error_msg = f"❌ Ошибка при отправке ответа пользователю (ID: {user_id}): {str(e)}"
        logging.error(error_msg)
        await bot.send_message(
            chat_id=message.chat.id,
            reply_to_message_id=message.message_id,
            text=error_msg
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(main())
