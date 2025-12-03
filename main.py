# Импорт библиотек
import pandas as pd
import sys
import logging
import asyncio
import os
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
from aiogram.dispatcher.event.bases import SkipHandler

from services.dictionaries import (
    update_dictionaries,
    read_table,
    get_key_buttons_1rang,
    get_key_buttons_text,
    get_key_buttons_termins,
    get_key_all_opros,
)
from services.pending_questions import (
    add_pending_question,
    get_pending_question_by_message,
    mark_question_answered,
)
from services.ratings import (
    add_rating,
    get_ratings,
    has_user_rated,
    get_user_rating,
    get_marker_from_hash,
    store_marker_hash,
    get_all_ratings,
)
from services.user_stats import record_user_activity_from_message
from services.tildaforms import (
    parse_tildaforms_message,
    create_html_from_tildaforms_data,
    save_html_temp,
)

load_dotenv()

TOKEN = os.getenv("TOKEN")
TOKEN_GROUP = os.getenv("TOKEN_GROUP")
TILDAFORMS_GROUP = os.getenv("TILDAFORMS_GROUP")

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

update_dictionaries()


user_context={}

# --------------------------------Система ответов администраторов--------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------

# --------------------------------Система оценок------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------

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
            pd.read_excel(destination, engine='openpyxl')

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
    if message.chat.type != "private":
        await message.reply(
            "Пожалуйста, напишите боту в личные сообщения, чтобы начать работу.",
            reply_markup=ReplyKeyboardRemove()
        )
        return
    record_user_activity_from_message(message)
    level, marker, text_messege, buttns = await read_table(1)
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
    level, marker, text_messege, buttns = await read_table(1)
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





@router.message(lambda message: message.chat.type == "private" and message.text not in get_key_buttons_text()) # Второй уровень - работа с текстом
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
        level, marker, expected_text, buttns = await read_table(3)

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
            level, marker, text_messege, buttns = await read_table(3)
            await bot.send_message(
                chat_id=message.chat.id,
                text=buttns, 
                parse_mode=ParseMode.HTML
            )

        # Очищаем контекст
        del user_context[user_id]
        return


@router.message(lambda message: message.chat.type == "private" and message.text in get_key_buttons_1rang()) # Второй уровень
async def fn_1(message: Message, bot: Bot):
    record_user_activity_from_message(message)
    buttons_map = get_key_buttons_1rang()
    internal_command = buttons_map.get(message.text)
    # print(message.text)

    level, marker, text_messege, buttns = await read_table(1)
    buttns_list = [btn.strip() for btn in buttns.strip('[]').split(']\n[')]

    if internal_command == 2 and message.text != 'Задать свой вопрос о ЦФА':
        level, marker, text_messege, buttns = await read_table(2, message.text)
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
        level, marker, text_messege, buttns = await read_table(3)

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


@router.message(lambda message: message.chat.type == "private" and message.text in get_key_buttons_termins()) # Третий уровень ответы
async def fn_2(message: Message, bot: Bot):
    record_user_activity_from_message(message)
    # print('key_buttons_termins = ', message.text)
    level, marker, text_messege, buttns = await read_table(4, message.text)
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


@router.message(lambda message: message.chat.type == "private" and message.text not in get_key_all_opros()) # Третий уровень опрос
async def fn_3(message: Message, bot: Bot):
    record_user_activity_from_message(message)
    # print('key_all_opros = ', message.text)
    level, marker, text_messege, buttns = await read_table(5, message.text)

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











# --------------------------------Обработчик сообщений от TildaForms----------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
# Добавляем простой обработчик для отладки - логирует ВСЕ сообщения из групп
@dp.message(lambda m: m.chat.type in ("supergroup", "group"))
async def debug_all_group_messages(message: Message, bot: Bot):
    """Отладочный обработчик - логирует все сообщения из групп для диагностики"""
    logging.info(
        f"[DEBUG] Получено сообщение в группе: chat_id={message.chat.id}, "
        f"chat_type={message.chat.type}, message_id={message.message_id}, "
        f"has_text={bool(message.text)}, has_caption={bool(message.caption)}, "
        f"from_user_id={message.from_user.id if message.from_user else None}, "
        f"from_username={message.from_user.username if message.from_user else None}"
    )
    # Продолжаем обработку следующими хендлерами
    raise SkipHandler

@dp.message(lambda m: m.chat.type in ("supergroup", "group") and not m.reply_to_message)
async def handle_tildaforms_message(message: Message, bot: Bot):
    """Обработчик сообщений от TildaForms бота в группе"""
    logging.info(f"[TildaForms] Получено сообщение в группе: chat_id={message.chat.id}, chat_type={message.chat.type}, message_id={message.message_id}")
    
    # Проверяем, что сообщение пришло из группы TildaForms
    try:
        # Используем TILDAFORMS_GROUP, если задан, иначе используем TOKEN_GROUP
        target_group = TILDAFORMS_GROUP or TOKEN_GROUP
        if not target_group:
            logging.error(f"[TildaForms] Не указана группа для TildaForms! Установите TILDAFORMS_GROUP или TOKEN_GROUP в .env")
            return
        
        group_id = int(target_group) if target_group.lstrip('-').isdigit() else None
        if group_id is not None:
            if message.chat.id != group_id:
                logging.info(f"[TildaForms] Сообщение не из нужной группы: chat_id={message.chat.id}, нужна группа {group_id}")
                return
        else:
            # Если группа не число, сравниваем как строки
            if str(message.chat.id) != str(target_group):
                logging.info(f"[TildaForms] Сообщение не из нужной группы (строки): chat_id={message.chat.id}, нужна группа {target_group}")
                return
    except Exception as e:
        logging.error(f"[TildaForms] Ошибка проверки группы: {e}, chat_id={message.chat.id}, TILDAFORMS_GROUP={TILDAFORMS_GROUP}, TOKEN_GROUP={TOKEN_GROUP}")
        return
    
    # Проверяем, что есть текст сообщения (может быть в text или caption)
    text = message.text or message.caption or ""
    
    # Логируем всю информацию о сообщении для отладки
    sender_info = {}
    sender_chat_info = {}
    
    if message.from_user:
        sender_info = {
            'id': message.from_user.id,
            'username': message.from_user.username,
            'first_name': message.from_user.first_name,
            'is_bot': getattr(message.from_user, 'is_bot', False)
        }
    
    if message.sender_chat:
        sender_chat_info = {
            'id': message.sender_chat.id,
            'title': message.sender_chat.title,
            'username': message.sender_chat.username,
            'type': message.sender_chat.type
        }
    
    logging.info(f"[TildaForms] Детали сообщения: message_id={message.message_id}, "
                 f"has_text={bool(message.text)}, has_caption={bool(message.caption)}, "
                 f"text_length={len(text)}, from_user={sender_info}, sender_chat={sender_chat_info}")
    
    # Если нет текста, но есть документ или фото, возможно текст в caption
    # Также проверяем, может быть это сообщение от TildaForms бота по username
    is_tildaforms_by_username = False
    if message.from_user:
        username_lower = (message.from_user.username or "").lower()
        is_tildaforms_by_username = "tildaforms" in username_lower or "tildaformsbot" in username_lower
    
    if message.sender_chat:
        chat_username_lower = (message.sender_chat.username or "").lower()
        if not is_tildaforms_by_username:
            is_tildaforms_by_username = "tildaforms" in chat_username_lower
    
    if not text:
        if is_tildaforms_by_username:
            logging.info(f"[TildaForms] Сообщение от TildaForms бота без текста, но это может быть документ/фото. "
                        f"has_document={bool(message.document)}, has_photo={bool(message.photo)}")
            # Если это точно TildaForms бот, но нет текста - возможно это служебное сообщение, пропускаем
            return
        else:
            logging.info(f"[TildaForms] Сообщение без текста и не от TildaForms бота, пропускаем")
            return
    
    # Проверяем, что сообщение от TildaForms бота
    # Может быть через from_user или sender_chat
    sender_username = ""
    sender_first_name = ""
    sender_id = None
    is_bot = False
    
    # Проверяем from_user
    if message.from_user:
        sender_username = (message.from_user.username or "").lower()
        sender_first_name = (message.from_user.first_name or "").lower()
        sender_id = message.from_user.id
        is_bot = getattr(message.from_user, 'is_bot', False)
    
    # Если сообщение от канала/бота через sender_chat, проверяем его
    if message.sender_chat and not sender_username:
        sender_username = (message.sender_chat.username or "").lower()
        sender_first_name = (message.sender_chat.title or "").lower()
        sender_id = message.sender_chat.id
    
    logging.info(f"[TildaForms] Проверка сообщения: sender_id={sender_id}, username={sender_username}, "
                 f"first_name={sender_first_name}, is_bot={is_bot}, text_preview={text[:200]}")
    
    # Проверяем формат сообщения TildaForms (должно содержать tg_user_id и другие поля)
    text_lower = text.lower() if text else ""
    
    # Проверяем по username бота (TildaFormsBot обычно имеет username "tildaformsbot")
    is_tildaforms_bot = (
        "tildaforms" in sender_username or
        "tildaforms" in sender_first_name or
        "tildaformsbot" in sender_username
    )
    
    # Проверяем по содержимому сообщения (только если есть текст)
    has_tg_user_id = "tg_user_id" in text_lower if text else False
    has_form_format = (":" in text and "\n" in text and len(text.split("\n")) >= 3) if text else False
    
    # Если это точно TildaForms бот по username, считаем что это сообщение от него
    # Или если в тексте есть признаки формы
    is_tildaforms = is_tildaforms_bot or has_tg_user_id or has_form_format
    
    logging.info(f"[TildaForms] Детальная проверка: is_tildaforms_bot={is_tildaforms_bot}, "
                 f"has_tg_user_id={has_tg_user_id}, has_form_format={has_form_format}, "
                 f"is_tildaforms={is_tildaforms}, text_exists={bool(text)}")
    
    logging.info(f"[TildaForms] Результат проверки на TildaForms: {is_tildaforms}")
    
    if not is_tildaforms:
        # Проверяем формат формы (выносим в переменную, чтобы избежать проблемы с обратным слешем в f-string)
        has_format = (":" in text and "\n" in text and len(text.split("\n")) >= 3) if text else False
        logging.info(f"[TildaForms] Сообщение не распознано как TildaForms: tg_user_id в тексте={'tg_user_id' in text_lower if text else False}, "
                     f"tildaforms в username={'tildaforms' in sender_username}, "
                     f"формат формы={has_format}")
        if text:
            logging.info(f"[TildaForms] Полный текст сообщения: {text}")
        else:
            logging.info(f"[TildaForms] Текст сообщения отсутствует. Возможно, это служебное сообщение или сообщение в другом формате.")
        return
    
    logging.info(f"[TildaForms] ✓ Получено сообщение от TildaForms: message_id={message.message_id} в группе {message.chat.id}")
    
    try:
        # Парсим данные из сообщения
        data = parse_tildaforms_message(text)
        
        # Проверяем, что есть данные пользователя
        if 'tg_user_id' not in data:
            logging.warning(f"В сообщении TildaForms нет tg_user_id: {text[:100]}")
            return
        
        user_id = data.get('tg_user_id')
        try:
            user_id = int(user_id)
        except (ValueError, TypeError):
            logging.error(f"Некорректный user_id: {user_id}")
            return
        
        # Создаем HTML-страницу
        html_content = create_html_from_tildaforms_data(data)
        html_file_path = save_html_temp(html_content)
        
        try:
            # Отправляем HTML-файл пользователю
            await bot.send_document(
                chat_id=user_id,
                document=FSInputFile(html_file_path, filename='Заявка_на_выпуск_ЦФА.html'),
                caption="🎉 <b>Ваша заявка на выпуск ЦФА принята!</b>\n\nСпасибо за обращение. Ваша заявка была успешно обработана.",
                parse_mode=ParseMode.HTML
            )
            
            logging.info(f"HTML-файл успешно отправлен пользователю {user_id}")
            
            # Уведомляем в группе, что заявка обработана
            username = data.get('tg_username', 'Не указан')
            first_name = data.get('tg_first_name', 'Пользователь')
            await bot.send_message(
                chat_id=message.chat.id,
                reply_to_message_id=message.message_id,
                text=f"✅ Заявка обработана! HTML-файл отправлен пользователю @{username} ({first_name}, ID: {user_id})"
            )
        except Exception as send_error:
            error_msg = f"Ошибка при отправке HTML пользователю (ID: {user_id}): {str(send_error)}"
            logging.error(error_msg)
            await bot.send_message(
                chat_id=message.chat.id,
                reply_to_message_id=message.message_id,
                text=f"❌ {error_msg}"
            )
        finally:
            # Удаляем временный файл
            try:
                if os.path.exists(html_file_path):
                    os.remove(html_file_path)
            except Exception as del_error:
                logging.warning(f"Не удалось удалить временный файл {html_file_path}: {del_error}")
        
    except Exception as e:
        error_msg = f"Ошибка при обработке сообщения от TildaForms: {str(e)}"
        logging.error(error_msg)
        await bot.send_message(
            chat_id=message.chat.id,
            reply_to_message_id=message.message_id,
            text=f"❌ {error_msg}"
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
