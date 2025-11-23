import asyncio
import logging
import os
from datetime import datetime

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import (
    Message, 
    CallbackQuery, 
    InlineKeyboardMarkup, 
    InlineKeyboardButton,
    BusinessConnection,
    BusinessMessagesDeleted
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

from config import BOT_TOKEN
from database import db
from whisper_client import transcribe_audio
from ai_client import analyze_messages, custom_analysis

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Инициализация бота
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)


# FSM состояния
class AnalysisStates(StatesGroup):
    waiting_for_custom_query = State()


# === Клавиатуры ===

def get_main_menu_keyboard():
    """Главное меню"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Личные чаты", callback_data="list_private")],
        [InlineKeyboardButton(text="👥 Группы", callback_data="list_groups")],
        [InlineKeyboardButton(text="📢 Каналы", callback_data="list_channels")],
        [InlineKeyboardButton(text="⚙️ Настройки", callback_data="settings")],
    ])
    return keyboard


def get_chat_actions_keyboard(chat_id: int):
    """Меню действий для чата"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Резюме", callback_data=f"analyze_summary_{chat_id}")],
        [InlineKeyboardButton(text="💡 Инсайты", callback_data=f"analyze_insights_{chat_id}")],
        [InlineKeyboardButton(text="📝 Темы", callback_data=f"analyze_topics_{chat_id}")],
        [InlineKeyboardButton(text="😊 Тональность", callback_data=f"analyze_sentiment_{chat_id}")],
        [InlineKeyboardButton(text="❓ Свой вопрос", callback_data=f"analyze_custom_{chat_id}")],
        [InlineKeyboardButton(text="⚙️ Настройки чата", callback_data=f"chat_settings_{chat_id}")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_menu")],
    ])
    return keyboard


def get_chat_settings_keyboard(chat_id: int, transcription_enabled: bool):
    """Меню настроек чата"""
    transcription_text = "✅ Транскрибация включена" if transcription_enabled else "❌ Транскрибация выключена"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=transcription_text, callback_data=f"toggle_transcription_{chat_id}")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data=f"chat_actions_{chat_id}")],
    ])
    return keyboard


def get_back_keyboard():
    """Кнопка назад"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_menu")],
    ])
    return keyboard


# === Обработчики команд ===

@dp.message(Command("start"))
async def cmd_start(message: Message):
    """Обработка команды /start"""
    user = message.from_user
    
    # Сохраняем пользователя в БД
    await db.add_user(
        user_id=user.id,
        username=user.username,
        first_name=user.first_name,
        last_name=user.last_name
    )
    
    welcome_text = (
        f"👋 Привет, {user.first_name}!\n\n"
        "Я бот для анализа твоих переписок в Telegram.\n\n"
        "🎯 Что я умею:\n"
        "• Автоматически транскрибирую голосовые сообщения\n"
        "• Анализирую переписки с помощью AI\n"
        "• Создаю резюме и выделяю ключевые темы\n"
        "• Отвечаю на твои вопросы о переписках\n\n"
        "📱 Выбери категорию чатов для анализа:"
    )
    
    await message.answer(welcome_text, reply_markup=get_main_menu_keyboard())


@dp.message(Command("menu"))
async def cmd_menu(message: Message):
    """Показать главное меню"""
    await message.answer("📱 Главное меню:", reply_markup=get_main_menu_keyboard())


# === Обработчики голосовых сообщений ===

@dp.message(F.voice)
async def handle_voice(message: Message):
    """Обработка голосовых сообщений"""
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    # Проверяем авторизацию
    if not await db.is_user_authorized(user_id):
        await message.answer("❌ Вы не авторизованы. Используйте /start")
        return
    
    # Проверяем, включена ли транскрибация для этого чата
    if not await db.is_transcription_enabled(chat_id):
        # Сохраняем сообщение без транскрибации
        await db.add_message(
            chat_id=chat_id,
            user_id=user_id,
            message_date=datetime.fromtimestamp(message.date.timestamp()),
            is_voice=True
        )
        return
    
    # Сохраняем информацию о чате
    await db.add_chat(
        chat_id=chat_id,
        chat_type=message.chat.type,
        title=message.chat.title or message.chat.first_name
    )
    
    try:
        # Скачиваем голосовое сообщение
        voice_file = await bot.get_file(message.voice.file_id)
        voice_path = f"/tmp/voice_{message.voice.file_id}.ogg"
        await bot.download_file(voice_file.file_path, voice_path)
        
        # Отправляем уведомление о начале транскрибации
        status_msg = await message.answer("🎤 Транскрибирую голосовое сообщение...")
        
        # Транскрибируем
        transcription = await transcribe_audio(voice_path)
        
        # Удаляем временный файл
        if os.path.exists(voice_path):
            os.remove(voice_path)
        
        # Сохраняем в БД
        await db.add_message(
            chat_id=chat_id,
            user_id=user_id,
            message_date=datetime.fromtimestamp(message.date.timestamp()),
            is_voice=True,
            transcription=transcription
        )
        
        # Отправляем результат
        await status_msg.edit_text(f"📝 Транскрибация:\n\n{transcription}")
        
    except Exception as e:
        logger.error(f"Error processing voice message: {e}")
        await message.answer(f"❌ Ошибка при обработке голосового сообщения: {str(e)}")


# === Обработчики текстовых сообщений ===

@dp.message(F.text)
async def handle_text(message: Message, state: FSMContext):
    """Обработка текстовых сообщений"""
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    # Проверяем, ожидаем ли мы кастомный запрос
    current_state = await state.get_state()
    if current_state == AnalysisStates.waiting_for_custom_query.state:
        data = await state.get_data()
        target_chat_id = data.get("chat_id")
        
        # Получаем сообщения для анализа
        messages = await db.get_chat_messages(target_chat_id, limit=300)
        
        if not messages:
            await message.answer("❌ Нет сообщений для анализа")
            await state.clear()
            return
        
        # Отправляем уведомление
        status_msg = await message.answer("🤔 Анализирую переписку...")
        
        # Выполняем анализ
        result = await custom_analysis(messages, message.text)
        
        # Сохраняем результат
        await db.add_analysis_result(
            chat_id=target_chat_id,
            user_id=user_id,
            analysis_type="custom",
            result_text=result
        )
        
        # Отправляем результат
        await status_msg.edit_text(f"💡 Результат анализа:\n\n{result}")
        await message.answer("Выберите действие:", reply_markup=get_chat_actions_keyboard(target_chat_id))
        
        await state.clear()
        return
    
    # Проверяем авторизацию
    if not await db.is_user_authorized(user_id):
        return
    
    # Сохраняем обычное текстовое сообщение
    await db.add_chat(
        chat_id=chat_id,
        chat_type=message.chat.type,
        title=message.chat.title or message.chat.first_name
    )
    
    await db.add_message(
        chat_id=chat_id,
        user_id=user_id,
        message_text=message.text,
        message_date=datetime.fromtimestamp(message.date.timestamp())
    )


# === Обработчики callback-запросов ===

@dp.callback_query(F.data == "back_to_menu")
async def callback_back_to_menu(callback: CallbackQuery, state: FSMContext):
    """Возврат в главное меню"""
    await state.clear()
    await callback.message.edit_text("📱 Главное меню:", reply_markup=get_main_menu_keyboard())
    await callback.answer()


@dp.callback_query(F.data == "list_private")
async def callback_list_private(callback: CallbackQuery):
    """Список личных чатов"""
    await callback.message.edit_text(
        "👤 Личные чаты:\n\n"
        "Эта функция пока в разработке. Скоро здесь будет список ваших личных чатов.",
        reply_markup=get_back_keyboard()
    )
    await callback.answer()


@dp.callback_query(F.data == "list_groups")
async def callback_list_groups(callback: CallbackQuery):
    """Список групп"""
    await callback.message.edit_text(
        "👥 Группы:\n\n"
        "Эта функция пока в разработке. Скоро здесь будет список ваших групп.",
        reply_markup=get_back_keyboard()
    )
    await callback.answer()


@dp.callback_query(F.data == "list_channels")
async def callback_list_channels(callback: CallbackQuery):
    """Список каналов"""
    await callback.message.edit_text(
        "📢 Каналы:\n\n"
        "Эта функция пока в разработке. Скоро здесь будет список ваших каналов.",
        reply_markup=get_back_keyboard()
    )
    await callback.answer()


@dp.callback_query(F.data == "settings")
async def callback_settings(callback: CallbackQuery):
    """Настройки"""
    await callback.message.edit_text(
        "⚙️ Настройки:\n\n"
        "Эта функция пока в разработке. Скоро здесь будут глобальные настройки бота.",
        reply_markup=get_back_keyboard()
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("chat_actions_"))
async def callback_chat_actions(callback: CallbackQuery):
    """Действия для чата"""
    chat_id = int(callback.data.split("_")[2])
    chat = await db.get_chat(chat_id)
    
    if not chat:
        await callback.answer("❌ Чат не найден", show_alert=True)
        return
    
    await callback.message.edit_text(
        f"💬 Чат: {chat.get('title', 'Без названия')}\n\n"
        "Выберите действие:",
        reply_markup=get_chat_actions_keyboard(chat_id)
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("chat_settings_"))
async def callback_chat_settings(callback: CallbackQuery):
    """Настройки чата"""
    chat_id = int(callback.data.split("_")[2])
    chat = await db.get_chat(chat_id)
    
    if not chat:
        await callback.answer("❌ Чат не найден", show_alert=True)
        return
    
    transcription_enabled = chat.get("transcription_enabled", True)
    
    await callback.message.edit_text(
        f"⚙️ Настройки чата: {chat.get('title', 'Без названия')}\n\n"
        "Управление транскрибацией голосовых сообщений:",
        reply_markup=get_chat_settings_keyboard(chat_id, transcription_enabled)
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("toggle_transcription_"))
async def callback_toggle_transcription(callback: CallbackQuery):
    """Переключение транскрибации"""
    chat_id = int(callback.data.split("_")[2])
    chat = await db.get_chat(chat_id)
    
    if not chat:
        await callback.answer("❌ Чат не найден", show_alert=True)
        return
    
    # Переключаем состояние
    current_state = chat.get("transcription_enabled", True)
    new_state = not current_state
    await db.set_transcription_enabled(chat_id, new_state)
    
    # Обновляем клавиатуру
    await callback.message.edit_reply_markup(
        reply_markup=get_chat_settings_keyboard(chat_id, new_state)
    )
    
    status = "включена" if new_state else "выключена"
    await callback.answer(f"✅ Транскрибация {status}")


@dp.callback_query(F.data.startswith("analyze_"))
async def callback_analyze(callback: CallbackQuery, state: FSMContext):
    """Анализ переписки"""
    parts = callback.data.split("_")
    analysis_type = parts[1]
    chat_id = int(parts[2])
    
    user_id = callback.from_user.id
    
    # Для кастомного запроса переходим в состояние ожидания
    if analysis_type == "custom":
        await state.set_state(AnalysisStates.waiting_for_custom_query)
        await state.update_data(chat_id=chat_id)
        await callback.message.answer("❓ Задайте свой вопрос о переписке:")
        await callback.answer()
        return
    
    # Получаем сообщения для анализа
    messages = await db.get_chat_messages(chat_id, limit=300)
    
    if not messages:
        await callback.answer("❌ Нет сообщений для анализа", show_alert=True)
        return
    
    # Отправляем уведомление
    await callback.message.edit_text("🤔 Анализирую переписку...")
    
    # Выполняем анализ
    result = await analyze_messages(messages, analysis_type)
    
    # Сохраняем результат
    await db.add_analysis_result(
        chat_id=chat_id,
        user_id=user_id,
        analysis_type=analysis_type,
        result_text=result
    )
    
    # Определяем заголовок
    titles = {
        "summary": "📊 Резюме переписки",
        "insights": "💡 Ключевые инсайты",
        "topics": "📝 Основные темы",
        "sentiment": "😊 Анализ тональности"
    }
    title = titles.get(analysis_type, "📊 Результат анализа")
    
    # Отправляем результат
    await callback.message.edit_text(f"{title}:\n\n{result}")
    await callback.message.answer("Выберите действие:", reply_markup=get_chat_actions_keyboard(chat_id))
    await callback.answer()


# === Telegram Business обработчики ===

@dp.business_connection()
async def handle_business_connection(business_connection: BusinessConnection):
    """Обработка подключения Telegram Business"""
    logger.info(f"Business connection: {business_connection.id} from user {business_connection.user.id}")
    
    if business_connection.is_enabled:
        # Сохраняем подключение
        await db.add_business_connection(
            connection_id=business_connection.id,
            user_id=business_connection.user.id,
            user_chat_id=business_connection.user_chat_id
        )
        logger.info(f"Business connection {business_connection.id} activated")
    else:
        # Удаляем подключение
        await db.remove_business_connection(business_connection.id)
        logger.info(f"Business connection {business_connection.id} deactivated")


@dp.business_message(F.voice)
async def handle_business_voice(message: Message):
    """Обработка голосовых сообщений из Telegram Business"""
    if not message.business_connection_id:
        return
    
    # Проверяем, есть ли активное подключение
    connection = await db.get_business_connection(message.business_connection_id)
    if not connection:
        logger.warning(f"Business connection {message.business_connection_id} not found")
        return
    
    chat_id = message.chat.id
    user_id = message.from_user.id if message.from_user else 0
    
    # Проверяем, включена ли транскрибация
    if not await db.is_transcription_enabled(chat_id):
        await db.add_message(
            chat_id=chat_id,
            user_id=user_id,
            message_date=datetime.fromtimestamp(message.date.timestamp()),
            is_voice=True
        )
        return
    
    # Сохраняем информацию о чате
    await db.add_chat(
        chat_id=chat_id,
        chat_type="business",
        title=message.chat.title or message.chat.first_name or "Business Chat"
    )
    
    try:
        # Скачиваем голосовое сообщение
        voice_file = await bot.get_file(message.voice.file_id)
        voice_path = f"/tmp/voice_{message.voice.file_id}.ogg"
        await bot.download_file(voice_file.file_path, voice_path)
        
        logger.info(f"Transcribing business voice message from chat {chat_id}")
        
        # Транскрибируем
        transcription = await transcribe_audio(voice_path)
        
        # Удаляем временный файл
        if os.path.exists(voice_path):
            os.remove(voice_path)
        
        # Сохраняем в БД
        await db.add_message(
            chat_id=chat_id,
            user_id=user_id,
            message_date=datetime.fromtimestamp(message.date.timestamp()),
            is_voice=True,
            transcription=transcription
        )
        
        # Отправляем результат в business чат
        await bot.send_message(
            chat_id=chat_id,
            text=f"📝 Транскрибация:\n\n{transcription}",
            business_connection_id=message.business_connection_id,
            reply_to_message_id=message.message_id
        )
        
        logger.info(f"Business voice transcription completed for chat {chat_id}")
        
    except Exception as e:
        logger.error(f"Error processing business voice message: {e}")
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=f"❌ Ошибка при обработке голосового сообщения: {str(e)}",
                business_connection_id=message.business_connection_id,
                reply_to_message_id=message.message_id
            )
        except:
            pass


@dp.business_message(F.text)
async def handle_business_text(message: Message):
    """Обработка текстовых сообщений из Telegram Business"""
    if not message.business_connection_id:
        return
    
    connection = await db.get_business_connection(message.business_connection_id)
    if not connection:
        return
    
    chat_id = message.chat.id
    user_id = message.from_user.id if message.from_user else 0
    
    # Сохраняем чат и сообщение
    await db.add_chat(
        chat_id=chat_id,
        chat_type="business",
        title=message.chat.title or message.chat.first_name or "Business Chat"
    )
    
    await db.add_message(
        chat_id=chat_id,
        user_id=user_id,
        message_text=message.text,
        message_date=datetime.fromtimestamp(message.date.timestamp())
    )


# === Главная функция ===

async def main():
    """Запуск бота"""
    logger.info("Starting bot...")
    
    # Подключаемся к базе данных
    await db.connect()
    logger.info("Database connected")
    
    try:
        # Запускаем polling
        await dp.start_polling(bot)
    finally:
        # Закрываем соединения
        await db.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
