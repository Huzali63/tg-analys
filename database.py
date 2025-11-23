import asyncpg
from typing import Optional, List, Dict, Any
from config import DATABASE_URL


class Database:
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        """Создание пула подключений к базе данных"""
        self.pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
        await self.create_tables()

    async def close(self):
        """Закрытие пула подключений"""
        if self.pool:
            await self.pool.close()

    async def create_tables(self):
        """Создание таблиц в базе данных"""
        async with self.pool.acquire() as conn:
            # Таблица пользователей
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    username VARCHAR(255),
                    first_name VARCHAR(255),
                    last_name VARCHAR(255),
                    is_authorized BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Таблица чатов
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS chats (
                    chat_id BIGINT PRIMARY KEY,
                    chat_type VARCHAR(50),
                    title VARCHAR(255),
                    transcription_enabled BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Таблица сообщений
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    message_id BIGSERIAL PRIMARY KEY,
                    chat_id BIGINT,
                    user_id BIGINT,
                    message_text TEXT,
                    message_date TIMESTAMP,
                    is_voice BOOLEAN DEFAULT FALSE,
                    transcription TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Таблица business подключений
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS business_connections (
                    connection_id VARCHAR(255) PRIMARY KEY,
                    user_id BIGINT,
                    user_chat_id BIGINT,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Таблица результатов анализа
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS analysis_results (
                    id BIGSERIAL PRIMARY KEY,
                    chat_id BIGINT,
                    user_id BIGINT,
                    analysis_type VARCHAR(100),
                    result_text TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

    # === User Operations ===
    
    async def add_user(self, user_id: int, username: str = None, first_name: str = None, last_name: str = None):
        """Добавление или обновление пользователя"""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO users (user_id, username, first_name, last_name, is_authorized)
                VALUES ($1, $2, $3, $4, TRUE)
                ON CONFLICT (user_id) DO UPDATE
                SET username = $2, first_name = $3, last_name = $4, is_authorized = TRUE
            """, user_id, username, first_name, last_name)

    async def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Получение информации о пользователе"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
            return dict(row) if row else None

    async def is_user_authorized(self, user_id: int) -> bool:
        """Проверка авторизации пользователя"""
        user = await self.get_user(user_id)
        return user and user.get("is_authorized", False)

    # === Chat Operations ===
    
    async def add_chat(self, chat_id: int, chat_type: str, title: str = None):
        """Добавление или обновление чата"""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO chats (chat_id, chat_type, title)
                VALUES ($1, $2, $3)
                ON CONFLICT (chat_id) DO UPDATE
                SET chat_type = $2, title = $3
            """, chat_id, chat_type, title)

    async def get_chat(self, chat_id: int) -> Optional[Dict[str, Any]]:
        """Получение информации о чате"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM chats WHERE chat_id = $1", chat_id)
            return dict(row) if row else None

    async def set_transcription_enabled(self, chat_id: int, enabled: bool):
        """Включение/выключение транскрибации для чата"""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                UPDATE chats SET transcription_enabled = $1 WHERE chat_id = $2
            """, enabled, chat_id)

    async def is_transcription_enabled(self, chat_id: int) -> bool:
        """Проверка, включена ли транскрибация для чата"""
        chat = await self.get_chat(chat_id)
        if not chat:
            # По умолчанию для личных чатов транскрибация включена
            return True
        return chat.get("transcription_enabled", True)

    # === Message Operations ===
    
    async def add_message(
        self, 
        chat_id: int, 
        user_id: int, 
        message_text: str = None, 
        message_date = None,
        is_voice: bool = False,
        transcription: str = None
    ):
        """Добавление сообщения"""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO messages (chat_id, user_id, message_text, message_date, is_voice, transcription)
                VALUES ($1, $2, $3, $4, $5, $6)
            """, chat_id, user_id, message_text, message_date, is_voice, transcription)

    async def get_chat_messages(self, chat_id: int, limit: int = 300) -> List[Dict[str, Any]]:
        """Получение последних сообщений из чата"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM messages 
                WHERE chat_id = $1 
                ORDER BY message_date DESC 
                LIMIT $2
            """, chat_id, limit)
            return [dict(row) for row in rows]

    # === Analysis Operations ===
    
    async def add_analysis_result(
        self, 
        chat_id: int, 
        user_id: int, 
        analysis_type: str, 
        result_text: str
    ):
        """Сохранение результата анализа"""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO analysis_results (chat_id, user_id, analysis_type, result_text)
                VALUES ($1, $2, $3, $4)
            """, chat_id, user_id, analysis_type, result_text)

    async def get_analysis_results(self, chat_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        """Получение результатов анализа для чата"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM analysis_results 
                WHERE chat_id = $1 
                ORDER BY created_at DESC 
                LIMIT $2
            """, chat_id, limit)
            return [dict(row) for row in rows]

    # === Business Connection Operations ===
    
    async def add_business_connection(
        self,
        connection_id: str,
        user_id: int,
        user_chat_id: int
    ):
        """Добавление или обновление business подключения"""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO business_connections (connection_id, user_id, user_chat_id, is_active)
                VALUES ($1, $2, $3, TRUE)
                ON CONFLICT (connection_id) DO UPDATE
                SET user_id = $2, user_chat_id = $3, is_active = TRUE
            """, connection_id, user_id, user_chat_id)

    async def remove_business_connection(self, connection_id: str):
        """Удаление business подключения"""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                UPDATE business_connections SET is_active = FALSE WHERE connection_id = $1
            """, connection_id)

    async def get_business_connection(self, connection_id: str) -> Optional[Dict[str, Any]]:
        """Получение информации о business подключении"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT * FROM business_connections WHERE connection_id = $1 AND is_active = TRUE
            """, connection_id)
            return dict(row) if row else None


# Глобальный экземпляр базы данных
db = Database()
