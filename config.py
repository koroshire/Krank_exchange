import os
from dotenv import load_dotenv

# Загружаем переменные из .env файла
load_dotenv()

# Получаем токен и ID менеджера
BOT_TOKEN = os.getenv("BOT_TOKEN")
MANAGER_ID = int(os.getenv("MANAGER_ID", "0"))

# Проверяем, что токен загружен
if not BOT_TOKEN:
    raise ValueError("❌ Нет токена бота! Создай файл .env и добавь BOT_TOKEN=твой_токен")

if MANAGER_ID == 0:
    raise ValueError("❌ Нет ID менеджера! Добавь MANAGER_ID=твой_telegram_id в .env файл")

print("✅ Токен и ID менеджера успешно загружены из .env")