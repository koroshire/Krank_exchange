# ==================== ИМПОРТЫ ====================
import asyncio
import logging
import re
import sys
import time
from datetime import datetime
from typing import Dict, Optional, List, Any, Tuple
from dataclasses import dataclass, field
from collections import defaultdict

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton, 
    InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery
)
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
import aiohttp
import os
from dotenv import load_dotenv
import base58


# ==================== НАСТРОЙКА ЛОГИРОВАНИЯ ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('bot.log'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


# ==================== ЗАГРУЗКА КОНФИГУРАЦИИ ====================
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID_STR = os.getenv("ADMIN_ID")
if not BOT_TOKEN:
    logger.error("❌ BOT_TOKEN не найден")
    sys.exit(1)
if not ADMIN_ID_STR:
    logger.error("❌ ADMIN_ID не найден в .env файле!")
    sys.exit(1)
ADMIN_IDS = []
for aid in ADMIN_ID_STR.split(','):
    try:
        ADMIN_IDS.append(int(aid.strip()))
    except ValueError:
        logger.error(f"❌ ADMIN_ID '{aid}' должен быть числом")
        sys.exit(1)
if not ADMIN_IDS:
    logger.error("❌ Нет ни одного ADMIN_ID")
    sys.exit(1)
ADMIN_ID = ADMIN_IDS[0]
logger.info(f"✅ Загружено админов: {len(ADMIN_IDS)}")
logger.info(f"✅ ID админов: {ADMIN_IDS}")

# ==================== КОНФИГУРАЦИЯ БОТА ====================
class Config:
    MIN_SUM_RUB = 500
    MAX_SUM_RUB = 1_000_000
    BUY_MARKUP = 21
    SELL_MARKUP = 15
    PRICE_UPDATE_INTERVAL = 60
    MAX_ORDERS = 1000
    
    NETWORK_FEES = {'USDT': 1.0, 'BTC': 0.00002, 'LTC': 0.02, 'TON': 1.0}
    CRYPTO_SYMBOLS = {'USDT': '💵', 'BTC': '₿', 'LTC': 'Ł', 'TON': '💎'}
    CRYPTO_MAX_AMOUNTS = {'USDT': 10000, 'BTC': 10, 'LTC': 100, 'TON': 1000}
    
    BANKS = {
        'sber': '🏦 Сбербанк',
        'tbank': '🏦 Т-Банк',
        'alfa': '🏦 Альфа-Банк',
        'other': '💳 Другой банк'
    }
    
    RATE_LIMIT_MESSAGES = 100
    RATE_LIMIT_PERIOD = 60
    BLOCK_TIME = 60

config = Config()


# ==================== ИНИЦИАЛИЗАЦИЯ БОТА ====================
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
logger.info("✅ Бот инициализирован")


# ==================== МОДЕЛИ ДАННЫХ ====================
@dataclass
class Order:
    order_id: int
    operation: str
    crypto: str
    user_id: int
    user_name: str
    username: str
    rub_amount: float
    amount: float
    price: float
    fee: float
    price_currency: str = 'RUB'
    status: str = 'new'
    payment_method: Optional[str] = None
    payment_details: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: Optional[datetime] = None


class OrdersDB:
    def __init__(self, max_orders: int = 1000):
        self.orders: Dict[int, Order] = {}
        self.counter: int = 0
        self.lock = asyncio.Lock()
        self.max_orders = max_orders
    
    async def add(self, order_data: Dict) -> int:
        async with self.lock:
            self.counter += 1
            order = Order(order_id=self.counter, **order_data)
            self.orders[self.counter] = order
            if len(self.orders) > self.max_orders:
                del self.orders[min(self.orders.keys())]
            logger.info(f"✅ Создана заявка #{self.counter}")
            return self.counter
    
    async def get(self, order_id: int) -> Optional[Order]:
        return self.orders.get(order_id)
    
    async def update_status(self, order_id: int, status: str, **kwargs):
        if order_id in self.orders:
            self.orders[order_id].status = status
            self.orders[order_id].updated_at = datetime.now()
            for key, value in kwargs.items():
                setattr(self.orders[order_id], key, value)
            logger.info(f"📝 Заявка #{order_id} обновлена: статус={status}")
    
    def get_all(self) -> List[Order]:
        return list(self.orders.values())
    
    def get_by_status(self, status: str) -> List[Order]:
        return [o for o in self.orders.values() if o.status == status]
    
    def get_stats(self) -> Dict:
        if not self.orders:
            return {}
        orders_list = self.orders.values()
        return {
            'total': len(self.orders),
            'buy_count': sum(1 for o in orders_list if o.operation == 'buy'),
            'sell_count': sum(1 for o in orders_list if o.operation == 'sell'),
            'buy_volume': sum(o.rub_amount for o in orders_list if o.operation == 'buy'),
            'sell_volume': sum(o.amount for o in orders_list if o.operation == 'sell'),
            'completed': sum(1 for o in orders_list if o.status == 'completed')
        }

orders_db = OrdersDB(max_orders=config.MAX_ORDERS)


# ==================== ВАЛИДАТОРЫ ====================
class WalletValidator:
    PATTERNS = {
        'BTC': r'^(1|3|bc1|bc1p)[a-zA-HJ-NP-Z0-9]{25,62}$',
        'LTC': r'^(L|M|ltc1)[a-zA-HJ-NP-Z0-9]{26,42}$',
        'USDT': {
            'TRC20': r'^T[A-Za-z0-9]{33}$',
            'ERC20': r'^0x[a-fA-F0-9]{40}$',
            'BEP20': r'^0x[a-fA-F0-9]{40}$'
        },
        'TON': r'^(EQ|UQ)[A-Za-z0-9_-]{46,48}$'
    }
    
    NETWORK_NAMES = {
        'BTC': 'Bitcoin', 'LTC': 'Litecoin',
        'TRC20': 'TRC-20 (Tron)', 'ERC20': 'ERC-20 (Ethereum)',
        'BEP20': 'BEP-20 (BSC)', 'TON': 'TON'
    }
    
    @staticmethod
    async def validate(address: str, crypto: str) -> Tuple[bool, str, Optional[str]]:
        address = address.strip()
        if not address:
            return False, "❌ Адрес не может быть пустым", None
        if len(address) < 5 or len(address) > 100:
            return False, "❌ Неверная длина адреса", None
        
        validators = {
            'BTC': WalletValidator._validate_btc,
            'LTC': WalletValidator._validate_ltc,
            'USDT': WalletValidator._validate_usdt,
            'TON': WalletValidator._validate_ton
        }
        validator = validators.get(crypto)
        if validator:
            return await validator(address)
        return True, "⚠️ Нет проверки для этой валюты", "unknown"
    
    @staticmethod
    async def _validate_btc(address: str) -> Tuple[bool, str, str]:
        if re.match(WalletValidator.PATTERNS['BTC'], address):
            return True, "✅ Корректный Bitcoin адрес", "BTC"
        return False, "❌ Неверный формат Bitcoin адреса", None
    
    @staticmethod
    async def _validate_ltc(address: str) -> Tuple[bool, str, str]:
        if re.match(WalletValidator.PATTERNS['LTC'], address):
            return True, "✅ Корректный Litecoin адрес", "LTC"
        return False, "❌ Неверный формат Litecoin адреса", None
    
    @staticmethod
    async def _validate_usdt(address: str) -> Tuple[bool, str, str]:
        for network, pattern in WalletValidator.PATTERNS['USDT'].items():
            if re.match(pattern, address):
                return True, f"✅ Корректный {network} адрес", network
        return False, "❌ Неверный формат USDT адреса", None
    
    @staticmethod
    async def _validate_ton(address: str) -> Tuple[bool, str, str]:
        if re.match(WalletValidator.PATTERNS['TON'], address):
            return True, "✅ Корректный TON адрес", "TON"
        return False, "❌ Неверный формат TON адреса", None


class PaymentValidator:
    CARD_PATTERNS = {
        'visa': r'^4[0-9]{12}(?:[0-9]{3})?$',
        'mastercard': r'^5[1-5][0-9]{14}$',
        'maestro': r'^(5018|5020|5038|5612|5893|6304|6759|6761|6762|6763)[0-9]{8,15}$',
        'mir': r'^220[0-4][0-9]{12}$',
        'amex': r'^3[47][0-9]{13}$'
    }
    
    CARD_NAMES = {
        'visa': 'Visa', 'mastercard': 'MasterCard',
        'maestro': 'Maestro', 'mir': 'Мир', 'amex': 'American Express'
    }
    
    @staticmethod
    def validate_card(card_number: str) -> Tuple[bool, str, Optional[str]]:
        cleaned = ''.join(filter(str.isdigit, card_number))
        if len(cleaned) < 13 or len(cleaned) > 19:
            return False, "❌ Неверный номер карты", None
        if not PaymentValidator._luhn_check(cleaned):
            return False, "❌ Неверный номер карты", None
        card_type = PaymentValidator._detect_card_type(cleaned)
        type_name = PaymentValidator.CARD_NAMES.get(card_type, 'карта')
        return True, f"✅ Корректная {type_name}", card_type
    
    @staticmethod
    def validate_phone(phone: str) -> Tuple[bool, str, Optional[str]]:
        cleaned = ''.join(filter(str.isdigit, phone))
        if len(cleaned) == 11 and cleaned.startswith(('7', '8')):
            formatted = PaymentValidator.format_phone(cleaned)
            return True, f"✅ Корректный номер: {formatted}", formatted
        elif len(cleaned) == 10:
            formatted = PaymentValidator.format_phone('7' + cleaned)
            return True, f"✅ Корректный номер: {formatted}", formatted
        return False, "❌ Неверный номер телефона", None
    
    @staticmethod
    def _luhn_check(card_number: str) -> bool:
        digits = [int(d) for d in card_number]
        odd_digits = digits[-1::-2]
        even_digits = digits[-2::-2]
        total = sum(odd_digits)
        for d in even_digits:
            total += sum(divmod(d * 2, 10))
        return total % 10 == 0
    
    @staticmethod
    def _detect_card_type(card_number: str) -> Optional[str]:
        for card_type, pattern in PaymentValidator.CARD_PATTERNS.items():
            if re.match(pattern, card_number):
                return card_type
        return None
    
    @staticmethod
    def format_phone(phone: str) -> str:
        cleaned = ''.join(filter(str.isdigit, phone))
        if len(cleaned) == 11 and cleaned.startswith('7'):
            return f"+7 ({cleaned[1:4]}) {cleaned[4:7]}-{cleaned[7:9]}-{cleaned[9:11]}"
        return phone
    
    @staticmethod
    def format_card(card: str) -> str:
        cleaned = ''.join(filter(str.isdigit, card))
        if len(cleaned) == 16:
            return f"{cleaned[:4]} {cleaned[4:8]} {cleaned[8:12]} {cleaned[12:]}"
        return ' '.join([cleaned[i:i+4] for i in range(0, len(cleaned), 4)])

wallet_validator = WalletValidator()
payment_validator = PaymentValidator()


# ==================== УПРАВЛЕНИЕ КУРСАМИ ====================
class PriceManager:
    def __init__(self):
        self.prices: Dict[str, Optional[float]] = {'USDT': None, 'BTC': None, 'LTC': None, 'TON': None}
        self.last_update: Optional[datetime] = None
        self.lock = asyncio.Lock()
        self._update_count = 0
    
    async def fetch_prices(self) -> bool:
        for attempt in range(3):
            try:
                async with aiohttp.ClientSession() as session:
                    url = "https://api.coingecko.com/api/v3/simple/price"
                    params = {"ids": "tether,bitcoin,litecoin,the-open-network", "vs_currencies": "rub"}
                    async with session.get(url, params=params, timeout=10) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            async with self.lock:
                                self.prices['USDT'] = float(data['tether']['rub'])
                                self.prices['BTC'] = float(data['bitcoin']['rub'])
                                self.prices['LTC'] = float(data['litecoin']['rub'])
                                self.prices['TON'] = float(data['the-open-network']['rub'])
                                self.last_update = datetime.now()
                                self._update_count += 1
                            logger.info(f"✅ Курсы обновлены (#{self._update_count})")
                            return True
            except Exception as e:
                logger.warning(f"⚠️ Ошибка получения курсов: {e}")
            await asyncio.sleep(1)
        return False
    
    async def background_updater(self):
        while True:
            await self.fetch_prices()
            await asyncio.sleep(config.PRICE_UPDATE_INTERVAL)
    
    def get_price(self, crypto: str) -> Optional[float]:
        return self.prices.get(crypto)
    
    def get_buy_price(self, crypto: str) -> Optional[float]:
        price = self.prices.get(crypto)
        return price * (1 + config.BUY_MARKUP / 100) if price else None
    
    def get_sell_price(self, crypto: str) -> Optional[float]:
        price = self.prices.get(crypto)
        return price * (1 - config.SELL_MARKUP / 100) if price else None

price_manager = PriceManager()


# ==================== ЗАЩИТА ОТ ФЛУДА ====================
class RateLimiter:
    def __init__(self):
        self.user_messages = defaultdict(list)
        self.blocked_users = {}
    
    def is_rate_limited(self, user_id: int) -> Tuple[bool, Optional[int]]:
        now = time.time()
        if user_id in self.blocked_users:
            if now < self.blocked_users[user_id]:
                return True, int(self.blocked_users[user_id] - now)
            del self.blocked_users[user_id]
        self.user_messages[user_id] = [t for t in self.user_messages[user_id] if now - t < config.RATE_LIMIT_PERIOD]
        if len(self.user_messages[user_id]) >= config.RATE_LIMIT_MESSAGES:
            self.blocked_users[user_id] = now + config.BLOCK_TIME
            return True, config.BLOCK_TIME
        self.user_messages[user_id].append(now)
        return False, None

rate_limiter = RateLimiter()


# ==================== ФОРМАТТЕРЫ ====================
def format_amount(amount: float, crypto: str) -> str:
    if crypto == 'BTC':
        return f"{amount:.8f}".rstrip('0').rstrip('.') or '0'
    elif crypto == 'LTC':
        return f"{amount:.4f}".rstrip('0').rstrip('.') or '0'
    return f"{amount:.2f}".rstrip('0').rstrip('.') or '0'


# ==================== СОСТОЯНИЯ FSM ====================
class BuyStates(StatesGroup):
    choosing_crypto = State()
    choosing_payment = State()
    choosing_amount = State()
    entering_wallet = State()
    confirming = State()

class SellStates(StatesGroup):
    choosing_crypto = State()
    choosing_payment = State()
    choosing_amount = State()
    choosing_bank = State()
    entering_bank_name = State()
    entering_card = State()
    entering_phone = State()
    confirming = State()

class AdminStates(StatesGroup):
    waiting_for_message = State()
    waiting_for_reject_reason = State()


# ==================== ТЕКСТЫ ====================
WELCOME_TEXT = """
Krank exchange 24/7 — криптообменник с низкой комиссией

💸 Купить/💰 Продать криптовалюту

🐶 Тех.поддержка 24/7: @kb_BigsilA
"""

ADMIN_WELCOME_TEXT = "👋 Добро пожаловать в админ-панель!"

HELP_TEXT = """
📌 <b>Как пользоваться ботом:</b>

<b>Купить криптовалюту:</b>
1️⃣ Нажмите '💸 Купить'
2️⃣ Выберите валюту
3️⃣ Введите сумму
4️⃣ Укажите адрес кошелька
5️⃣ Подтвердите сделку

<b>Продать криптовалюту:</b>
1️⃣ Нажмите '💰 Продать'
2️⃣ Выберите валюту
3️⃣ Введите сумму
4️⃣ Выберите банк
5️⃣ Введите карту и телефон
6️⃣ Подтвердите сделку
"""

CONTACTS_TEXT = """
📞 <b>Контакты</b>

🐶 Тех.поддержка: @kb_BigsilA
⭐ Отзывы: @Krankreviewss
"""


# ==================== КЛАВИАТУРЫ ====================
def create_keyboard(buttons: List[List[str]]) -> ReplyKeyboardMarkup:
    keyboard = [[KeyboardButton(text=btn) for btn in row] for row in buttons]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

main_keyboard = create_keyboard([
    ["💸 Купить", "💰 Продать"],
    ["📞 Контакты", "❓ Помощь"]
])

admin_full_keyboard = create_keyboard([
    ["📥 Все заявки", "📋 Активные заявки"],
    ["💰 Все курсы", "📊 Статистика"],
    ["👤 Переключиться в пользователя"]
])

admin_user_keyboard = create_keyboard([
    ["💸 Купить", "💰 Продать"],
    ["📞 Контакты", "❓ Помощь"],
    ["👑 В админ-режим"]
])

navigation_keyboard = create_keyboard([["◀️ Назад", "🏠 Главное меню"]])

buy_crypto_keyboard = create_keyboard([
    ["💵 USDT", "₿ BTC", "Ł LTC", "💎 TON"],
    ["◀️ Назад", "🏠 Главное меню"]
])

sell_crypto_keyboard = create_keyboard([
    ["💵 USDT", "₿ BTC", "Ł LTC", "💎 TON"],
    ["◀️ Назад", "🏠 Главное меню"]
])

bank_keyboard = create_keyboard([
    ["🏦 Сбербанк", "🏦 Т-Банк", "🏦 Альфа-Банк"],
    ["💳 Другой банк"],
    ["◀️ Назад", "🏠 Главное меню"]
])

def get_payment_currency_keyboard(crypto: str) -> ReplyKeyboardMarkup:
    return create_keyboard([
        ["🇷🇺 Рубли (RUB)"],
        [f"{config.CRYPTO_SYMBOLS[crypto]} {crypto}"],
        ["◀️ Назад", "🏠 Главное меню"]
    ])

def get_sell_payment_keyboard(crypto: str) -> ReplyKeyboardMarkup:
    return create_keyboard([
        [f"{config.CRYPTO_SYMBOLS[crypto]} {crypto}"],
        ["🇷🇺 Рубли (RUB)"],
        ["◀️ Назад", "🏠 Главное меню"]
    ])


# ==================== СЛУЖЕБНЫЕ ФУНКЦИИ ====================
admin_mode: Dict[int, bool] = {}
active_dialogs: Dict[int, int] = {}


async def notify_admin_new_order(order_id: int, order_data: Dict):
    crypto = order_data['crypto']
    operation = order_data['operation']
    rub = order_data['rub_amount']
    amount = order_data['amount']
    user_name = order_data['user_name']
    username = order_data.get('username', 'нет')
    user_id = order_data['user_id']
    fee = order_data['fee']
    
    emoji = "🟢" if operation == 'buy' else "🔴"
    action = "ПОКУПКА" if operation == 'buy' else "ПРОДАЖА"
    payment_details = order_data.get('payment_details', '')
    
    # Функция для безопасного экранирования HTML
    def h(text):
        if text is None:
            return ""
        # Заменяем специальные HTML-символы
        return str(text).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;')
    
    text = (
        f"<b>🎫 НОВАЯ ЗАЯВКА #{order_id}</b>\n\n"
        f"{emoji} <b>{action} {config.CRYPTO_SYMBOLS[crypto]} {crypto}</b>\n\n"
        f"👤 <b>Клиент:</b> {h(user_name)}\n"
        f"📱 <b>Username:</b> @{h(username)}\n"
        f"🆔 <b>ID:</b> {user_id}\n"
        f"⏱ <b>Время:</b> {datetime.now().strftime('%d.%m.%Y %H:%M')}\n\n"
        f"💰 <b>ДЕТАЛИ СДЕЛКИ:</b>\n"
    )
    
    if operation == 'buy':
        wallet_address = "не указан"
        network_type = "неизвестно"
        if payment_details:
            address_match = re.search(r'Адрес: (\S+)', payment_details)
            if address_match:
                wallet_address = address_match.group(1)
            network_match = re.search(r'Сеть: ([^\n]+)', payment_details)
            if network_match:
                network_type = network_match.group(1)
        
        text += (
            f"💸 <b>Платит:</b> {rub:,.2f} RUB\n"
            f"💎 <b>Получает:</b> {format_amount(amount, crypto)} {crypto}\n"
            f"⛽ <b>Комиссия сети:</b> {format_amount(fee, crypto)} {crypto}\n"
            f"💱 <b>Курс:</b> {order_data['price']:,.2f} RUB\n\n"
            f"💳 <b>КОШЕЛЕК:</b>\n"
            f"• Адрес: <code>{h(wallet_address)}</code>\n"
            f"• Сеть: {h(network_type)}\n"
        )
    else:
        bank_name = order_data.get('payment_method', 'не указан')
        card_display = "не указана"
        phone_display = "не указан"
        if payment_details:
            card_match = re.search(r'Карта: ([\d\s]+)', payment_details)
            if card_match:
                card_display = card_match.group(1)
            phone_match = re.search(r'Телефон: ([\d\s\+\(\)\-]+)', payment_details)
            if phone_match:
                phone_display = phone_match.group(1)
        
        text += (
            f"💰 <b>Продает:</b> {format_amount(amount, crypto)} {crypto}\n"
            f"💸 <b>Получает:</b> {rub:,.2f} RUB\n"
            f"⛽ <b>Комиссия сети:</b> {format_amount(fee, crypto)} {crypto}\n"
            f"💱 <b>Курс:</b> {order_data['price']:,.2f} RUB\n\n"
            f"🏦 <b>РЕКВИЗИТЫ:</b>\n"
            f"• Банк: {h(bank_name)}\n"
            f"• Карта: <code>{h(card_display)}</code>\n"
            f"• Телефон: <code>{h(phone_display)}</code>\n"
        )
    
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Взять в работу", callback_data=f"take_{order_id}"),
             InlineKeyboardButton(text="💬 Написать", callback_data=f"msg_{order_id}")],
            [InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_{order_id}")]
        ]
    )
    
    # Отправляем ВСЕМ админам
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, text, reply_markup=keyboard, parse_mode="HTML")
            logger.info(f"📤 Уведомление о заявке #{order_id} отправлено админу {admin_id}")
        except Exception as e:
            logger.error(f"❌ Ошибка отправки админу {admin_id}: {e}")


async def send_order_details_to_user(order_id: int, user_id: int):
    logger.info(f"📤 Отправка деталей заявки #{order_id} пользователю {user_id}")
    order = await orders_db.get(order_id)
    if not order:
        logger.error(f"❌ Заявка #{order_id} не найдена")
        return
    
    # Функция для безопасного экранирования HTML
    def h(text):
        if text is None:
            return ""
        return str(text).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;')
    
    if order.operation == 'buy':
        wallet_address = "не указан"
        network = "неизвестно"
        if order.payment_details:
            addr_match = re.search(r'Адрес: (\S+)', order.payment_details)
            if addr_match:
                wallet_address = addr_match.group(1)
            net_match = re.search(r'Сеть: ([^\n]+)', order.payment_details)
            if net_match:
                network = net_match.group(1)
        
        text = (
            f"<b>✅ Заявка #{order_id} принята!</b>\n\n"
            f"📊 <b>Детали:</b>\n"
            f"💸 Вы платите: {order.rub_amount:,.2f} RUB\n"
            f"💎 Вы получите: {format_amount(order.amount, order.crypto)} {order.crypto}\n"
            f"⛽ Комиссия: {format_amount(order.fee, order.crypto)} {order.crypto}\n"
            f"💳 Адрес: <code>{h(wallet_address)}</code>\n"
            f"🌐 Сеть: {h(network)}\n"
            f"💱 Курс: {order.price:,.2f} RUB\n\n"
            f"👨‍💼 Оператор скоро свяжется с вами!"
        )
    else:
        bank_name = order.payment_method or "не указан"
        card_display = "не указана"
        phone_display = "не указан"
        if order.payment_details:
            card_match = re.search(r'Карта: ([\d\s]+)', order.payment_details)
            if card_match:
                card_display = card_match.group(1)
            phone_match = re.search(r'Телефон: ([\d\s\+\(\)\-]+)', order.payment_details)
            if phone_match:
                phone_display = phone_match.group(1)
        
        text = (
            f"<b>✅ Заявка #{order_id} принята!</b>\n\n"
            f"📊 <b>Детали:</b>\n"
            f"💰 Вы продаете: {format_amount(order.amount, order.crypto)} {order.crypto}\n"
            f"💸 Вы получите: {order.rub_amount:,.2f} RUB\n"
            f"⛽ Комиссия: {format_amount(order.fee, order.crypto)} {order.crypto}\n"
            f"🏦 Банк: {h(bank_name)}\n"
            f"💳 Карта: <code>{h(card_display)}</code>\n"
            f"📱 Телефон: <code>{h(phone_display)}</code>\n"
            f"💱 Курс: {order.price:,.2f} RUB\n\n"
            f"👨‍💼 Оператор скоро свяжется с вами!"
        )
    
    try:
        await bot.send_message(user_id, text, parse_mode="HTML")
        logger.info(f"✅ Детали заявки #{order_id} отправлены")
    except Exception as e:
        logger.error(f"❌ Ошибка отправки: {e}")


# ==================== MIDDLEWARE ====================
@dp.message.outer_middleware()
async def rate_limit_middleware(handler, event: types.Message, data: dict):
    if event.from_user.id in ADMIN_IDS:
        return await handler(event, data)
    is_limited, seconds = rate_limiter.is_rate_limited(event.from_user.id)
    if is_limited:
        await event.answer(f"⏳ Слишком много сообщений! Подождите {seconds // 60} мин {seconds % 60} сек.")
        return
    return await handler(event, data)


# ==================== ОБРАБОТЧИКИ КОМАНД ====================
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    if message.from_user.id in ADMIN_IDS:
        admin_mode[message.from_user.id] = True
        await message.answer(ADMIN_WELCOME_TEXT, reply_markup=admin_full_keyboard)
    else:
        await message.answer(WELCOME_TEXT, reply_markup=main_keyboard)

@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    await message.answer(HELP_TEXT, parse_mode="HTML")


# ==================== НАВИГАЦИЯ ====================
@dp.message(lambda message: message.text == "🏠 Главное меню")
async def go_to_main(message: types.Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    if user_id in ADMIN_IDS and admin_mode.get(user_id, True):
        await message.answer("◀️", reply_markup=admin_full_keyboard)
    elif user_id in ADMIN_IDS and not admin_mode.get(user_id, True):
        await message.answer("◀️", reply_markup=admin_user_keyboard)
    else:
        await message.answer("◀️", reply_markup=main_keyboard)

@dp.message(lambda message: message.text == "◀️ Назад")
async def go_back(message: types.Message, state: FSMContext):
    current = await state.get_state()
    if not current:
        await go_to_main(message, state)
        return
    if current.startswith('BuyStates'):
        await handle_buy_back(message, state)
    elif current.startswith('SellStates'):
        await handle_sell_back(message, state)
    else:
        await state.clear()
        await go_to_main(message, state)

async def handle_buy_back(message: types.Message, state: FSMContext):
    current = await state.get_state()
    data = await state.get_data()
    crypto = data.get('crypto')
    if current == BuyStates.confirming.state:
        await request_wallet_address(message, state, crypto)
    elif current == BuyStates.entering_wallet.state:
        await state.set_state(BuyStates.choosing_amount)
        await process_buy_amount_restart(message, state)
    elif current == BuyStates.choosing_amount.state:
        await state.set_state(BuyStates.choosing_payment)
        await show_payment_choice(message, crypto)
    elif current == BuyStates.choosing_payment.state:
        await state.set_state(BuyStates.choosing_crypto)
        await message.answer("💵 Выберите валюту:", reply_markup=buy_crypto_keyboard)
    else:
        await go_to_main(message, state)

async def handle_sell_back(message: types.Message, state: FSMContext):
    current = await state.get_state()
    data = await state.get_data()
    crypto = data.get('crypto')
    if current == SellStates.confirming.state:
        await state.set_state(SellStates.entering_phone)
        await message.answer("📱 Введите телефон:", reply_markup=navigation_keyboard)
    elif current == SellStates.entering_phone.state:
        await state.set_state(SellStates.entering_card)
        await message.answer("💳 Введите карту:", reply_markup=navigation_keyboard)
    elif current == SellStates.entering_card.state:
        await state.set_state(SellStates.choosing_bank)
        await message.answer("🏦 Выберите банк:", reply_markup=bank_keyboard)
    elif current == SellStates.choosing_bank.state:
        if data.get('bank_type') == 'other':
            await state.set_state(SellStates.entering_bank_name)
            await message.answer("💳 **Введите название вашего банка**\nНапример: ВТБ, Райффайзенбанк, Почта Банк", reply_markup=navigation_keyboard, parse_mode="Markdown")
        else:
            await state.set_state(SellStates.choosing_amount)
            await show_sell_amount_input(message, state, crypto)
    elif current == SellStates.entering_bank_name.state:
        await state.set_state(SellStates.choosing_bank)
        await message.answer("🏦 Выберите банк:", reply_markup=bank_keyboard)
    elif current == SellStates.choosing_amount.state:
        await state.set_state(SellStates.choosing_payment)
        await message.answer("Выберите способ:", reply_markup=get_sell_payment_keyboard(crypto))
    elif current == SellStates.choosing_payment.state:
        await state.set_state(SellStates.choosing_crypto)
        await message.answer("💸 Выберите валюту:", reply_markup=sell_crypto_keyboard)
    else:
        await go_to_main(message, state)


# ==================== АДМИН-ПАНЕЛЬ ====================
@dp.message(lambda message: message.from_user.id in ADMIN_IDS and message.text == "👤 Переключиться в пользователя")
async def switch_to_user(message: types.Message):
    admin_mode[message.from_user.id] = False
    await message.answer("🔄 Режим пользователя", reply_markup=admin_user_keyboard)

@dp.message(lambda message: message.from_user.id in ADMIN_IDS and message.text == "👑 В админ-режим")
async def switch_to_admin(message: types.Message):
    admin_mode[message.from_user.id] = True
    await message.answer(ADMIN_WELCOME_TEXT, reply_markup=admin_full_keyboard)

@dp.message(lambda message: message.from_user.id in ADMIN_IDS and admin_mode.get(message.from_user.id, True) and message.text == "📥 Все заявки")
async def admin_show_all_orders(message: types.Message):
    new_orders = orders_db.get_by_status('new')
    if not new_orders:
        await message.answer("📭 Нет новых заявок")
        return
    
    for order in sorted(new_orders, key=lambda x: x.created_at, reverse=True)[:15]:
        # Формируем ПОЛНУЮ информацию как в notify_admin_new_order
        emoji = "🟢" if order.operation == 'buy' else "🔴"
        action = "ПОКУПКА" if order.operation == 'buy' else "ПРОДАЖА"
        
        text = (
            f"🆕 **ЗАЯВКА #{order.order_id}**\n\n"
            f"{emoji} **{action} {config.CRYPTO_SYMBOLS[order.crypto]} {order.crypto}**\n\n"
            f"👤 **Клиент:** {order.user_name}\n"
            f"📱 **Username:** @{order.username if order.username else 'нет'}\n"
            f"🆔 **ID:** {order.user_id}\n"
            f"⏱ **Время:** {order.created_at.strftime('%d.%m.%Y %H:%M')}\n\n"
            f"💰 **ДЕТАЛИ СДЕЛКИ:**\n"
        )
        
        if order.operation == 'buy':
            wallet_address = "не указан"
            network_type = "неизвестно"
            if order.payment_details:
                address_match = re.search(r'Адрес: (\S+)', order.payment_details)
                if address_match:
                    wallet_address = address_match.group(1)
                network_match = re.search(r'Сеть: ([^\n]+)', order.payment_details)
                if network_match:
                    network_type = network_match.group(1)
            text += (
                f"💸 **Платит:** {order.rub_amount:,.2f} RUB\n"
                f"💎 **Получает:** {format_amount(order.amount, order.crypto)} {order.crypto}\n"
                f"⛽ **Комиссия сети:** {format_amount(order.fee, order.crypto)} {order.crypto}\n"
                f"💱 **Курс:** {order.price:,.2f} RUB\n\n"
                f"💳 **КОШЕЛЕК:**\n"
                f"• Адрес: `{wallet_address}`\n"
                f"• Сеть: {network_type}\n"
            )
        else:
            bank_name = order.payment_method or "не указан"
            card_display = "не указана"
            phone_display = "не указан"
            if order.payment_details:
                card_match = re.search(r'Карта: ([\d\s]+)', order.payment_details)
                if card_match:
                    card_display = card_match.group(1)
                phone_match = re.search(r'Телефон: ([\d\s\+\(\)\-]+)', order.payment_details)
                if phone_match:
                    phone_display = phone_match.group(1)
            text += (
                f"💰 **Продает:** {format_amount(order.amount, order.crypto)} {order.crypto}\n"
                f"💸 **Получает:** {order.rub_amount:,.2f} RUB\n"
                f"⛽ **Комиссия сети:** {format_amount(order.fee, order.crypto)} {order.crypto}\n"
                f"💱 **Курс:** {order.price:,.2f} RUB\n\n"
                f"🏦 **РЕКВИЗИТЫ:**\n"
                f"• Банк: {bank_name}\n"
                f"• Карта: `{card_display}`\n"
                f"• Телефон: `{phone_display}`\n"
            )
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✏️ Взять", callback_data=f"take_{order.order_id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_{order.order_id}")
        ]])
        
        await message.answer(text, reply_markup=keyboard)

@dp.message(lambda message: message.from_user.id in ADMIN_IDS and admin_mode.get(message.from_user.id, True) and message.text == "📋 Активные заявки")
async def admin_show_active_orders(message: types.Message):
    active = orders_db.get_by_status('waiting_payment') + orders_db.get_by_status('paid')
    if not active:
        await message.answer("📭 Нет активных заявок")
        return
    
    for order in sorted(active, key=lambda x: x.created_at, reverse=True):
        status_emoji = "⏳" if order.status == 'waiting_payment' else "✅"
        status_text = "ОЖИДАЕТ ОПЛАТЫ" if order.status == 'waiting_payment' else "ОПЛАЧЕНО"
        emoji = "🟢" if order.operation == 'buy' else "🔴"
        action = "ПОКУПКА" if order.operation == 'buy' else "ПРОДАЖА"
        
        text = (
            f"{status_emoji} **ЗАЯВКА #{order.order_id}**\n"
            f"**Статус:** {status_text}\n\n"
            f"{emoji} **{action} {config.CRYPTO_SYMBOLS[order.crypto]} {order.crypto}**\n\n"
            f"👤 **Клиент:** {order.user_name}\n"
            f"📱 **Username:** @{order.username if order.username else 'нет'}\n"
            f"🆔 **ID:** {order.user_id}\n"
            f"⏱ **Время:** {order.created_at.strftime('%d.%m.%Y %H:%M')}\n\n"
            f"💰 **ДЕТАЛИ СДЕЛКИ:**\n"
        )
        
        if order.operation == 'buy':
            wallet_address = "не указан"
            network_type = "неизвестно"
            if order.payment_details:
                address_match = re.search(r'Адрес: (\S+)', order.payment_details)
                if address_match:
                    wallet_address = address_match.group(1)
                network_match = re.search(r'Сеть: ([^\n]+)', order.payment_details)
                if network_match:
                    network_type = network_match.group(1)
            text += (
                f"💸 **Платит:** {order.rub_amount:,.2f} RUB\n"
                f"💎 **Получает:** {format_amount(order.amount, order.crypto)} {order.crypto}\n"
                f"⛽ **Комиссия сети:** {format_amount(order.fee, order.crypto)} {order.crypto}\n"
                f"💱 **Курс:** {order.price:,.2f} RUB\n\n"
                f"💳 **КОШЕЛЕК:**\n"
                f"• Адрес: `{wallet_address}`\n"
                f"• Сеть: {network_type}\n"
            )
        else:
            bank_name = order.payment_method or "не указан"
            card_display = "не указана"
            phone_display = "не указан"
            if order.payment_details:
                card_match = re.search(r'Карта: ([\d\s]+)', order.payment_details)
                if card_match:
                    card_display = card_match.group(1)
                phone_match = re.search(r'Телефон: ([\d\s\+\(\)\-]+)', order.payment_details)
                if phone_match:
                    phone_display = phone_match.group(1)
            text += (
                f"💰 **Продает:** {format_amount(order.amount, order.crypto)} {order.crypto}\n"
                f"💸 **Получает:** {order.rub_amount:,.2f} RUB\n"
                f"⛽ **Комиссия сети:** {format_amount(order.fee, order.crypto)} {order.crypto}\n"
                f"💱 **Курс:** {order.price:,.2f} RUB\n\n"
                f"🏦 **РЕКВИЗИТЫ:**\n"
                f"• Банк: {bank_name}\n"
                f"• Карта: `{card_display}`\n"
                f"• Телефон: `{phone_display}`\n"
            )
        
        # Кнопки для активных заявок
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="✅ Оплачено", callback_data=f"paid_{order.order_id}"),
                 InlineKeyboardButton(text="💬 Написать", callback_data=f"msg_{order.order_id}")],
                [InlineKeyboardButton(text="✅ Завершить", callback_data=f"complete_{order.order_id}"),
                 InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_{order.order_id}")]
            ]
        )
        
        await message.answer(text, reply_markup=keyboard)

@dp.message(lambda message: message.from_user.id in ADMIN_IDS and admin_mode.get(message.from_user.id, True) and message.text == "💰 Все курсы")
async def admin_show_prices(message: types.Message):
    text = "💰 **Текущие курсы:**\n\n"
    for crypto in ['USDT', 'BTC', 'LTC', 'TON']:
        if price_manager.prices.get(crypto):
            market = price_manager.prices[crypto]
            buy = price_manager.get_buy_price(crypto)
            sell = price_manager.get_sell_price(crypto)
            text += f"{config.CRYPTO_SYMBOLS[crypto]} **{crypto}**\n📊 Рынок: {market:,.2f} RUB\n📈 Покупка: {buy:,.2f} RUB\n📉 Продажа: {sell:,.2f} RUB\n\n"
    await message.answer(text, parse_mode="Markdown")

@dp.message(lambda message: message.from_user.id in ADMIN_IDS and admin_mode.get(message.from_user.id, True) and message.text == "📊 Статистика")
async def admin_show_stats(message: types.Message):
    stats = orders_db.get_stats()
    if not stats:
        await message.answer("📊 Статистика отсутствует")
        return
    new_count = len(orders_db.get_by_status('new'))
    waiting_count = len(orders_db.get_by_status('waiting_payment'))
    paid_count = len(orders_db.get_by_status('paid'))
    completed_count = len(orders_db.get_by_status('completed'))
    cancelled_count = len(orders_db.get_by_status('cancelled'))
    text = (
        f"📊 **СТАТИСТИКА**\n\n"
        f"📦 Всего заявок: {stats['total']}\n"
        f"🆕 Новые: {new_count}\n"
        f"⏳ В работе: {waiting_count}\n"
        f"✅ Оплачено: {paid_count}\n"
        f"🎉 Завершено: {completed_count}\n"
        f"❌ Отменено: {cancelled_count}\n\n"
        f"🟢 Покупок: {stats['buy_count']}\n"
        f"🔴 Продаж: {stats['sell_count']}\n"
    )
    await message.answer(text, parse_mode="Markdown")


# ==================== ОБРАБОТЧИКИ ДЕЙСТВИЙ С ЗАЯВКАМИ ====================
@dp.callback_query(lambda c: c.data and c.data.startswith('take_'))
async def take_order(callback: CallbackQuery):
    order_id = int(callback.data.split('_')[1])
    order = await orders_db.get(order_id)
    if not order:
        await callback.answer("❌ Заявка не найдена")
        return
    
    await orders_db.update_status(order_id, 'waiting_payment')
    
    if order.operation == 'buy':
        wallet_address = "не указан"
        if order.payment_details:
            addr_match = re.search(r'Адрес: (\S+)', order.payment_details)
            if addr_match:
                wallet_address = addr_match.group(1)
        text = (
            f"✅ **ЗАЯВКА ПРИНЯТА!**\n\n"
            f"{'=' * 30}\n"
            f"🔴🔴🔴 **НОМЕР ЗАЯВКИ: #{order_id}** 🔴🔴🔴\n"
            f"🔴🔴🔴 **ID ПОЛЬЗОВАТЕЛЯ: `{order.user_id}`** 🔴🔴🔴\n"
            f"{'=' * 30}\n\n"
            f"📊 **Детали сделки:**\n"
            f"💸 Вы платите: {order.rub_amount:,.2f} RUB\n"
            f"💎 Вы получите: {format_amount(order.amount, order.crypto)} {order.crypto}\n"
            f"⛽ Комиссия: {format_amount(order.fee, order.crypto)} {order.crypto}\n"
            f"💳 Адрес: `{wallet_address}`\n"
            f"💱 Курс: {order.price:,.2f} RUB\n\n"
            f"{'=' * 30}\n"
            f"🔴🔴🔴 **НОМЕР ЗАЯВКИ: #{order_id}** 🔴🔴🔴\n"
            f"🔴🔴🔴 **ID ПОЛЬЗОВАТЕЛЯ: `{order.user_id}`** 🔴🔴🔴\n"
            f"{'=' * 30}\n\n"
            f"👨‍💼 Оператор скоро свяжется с вами.\n"
            f"✉️ Для связи с поддержкой укажите номер заявки."
        )
    else:
        bank_display = order.payment_method or "не указан"
        text = (
            f"✅ **ЗАЯВКА ПРИНЯТА!**\n\n"
            f"{'=' * 30}\n"
            f"🔴🔴🔴 **НОМЕР ЗАЯВКИ: #{order_id}** 🔴🔴🔴\n"
            f"🔴🔴🔴 **ID ПОЛЬЗОВАТЕЛЯ: `{order.user_id}`** 🔴🔴🔴\n"
            f"{'=' * 30}\n\n"
            f"📊 **Детали сделки:**\n"
            f"💰 Вы продаете: {format_amount(order.amount, order.crypto)} {order.crypto}\n"
            f"💸 Вы получите: {order.rub_amount:,.2f} RUB\n"
            f"⛽ Комиссия: {format_amount(order.fee, order.crypto)} {order.crypto}\n"
            f"🏦 Банк: {bank_display}\n"
            f"💱 Курс: {order.price:,.2f} RUB\n\n"
            f"{'=' * 30}\n"
            f"🔴🔴🔴 **НОМЕР ЗАЯВКИ: #{order_id}** 🔴🔴🔴\n"
            f"🔴🔴🔴 **ID ПОЛЬЗОВАТЕЛЯ: `{order.user_id}`** 🔴🔴🔴\n"
            f"{'=' * 30}\n\n"
            f"👨‍💼 Оператор скоро свяжется с вами.\n"
            f"✉️ Для связи с поддержкой укажите номер заявки."
        )
    
    await bot.send_message(order.user_id, text, parse_mode="Markdown")
    await bot.send_message(order.user_id, f"📌 **ВАШ НОМЕР ЗАЯВКИ: #{order_id}**\n📌 **ВАШ ID: `{order.user_id}`**\nСохраните их!", parse_mode="Markdown")
    
    await callback.answer("✅ Заявка взята в работу")
    await callback.message.edit_text(f"{callback.message.text}\n\n🟢 В работе")

@dp.callback_query(lambda c: c.data and c.data.startswith('paid_'))
async def mark_paid(callback: CallbackQuery):
    order_id = int(callback.data.split('_')[1])
    order = await orders_db.get(order_id)
    if not order:
        await callback.answer("❌ Заявка не найдена")
        return
    await orders_db.update_status(order_id, 'paid')
    await bot.send_message(order.user_id, f"💰 По заявке #{order_id} получена оплата!\nСредства скоро будут отправлены.")
    await callback.answer("✅ Оплата отмечена")
    await callback.message.edit_text(f"{callback.message.text}\n\n💰 Оплачено")

@dp.callback_query(lambda c: c.data and c.data.startswith('complete_'))
async def complete_order(callback: CallbackQuery):
    order_id = int(callback.data.split('_')[1])
    order = await orders_db.get(order_id)
    if not order:
        await callback.answer("❌ Заявка не найдена")
        return
    await orders_db.update_status(order_id, 'completed')
    await bot.send_message(order.user_id, f"✅ Заявка #{order_id} завершена!\nСпасибо за использование сервиса!")
    await callback.answer("✅ Заявка завершена")
    await callback.message.edit_text(f"{callback.message.text}\n\n✅ Завершено")

@dp.callback_query(lambda c: c.data and c.data.startswith('reject_'))
async def reject_order_start(callback: CallbackQuery, state: FSMContext):
    order_id = int(callback.data.split('_')[1])
    order = await orders_db.get(order_id)
    if not order:
        await callback.answer("❌ Заявка не найдена")
        return
    await state.update_data(reject_order_id=order_id, reject_user_id=order.user_id)
    await callback.message.answer(f"❌ Отклонение заявки #{order_id}\nНапишите причину (или /skip):")
    await state.set_state(AdminStates.waiting_for_reject_reason)
    await callback.answer()

@dp.message(AdminStates.waiting_for_reject_reason)
async def reject_order_with_reason(message: types.Message, state: FSMContext):
    data = await state.get_data()
    order_id = data.get('reject_order_id')
    user_id = data.get('reject_user_id')
    reason = "не указана" if message.text == "/skip" else message.text
    await orders_db.update_status(order_id, 'cancelled')
    try:
        await bot.send_message(user_id, f"❌ Заявка #{order_id} отклонена\nПричина: {reason}")
        await message.answer(f"✅ Заявка #{order_id} отклонена")
    except Exception as e:
        await message.answer(f"✅ Заявка отклонена, но уведомить не удалось: {e}")
    await state.clear()

@dp.callback_query(lambda c: c.data and c.data.startswith('msg_'))
async def start_dialog(callback: CallbackQuery, state: FSMContext):
    order_id = int(callback.data.split('_')[1])
    order = await orders_db.get(order_id)
    if not order:
        await callback.answer("❌ Заявка не найдена")
        return
    await state.update_data(dialog_user_id=order.user_id, dialog_order_id=order_id)
    active_dialogs[callback.from_user.id] = order.user_id
    keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🔚 Завершить диалог")]], resize_keyboard=True)
    await callback.message.answer(f"💬 Диалог с {order.user_name}\nЗаявка #{order_id}\n\nНапишите сообщение:", reply_markup=keyboard)
    await state.set_state(AdminStates.waiting_for_message)
    await callback.answer()

@dp.message(AdminStates.waiting_for_message)
async def send_message_to_user(message: types.Message, state: FSMContext):
    if message.text == "🔚 Завершить диалог":
        await state.clear()
        if message.from_user.id in active_dialogs:
            del active_dialogs[message.from_user.id]
        await message.answer("✅ Диалог завершен", reply_markup=admin_full_keyboard)
        return
    data = await state.get_data()
    user_id = data.get('dialog_user_id')
    order_id = data.get('dialog_order_id')
    if not user_id:
        await message.answer("❌ Ошибка", reply_markup=admin_full_keyboard)
        await state.clear()
        return
    try:
        await bot.send_message(user_id, f"💬 Сообщение от оператора (Заявка #{order_id}):\n\n{message.text}")
        await message.answer("✅ Отправлено! Можете написать еще:")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")

@dp.message(lambda message: any(active_user_id == message.from_user.id for active_user_id in active_dialogs.values()))
async def forward_user_message(message: types.Message):
    for admin_id, user_id in active_dialogs.items():
        if user_id == message.from_user.id:
            try:
                await bot.send_message(admin_id, f"💬 Сообщение от {message.from_user.full_name}:\n\n{message.text}")
            except:
                pass
            break


# ==================== ПОКУПКА ====================
@dp.message(lambda message: message.text == "💸 Купить")
async def buy_menu(message: types.Message, state: FSMContext):
    if message.from_user.id in ADMIN_IDS and admin_mode.get(message.from_user.id, True):
        return
    await message.answer("💵 Выберите валюту:", reply_markup=buy_crypto_keyboard)
    await state.set_state(BuyStates.choosing_crypto)

@dp.message(BuyStates.choosing_crypto)
async def choose_crypto_buy(message: types.Message, state: FSMContext):
    crypto_map = {"💵 USDT": "USDT", "₿ BTC": "BTC", "Ł LTC": "LTC", "💎 TON": "TON"}
    if message.text not in crypto_map:
        await message.answer("❌ Выберите из меню")
        return
    crypto = crypto_map[message.text]
    await state.update_data(crypto=crypto)
    await show_payment_choice(message, crypto)
    await state.set_state(BuyStates.choosing_payment)

async def show_payment_choice(message: types.Message, crypto: str):
    buy_price = price_manager.get_buy_price(crypto)
    fee = config.NETWORK_FEES.get(crypto, 0)
    await message.answer(
        f"{config.CRYPTO_SYMBOLS[crypto]} **Покупка {crypto}**\n\n"
        f"💰 Курс: {buy_price:,.2f} RUB\n"
        f"⛽ Комиссия: {format_amount(fee, crypto)} {crypto}\n\n"
        f"Выберите чем оплатить:",
        reply_markup=get_payment_currency_keyboard(crypto),
        parse_mode="Markdown"
    )

@dp.message(BuyStates.choosing_payment)
async def choose_payment_currency(message: types.Message, state: FSMContext):
    data = await state.get_data()
    crypto = data['crypto']
    if message.text == "🇷🇺 Рубли (RUB)":
        await state.update_data(payment_currency='RUB')
        await message.answer("💰 Введите сумму в рублях:", reply_markup=navigation_keyboard)
        await state.set_state(BuyStates.choosing_amount)
    elif message.text == f"{config.CRYPTO_SYMBOLS[crypto]} {crypto}":
        await state.update_data(payment_currency='CRYPTO')
        await message.answer(f"💎 Введите сумму в {crypto} (сколько хотите получить):", reply_markup=navigation_keyboard)
        await state.set_state(BuyStates.choosing_amount)
    elif message.text == "◀️ Назад":
        await buy_menu(message, state)
    else:
        await message.answer("❌ Выберите из меню")

@dp.message(BuyStates.choosing_amount)
async def process_buy_amount(message: types.Message, state: FSMContext):
    try:
        amount = float(message.text.replace(',', '.').replace(' ', ''))
    except ValueError:
        await message.answer("❌ Введите число")
        return
    if amount <= 0:
        await message.answer("❌ Сумма должна быть > 0")
        return
    data = await state.get_data()
    crypto = data['crypto']
    payment_currency = data.get('payment_currency')
    buy_price = price_manager.get_buy_price(crypto)
    fee = config.NETWORK_FEES.get(crypto, 0)
    if not buy_price:
        await message.answer("❌ Ошибка получения курса")
        return
    if payment_currency == 'RUB':
        rub_amount = amount
        if rub_amount < config.MIN_SUM_RUB:
            await message.answer(f"❌ Минимум {config.MIN_SUM_RUB} RUB")
            return
        if rub_amount > config.MAX_SUM_RUB:
            await message.answer(f"❌ Максимум {config.MAX_SUM_RUB:,.0f} RUB")
            return
        crypto_amount = rub_amount / buy_price - fee
        if crypto_amount <= 0:
            await message.answer(f"❌ Слишком мало, комиссия {fee} {crypto}")
            return
        display = f"💸 Вы платите: {rub_amount:,.2f} RUB\n💎 Вы получите: {format_amount(crypto_amount, crypto)} {crypto}"
        await state.update_data(rub_amount=rub_amount, amount=crypto_amount, fee=fee, display_text=display)
    else:
        desired = amount
        rub_needed = (desired + fee) * buy_price
        if rub_needed < config.MIN_SUM_RUB:
            await message.answer("❌ Минимальная сумма в RUB слишком мала")
            return
        if rub_needed > config.MAX_SUM_RUB:
            await message.answer("❌ Превышен лимит")
            return
        display = f"💎 Вы получите: {format_amount(desired, crypto)} {crypto}\n💸 Нужно заплатить: {rub_needed:,.2f} RUB"
        await state.update_data(rub_amount=rub_needed, amount=desired, fee=fee, display_text=display)
    await request_wallet_address(message, state, crypto)

async def process_buy_amount_restart(message: types.Message, state: FSMContext):
    data = await state.get_data()
    crypto = data['crypto']
    if data.get('payment_currency') == 'RUB':
        await message.answer("💰 Введите сумму в рублях:", reply_markup=navigation_keyboard)
    else:
        await message.answer(f"💎 Введите сумму в {crypto}:", reply_markup=navigation_keyboard)

async def request_wallet_address(message: types.Message, state: FSMContext, crypto: str):
    examples = {'BTC': 'bc1q...', 'LTC': 'ltc1q...', 'USDT': 'T... или 0x...', 'TON': 'EQ...'}
    await message.answer(
        f"💳 **Укажите ваш {crypto} кошелек**\n\n"
        f"Пример: {examples.get(crypto, 'адрес')}\n\n"
        f"Внимательно проверьте адрес!",
        reply_markup=navigation_keyboard,
        parse_mode="Markdown"
    )
    await state.set_state(BuyStates.entering_wallet)

@dp.message(BuyStates.entering_wallet)
async def enter_wallet(message: types.Message, state: FSMContext):
    wallet = message.text.strip()
    if message.text == "◀️ Назад":
        await buy_menu(message, state)
        return
    data = await state.get_data()
    crypto = data['crypto']
    wait = await message.answer("🔄 Проверяю адрес...")
    is_valid, msg, network = await wallet_validator.validate(wallet, crypto)
    await wait.delete()
    if not is_valid:
        await message.answer("❌ Неверный адрес\nПопробуйте снова:", reply_markup=navigation_keyboard)
        return
    await state.update_data(wallet_address=wallet, network_type=network)
    data = await state.get_data()
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data="confirm_buy")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
    ])
    network_info = f"\n🌐 Сеть: {network}" if network else ""
    await message.answer(
        f"📊 **Детали покупки:**\n\n"
        f"{data['display_text']}\n"
        f"⛽ Комиссия: {format_amount(data['fee'], crypto)} {crypto}\n"
        f"💳 Кошелек: `{wallet}`{network_info}\n\n"
        f"Всё верно?",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )
    await state.set_state(BuyStates.confirming)


# ==================== ПРОДАЖА ====================
@dp.message(lambda message: message.text == "💰 Продать")
async def sell_menu(message: types.Message, state: FSMContext):
    if message.from_user.id in ADMIN_IDS and admin_mode.get(message.from_user.id, True):
        return
    await message.answer("💸 Выберите валюту:", reply_markup=sell_crypto_keyboard)
    await state.set_state(SellStates.choosing_crypto)

@dp.message(SellStates.choosing_crypto)
async def choose_crypto_sell(message: types.Message, state: FSMContext):
    crypto_map = {"💵 USDT": "USDT", "₿ BTC": "BTC", "Ł LTC": "LTC", "💎 TON": "TON"}
    if message.text not in crypto_map:
        await message.answer("❌ Выберите из меню")
        return
    crypto = crypto_map[message.text]
    await state.update_data(crypto=crypto)
    sell_price = price_manager.get_sell_price(crypto)
    fee = config.NETWORK_FEES.get(crypto, 0)
    await message.answer(
        f"{config.CRYPTO_SYMBOLS[crypto]} **Продажа {crypto}**\n\n"
        f"💰 Курс: {sell_price:,.2f} RUB\n"
        f"⛽ Комиссия: {fee} {crypto}\n\n"
        f"Выберите способ:",
        reply_markup=get_sell_payment_keyboard(crypto),
        parse_mode="Markdown"
    )
    await state.set_state(SellStates.choosing_payment)

@dp.message(SellStates.choosing_payment)
async def choose_sell_payment(message: types.Message, state: FSMContext):
    data = await state.get_data()
    crypto = data['crypto']
    if message.text == f"{config.CRYPTO_SYMBOLS[crypto]} {crypto}":
        await state.update_data(input_type='crypto')
        await show_sell_amount_input(message, state, crypto)
    elif message.text == "🇷🇺 Рубли (RUB)":
        await state.update_data(input_type='rub')
        await show_sell_amount_input(message, state, crypto)
    elif message.text == "◀️ Назад":
        await sell_menu(message, state)
    else:
        await message.answer("❌ Выберите из меню")

async def show_sell_amount_input(message: types.Message, state: FSMContext, crypto: str):
    data = await state.get_data()
    if data.get('input_type') == 'crypto':
        await message.answer(f"💰 Введите сумму в {crypto}:", reply_markup=navigation_keyboard)
    else:
        await message.answer("💸 Введите сумму в RUB:", reply_markup=navigation_keyboard)
    await state.set_state(SellStates.choosing_amount)

@dp.message(SellStates.choosing_amount)
async def process_sell_amount(message: types.Message, state: FSMContext):
    try:
        amount = float(message.text.replace(',', '.').replace(' ', ''))
    except ValueError:
        await message.answer("❌ Введите число")
        return
    data = await state.get_data()
    crypto = data['crypto']
    input_type = data.get('input_type')
    sell_price = price_manager.get_sell_price(crypto)
    fee = config.NETWORK_FEES.get(crypto, 0)
    if input_type == 'crypto':
        crypto_amount = amount
        if crypto_amount <= fee:
            await message.answer(f"❌ Должно быть больше комиссии {fee} {crypto}")
            return
        rub_amount = (crypto_amount - fee) * sell_price
        if rub_amount < config.MIN_SUM_RUB:
            await message.answer(f"❌ Минимальная сумма {config.MIN_SUM_RUB} RUB")
            return
        display = f"💰 Вы продаете: {format_amount(crypto_amount, crypto)} {crypto}\n💸 Вы получите: {rub_amount:,.2f} RUB"
        await state.update_data(crypto_amount=crypto_amount, rub_amount=rub_amount, amount_after_fee=crypto_amount - fee, fee=fee, display_text=display)
    else:
        rub_amount = amount
        if rub_amount < config.MIN_SUM_RUB:
            await message.answer(f"❌ Минимум {config.MIN_SUM_RUB} RUB")
            return
        crypto_needed = rub_amount / sell_price + fee
        display = f"💸 Вы получите: {rub_amount:,.2f} RUB\n💰 Нужно продать: {format_amount(crypto_needed, crypto)} {crypto}"
        await state.update_data(crypto_amount=crypto_needed, rub_amount=rub_amount, amount_after_fee=rub_amount / sell_price, fee=fee, display_text=display)
    await message.answer(f"📊 **Расчет:**\n\n{display}\n\n🏦 Выберите банк:", reply_markup=bank_keyboard, parse_mode="Markdown")
    await state.set_state(SellStates.choosing_bank)

@dp.message(SellStates.choosing_bank)
async def choose_bank(message: types.Message, state: FSMContext):
    bank_map = {"🏦 Сбербанк": "sber", "🏦 Т-Банк": "tbank", "🏦 Альфа-Банк": "alfa", "💳 Другой банк": "other"}
    if message.text not in bank_map:
        await message.answer("❌ Выберите из меню")
        return
    bank_type = bank_map[message.text]
    if bank_type == "other":
        await message.answer("💳 **Введите название вашего банка**\nНапример: ВТБ, Райффайзенбанк, Почта Банк", reply_markup=navigation_keyboard, parse_mode="Markdown")
        await state.set_state(SellStates.entering_bank_name)
    else:
        await state.update_data(bank_type=bank_type, bank_name=config.BANKS[bank_type])
        await message.answer("💳 **Введите номер карты:**\nПример: 2200 1234 5678 9012", reply_markup=navigation_keyboard, parse_mode="Markdown")
        await state.set_state(SellStates.entering_card)

@dp.message(SellStates.entering_bank_name)
async def enter_bank_name(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.set_state(SellStates.choosing_bank)
        await message.answer("🏦 Выберите банк:", reply_markup=bank_keyboard)
        return
    bank_name = message.text.strip()
    if len(bank_name) < 2 or len(bank_name) > 50:
        await message.answer("❌ Название банка должно быть от 2 до 50 символов")
        return
    await state.update_data(bank_type='other', bank_name=f"💳 {bank_name}")
    await message.answer("💳 **Введите номер карты:**\nПример: 2200 1234 5678 9012", reply_markup=navigation_keyboard, parse_mode="Markdown")
    await state.set_state(SellStates.entering_card)

@dp.message(SellStates.entering_card)
async def enter_card(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.set_state(SellStates.choosing_bank)
        await message.answer("🏦 Выберите банк:", reply_markup=bank_keyboard)
        return
    card = message.text.strip()
    wait = await message.answer("🔄 Проверяю карту...")
    is_valid, msg, card_type = payment_validator.validate_card(card)
    await wait.delete()
    if not is_valid:
        await message.answer("❌ Неверный номер карты\nПопробуйте снова:", reply_markup=navigation_keyboard)
        return
    formatted = payment_validator.format_card(card)
    await state.update_data(card_number=formatted, card_type=card_type)
    await message.answer(f"✅ {msg}\n\n📱 **Введите телефон:**\nПример: +7 (999) 123-45-67", reply_markup=navigation_keyboard, parse_mode="Markdown")
    await state.set_state(SellStates.entering_phone)

@dp.message(SellStates.entering_phone)
async def enter_phone(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.set_state(SellStates.entering_card)
        await message.answer("💳 Введите карту:", reply_markup=navigation_keyboard)
        return
    phone = message.text.strip()
    wait = await message.answer("🔄 Проверяю телефон...")
    is_valid, msg, formatted = payment_validator.validate_phone(phone)
    await wait.delete()
    if not is_valid:
        await message.answer("❌ Неверный номер\nПопробуйте снова:", reply_markup=navigation_keyboard)
        return
    await state.update_data(phone_number=formatted)
    data = await state.get_data()
    bank_display = data.get('bank_name', '🏦 Банк')
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data="confirm_sell")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
    ])
    await message.answer(
        f"📊 **Детали продажи:**\n\n"
        f"{data['display_text']}\n"
        f"⛽ Комиссия: {format_amount(data['fee'], data['crypto'])} {data['crypto']}\n"
        f"🏦 Банк: {bank_display}\n"
        f"💳 Карта: `{data['card_number']}`\n"
        f"📱 Телефон: `{formatted}`\n\n"
        f"Всё верно?",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )
    await state.set_state(SellStates.confirming)


# ==================== ПОДТВЕРЖДЕНИЕ ЗАЯВОК ====================
@dp.callback_query(lambda c: c.data == "confirm_buy")
async def confirm_buy(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    user = callback.from_user
    crypto = data['crypto']
    rate = price_manager.get_buy_price(crypto)
    network_info = f"\n🌐 Сеть: {wallet_validator.NETWORK_NAMES.get(data.get('network_type'), data.get('network_type', 'неизвестно'))}"
    wallet_info = f"Адрес: {data['wallet_address']}{network_info}"
    full_details = f"{data['display_text']}\n💳 Кошелек: {wallet_info}"
    order_data = {
        'operation': 'buy', 'crypto': crypto, 'user_id': user.id, 'user_name': user.full_name,
        'username': user.username, 'rub_amount': data['rub_amount'], 'amount': data['amount'],
        'price': rate, 'price_currency': 'RUB', 'fee': data['fee'], 'status': 'new',
        'payment_method': f"Кошелек {crypto}", 'payment_details': full_details
    }
    order_id = await orders_db.add(order_data)
    await notify_admin_new_order(order_id, order_data)
    await callback.message.edit_text(
        f"✅ **Ваша заявка №{order_id} отправлена на рассмотрение!**\n\nОжидайте, оператор скоро свяжется с вами.",
        parse_mode="Markdown", reply_markup=None
    )
    if user.id in ADMIN_IDS and admin_mode.get(user.id, True):
        await callback.message.answer("◀️", reply_markup=admin_full_keyboard)
    elif user.id in ADMIN_IDS and not admin_mode.get(user.id, True):
        await callback.message.answer("◀️", reply_markup=admin_user_keyboard)
    else:
        await callback.message.answer("◀️", reply_markup=main_keyboard)
    await state.clear()

@dp.callback_query(lambda c: c.data == "confirm_sell")
async def confirm_sell(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    user = callback.from_user
    crypto = data['crypto']
    bank_display = data.get('bank_name', '🏦 Банк')
    card = data['card_number']
    phone = data['phone_number']
    formatted_card = card if len(card) != 16 else f"{card[:4]} {card[4:8]} {card[8:12]} {card[12:]}"
    payment_details = f"Банк: {bank_display}\nКарта: {formatted_card}\nТелефон: {phone}"
    order_data = {
        'operation': 'sell', 'crypto': crypto, 'user_id': user.id, 'user_name': user.full_name,
        'username': user.username, 'rub_amount': data['rub_amount'], 'amount': data['amount_after_fee'],
        'price': price_manager.get_sell_price(crypto), 'price_currency': 'RUB', 'fee': data['fee'],
        'status': 'new', 'payment_method': bank_display, 'payment_details': payment_details
    }
    order_id = await orders_db.add(order_data)
    await notify_admin_new_order(order_id, order_data)
    await callback.message.edit_text(
        f"✅ **Ваша заявка №{order_id} отправлена на рассмотрение!**\n\nОжидайте, оператор скоро свяжется с вами.",
        parse_mode="Markdown", reply_markup=None
    )
    if user.id in ADMIN_IDS and admin_mode.get(user.id, True):
        await callback.message.answer("◀️", reply_markup=admin_full_keyboard)
    elif user.id in ADMIN_IDS and not admin_mode.get(user.id, True):
        await callback.message.answer("◀️", reply_markup=admin_user_keyboard)
    else:
        await callback.message.answer("◀️", reply_markup=main_keyboard)
    await state.clear()

@dp.callback_query(lambda c: c.data == "cancel")
async def cancel(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("❌ Отменено")
    await go_to_main(callback.message, state)


# ==================== ПРОЧИЕ ОБРАБОТЧИКИ ====================
@dp.message(lambda message: message.text == "📞 Контакты")
async def contacts(message: types.Message):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🐶 Поддержка", url="https://t.me/kb_BigsilA")],
        [InlineKeyboardButton(text="⭐ Отзывы", url="https://t.me/Krankreviewss")]
    ])
    await message.answer(CONTACTS_TEXT, reply_markup=keyboard, parse_mode="HTML")

@dp.message(lambda message: message.text == "❓ Помощь")
async def help_menu(message: types.Message):
    await cmd_help(message)


def escape_markdown(text: str) -> str:
    """Экранирует специальные символы Markdown"""
    special_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
    for char in special_chars:
        text = text.replace(char, f'\\{char}')
    return text


# ==================== ЗАПУСК БОТА ====================
async def main():
    logger.info("=" * 50)
    logger.info("ЗАПУСК БОТА")
    logger.info(f"📊 Администратор: {ADMIN_ID}")
    logger.info("=" * 50)
    await price_manager.fetch_prices()
    asyncio.create_task(price_manager.background_updater())
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Бот остановлен")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
