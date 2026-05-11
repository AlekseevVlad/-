"""
🧘 YOGA BOT v6 — Личный кабинет + подтверждение админа + канал уведомлений
+ админский сценарий добавления текущего студента вручную

pip install aiogram==3.13.0 asyncpg apscheduler==3.10.4 python-dotenv==1.0.1

.env:
    BOT_TOKEN=...
    ADMIN_IDS=123456789
    ADMIN_CHANNEL_ID=-5112695392
    DATABASE_URL=postgresql://yogauser:yoga1234@localhost:5432/yogabot
    STUDIO_ADDRESS=Адрес студии
    STUDIO_MAP_URL=https://maps.google.com
"""

import asyncio
import logging
import os
import json
from datetime import datetime, timedelta, date
from typing import Optional

import asyncpg
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, Router, F
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove,
)
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
#  КОНФИГУРАЦИЯ
# ══════════════════════════════════════════════════════════════════════════════

BOT_TOKEN        = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN")
ADMIN_IDS        = [int(x) for x in os.getenv("ADMIN_IDS", "0").split(",") if x.strip()]
ADMIN_CHANNEL_ID = int(os.getenv("ADMIN_CHANNEL_ID", "-5112695392"))
DATABASE_URL     = os.getenv("DATABASE_URL", "postgresql://yogauser:yoga1234@localhost:5432/yogabot")
STUDIO_MAP       = os.getenv("STUDIO_MAP_URL", "https://maps.google.com")
STUDIO_ADDR      = os.getenv("STUDIO_ADDRESS", "Адрес студии")
TZ               = "Asia/Ho_Chi_Minh"
MAX_STUDENTS     = 16
SUB_WARN_DAYS    = 5

# Слоты занятий
SLOTS = {
    "tue_morning": {"weekday": 1, "label": "Вт 09:00", "time_type": "morning", "hour": 9,  "minute": 0},
    "thu_morning": {"weekday": 3, "label": "Чт 09:00", "time_type": "morning", "hour": 9,  "minute": 0},
    "sat_morning": {"weekday": 5, "label": "Сб 09:00", "time_type": "morning", "hour": 9,  "minute": 0},
    "wed_evening": {"weekday": 2, "label": "Ср 19:00", "time_type": "evening", "hour": 19, "minute": 0},
    "fri_evening": {"weekday": 4, "label": "Пт 19:00", "time_type": "evening", "hour": 19, "minute": 0},
}
MORNING_SLOTS = ["tue_morning", "thu_morning", "sat_morning"]
EVENING_SLOTS = ["wed_evening", "fri_evening"]
ALL_SLOTS     = MORNING_SLOTS + EVENING_SLOTS

WEEKDAY_RU = {0: "Пн", 1: "Вт", 2: "Ср", 3: "Чт", 4: "Пт", 5: "Сб", 6: "Вс"}

# Правила выбора дней
RULES = {
    ("morning", 12): {"slots": MORNING_SLOTS, "per_week": 3, "calendar": False},
    ("morning",  8): {"slots": MORNING_SLOTS, "per_week": 2, "calendar": True},
    ("evening",  8): {"slots": EVENING_SLOTS, "per_week": 2, "calendar": False},
    ("mixed",   12): {"slots": ALL_SLOTS,     "per_week": 3, "calendar": True},
    ("mixed",    8): {"slots": ALL_SLOTS,     "per_week": 2, "calendar": True},
}

pool: Optional[asyncpg.Pool] = None

# ══════════════════════════════════════════════════════════════════════════════
#  БАЗА ДАННЫХ
# ══════════════════════════════════════════════════════════════════════════════

async def init_db():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    async with pool.acquire() as con:
        await con.execute("""
        CREATE TABLE IF NOT EXISTS students (
            id            SERIAL PRIMARY KEY,
            telegram_id   BIGINT UNIQUE NOT NULL,
            username      TEXT DEFAULT '',
            first_name    TEXT DEFAULT '',
            phone         TEXT DEFAULT '',
            group_type    TEXT,
            sub_type      TEXT,
            sub_status    TEXT DEFAULT 'none',
            classes_total INT DEFAULT 0,
            classes_left  INT DEFAULT 0,
            sub_start     DATE,
            sub_expires   DATE,
            registered_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS subscription_requests (
            id            SERIAL PRIMARY KEY,
            telegram_id   BIGINT NOT NULL,
            group_type    TEXT NOT NULL,
            sub_key       TEXT NOT NULL,
            classes       INT NOT NULL,
            selected_days TEXT,
            status        TEXT DEFAULT 'pending',
            created_at    TIMESTAMP DEFAULT NOW(),
            resolved_at   TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS enrollments (
            id                 SERIAL PRIMARY KEY,
            student_id         BIGINT NOT NULL,
            slot_key           TEXT NOT NULL,
            class_date         TIMESTAMP NOT NULL,
            status             TEXT DEFAULT 'confirmed',
            enrolled_at        TIMESTAMP DEFAULT NOW(),
            day_reminder_sent  BOOLEAN DEFAULT FALSE,
            hour_reminder_sent BOOLEAN DEFAULT FALSE,
            UNIQUE(student_id, class_date, slot_key)
        );
        """)
    log.info("✔️ База данных готова")


async def close_db():
    if pool:
        await pool.close()

# ── Студенты ──────────────────────────────────────────────────────────────────

async def db_get_student(tid: int) -> Optional[dict]:
    async with pool.acquire() as con:
        r = await con.fetchrow("SELECT * FROM students WHERE telegram_id=$1", tid)
        return dict(r) if r else None


async def db_create_student(tid: int, username: str, first_name: str, phone: str):
    async with pool.acquire() as con:
        await con.execute("""
            INSERT INTO students (telegram_id, username, first_name, phone)
            VALUES ($1,$2,$3,$4)
            ON CONFLICT (telegram_id) DO UPDATE
                SET username=$2, first_name=$3, phone=$4
        """, tid, username or "", first_name, phone)


async def db_all_students() -> list:
    async with pool.acquire() as con:
        rows = await con.fetch("SELECT * FROM students ORDER BY registered_at DESC")
        return [dict(r) for r in rows]


async def db_activate_subscription(tid: int, group: str, sub_key: str, classes: int):
    now = datetime.now().date()
    exp = now + timedelta(days=30)
    async with pool.acquire() as con:
        await con.execute("""
            UPDATE students SET
                group_type    = $1,
                sub_type      = $2,
                sub_status    = 'active',
                classes_total = $3,
                classes_left  = $3,
                sub_start     = $4,
                sub_expires   = $5
            WHERE telegram_id = $6
        """, group, sub_key, classes, now, exp, tid)


async def db_upsert_existing_student(
    tid: int,
    first_name: str,
    phone: str,
    group_type: str,
    sub_type: str,
    classes_total: int,
    classes_left: int,
    sub_start: date,
    sub_expires: date,
):
    async with pool.acquire() as con:
        await con.execute("""
            INSERT INTO students (
                telegram_id, username, first_name, phone,
                group_type, sub_type, sub_status,
                classes_total, classes_left, sub_start, sub_expires
            )
            VALUES ($1, '', $2, $3, $4, $5, 'active', $6, $7, $8, $9)
            ON CONFLICT (telegram_id) DO UPDATE SET
                first_name=$2,
                phone=$3,
                group_type=$4,
                sub_type=$5,
                sub_status='active',
                classes_total=$6,
                classes_left=$7,
                sub_start=$8,
                sub_expires=$9
        """, tid, first_name, phone, group_type, sub_type, classes_total, classes_left, sub_start, sub_expires)


async def db_delete_student(tid: int):
    async with pool.acquire() as con:
        await con.execute("DELETE FROM enrollments WHERE student_id=$1", tid)
        await con.execute("DELETE FROM subscription_requests WHERE telegram_id=$1", tid)
        await con.execute("DELETE FROM students WHERE telegram_id=$1", tid)


async def db_decrement_classes(tid: int) -> int:
    async with pool.acquire() as con:
        r = await con.fetchrow("""
            UPDATE students
            SET classes_left = GREATEST(classes_left - 1, 0)
            WHERE telegram_id = $1
            RETURNING classes_left
        """, tid)
        return r["classes_left"] if r else 0


async def db_increment_classes(tid: int):
    async with pool.acquire() as con:
        await con.execute("""
            UPDATE students SET classes_left = classes_left + 1
            WHERE telegram_id = $1
        """, tid)

# ── Заявки на абонемент ───────────────────────────────────────────────────────

async def db_create_request(tid: int, group: str, sub_key: str, classes: int, selected_days: str) -> int:
    async with pool.acquire() as con:
        r = await con.fetchrow("""
            INSERT INTO subscription_requests
                (telegram_id, group_type, sub_key, classes, selected_days)
            VALUES ($1,$2,$3,$4,$5)
            RETURNING id
        """, tid, group, sub_key, classes, selected_days)
        return r["id"]


async def db_get_request(req_id: int) -> Optional[dict]:
    async with pool.acquire() as con:
        r = await con.fetchrow("SELECT * FROM subscription_requests WHERE id=$1", req_id)
        return dict(r) if r else None


async def db_resolve_request(req_id: int, status: str):
    async with pool.acquire() as con:
        await con.execute("""
            UPDATE subscription_requests
            SET status=$1, resolved_at=NOW()
            WHERE id=$2
        """, status, req_id)


async def db_pending_requests() -> list:
    async with pool.acquire() as con:
        rows = await con.fetch("""
            SELECT r.*, s.first_name, s.username, s.phone
            FROM subscription_requests r
            JOIN students s ON s.telegram_id = r.telegram_id
            WHERE r.status='pending'
            ORDER BY r.created_at
        """)
        return [dict(r) for r in rows]

# ── Записи на занятия ─────────────────────────────────────────────────────────

async def db_enroll(tid: int, slot_key: str, class_date: datetime):
    async with pool.acquire() as con:
        await con.execute("""
            INSERT INTO enrollments (student_id, slot_key, class_date, status)
            VALUES ($1,$2,$3,'confirmed')
            ON CONFLICT DO NOTHING
        """, tid, slot_key, class_date)


async def db_cancel_enrollment(tid: int, slot_key: str, class_date: datetime):
    async with pool.acquire() as con:
        await con.execute("""
            UPDATE enrollments SET status='cancelled'
            WHERE student_id=$1 AND slot_key=$2 AND class_date=$3
        """, tid, slot_key, class_date)


async def db_complete_past_enrollments():
    async with pool.acquire() as con:
        await con.execute("""
            UPDATE enrollments SET status='completed'
            WHERE class_date < NOW() AND status='confirmed'
        """)


async def db_future_enrollments(tid: int) -> list:
    async with pool.acquire() as con:
        rows = await con.fetch("""
            SELECT * FROM enrollments
            WHERE student_id=$1
              AND status='confirmed'
              AND class_date > NOW()
            ORDER BY class_date
        """, tid)
        return [dict(r) for r in rows]


async def db_count_enrolled(slot_key: str, class_date: datetime) -> int:
    async with pool.acquire() as con:
        r = await con.fetchrow("""
            SELECT COUNT(*) as c FROM enrollments
            WHERE slot_key=$1 AND class_date=$2 AND status='confirmed'
        """, slot_key, class_date)
        return r["c"]


async def db_enrollments_for_slot(slot_key: str, class_date: datetime) -> list:
    async with pool.acquire() as con:
        rows = await con.fetch("""
            SELECT e.*, s.first_name, s.username, s.sub_type
            FROM enrollments e
            JOIN students s ON s.telegram_id = e.student_id
            WHERE e.slot_key=$1 AND e.class_date=$2 AND e.status='confirmed'
        """, slot_key, class_date)
        return [dict(r) for r in rows]


async def db_pending_day_reminder(class_date: datetime) -> list:
    async with pool.acquire() as con:
        rows = await con.fetch("""
            SELECT * FROM enrollments
            WHERE class_date=$1 AND status='confirmed' AND day_reminder_sent=FALSE
        """, class_date)
        return [dict(r) for r in rows]


async def db_mark_day_reminder(eid: int):
    async with pool.acquire() as con:
        await con.execute("UPDATE enrollments SET day_reminder_sent=TRUE WHERE id=$1", eid)


async def db_pending_hour_reminder(class_date: datetime) -> list:
    async with pool.acquire() as con:
        rows = await con.fetch("""
            SELECT * FROM enrollments
            WHERE class_date=$1 AND status='confirmed' AND hour_reminder_sent=FALSE
        """, class_date)
        return [dict(r) for r in rows]


async def db_mark_hour_reminder(eid: int):
    async with pool.acquire() as con:
        await con.execute("UPDATE enrollments SET hour_reminder_sent=TRUE WHERE id=$1", eid)


async def db_expiring_soon() -> list:
    target = datetime.now().date() + timedelta(days=SUB_WARN_DAYS)
    async with pool.acquire() as con:
        rows = await con.fetch(
            "SELECT * FROM students WHERE sub_expires=$1 AND sub_status='active'",
            target
        )
        return [dict(r) for r in rows]

# ══════════════════════════════════════════════════════════════════════════════
#  ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ══════════════════════════════════════════════════════════════════════════════

def _get_weeks(n: int = 4) -> list[dict]:
    today = datetime.now().date()
    days_ahead = (7 - today.weekday()) % 7 or 7
    monday = today + timedelta(days=days_ahead)
    weeks = []
    for i in range(n):
        ws = monday + timedelta(weeks=i)
        we = ws + timedelta(days=6)
        weeks.append({
            "num": i + 1,
            "monday": ws,
            "label": f"{i+1} неделя ({ws.strftime('%d.%m')}–{we.strftime('%d.%m')})",
        })
    return weeks


def _same_week(d: date, monday: date) -> bool:
    return monday <= d <= monday + timedelta(days=6)


def _auto_book_dates(slot_keys: list, classes: int) -> list[tuple]:
    weeks = _get_weeks(4)
    result = []
    count = 0
    for w in weeks:
        monday = w["monday"]
        for sk in slot_keys:
            if count >= classes:
                break
            slot = SLOTS[sk]
            days_ahead = (slot["weekday"] - monday.weekday()) % 7
            d = monday + timedelta(days=days_ahead)
            dt = datetime(d.year, d.month, d.day, slot["hour"], slot["minute"])
            result.append((sk, dt))
            count += 1
        if count >= classes:
            break
    return result


async def _get_single_dates(slot_keys: list, weeks_ahead: int = 2) -> list:
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    result = []
    for i in range(1, weeks_ahead * 7 + 1):
        day = today + timedelta(days=i)
        for sk in slot_keys:
            slot = SLOTS[sk]
            if day.weekday() == slot["weekday"]:
                dt = day.replace(hour=slot["hour"], minute=slot["minute"])
                enrolled = await db_count_enrolled(sk, dt)
                free = max(0, MAX_STUDENTS - enrolled)
                result.append({"slot_key": sk, "date": dt, "free": free, "enrolled": enrolled})
    result.sort(key=lambda x: x["date"])
    return result


def _group_label(group: str) -> str:
    return {
        "morning": "🌞 Утренняя",
        "evening": "🌙 Вечерняя",
        "mixed": "💫 Смешанная",
        "single": "👀 Разовое занятие",
    }.get(group, group)


def _format_book_lines(to_book: list) -> list[str]:
    lines = []
    for sk, dt in to_book:
        slot = SLOTS[sk]
        wd = WEEKDAY_RU[dt.weekday()]
        emoji = "🌞" if slot["time_type"] == "morning" else "🌙"
        lines.append(f"{emoji} {wd}, {dt.strftime('%d.%m.%Y')} — {slot['label']}")
    return lines


def _cal_text(week: dict, per_week: int, selected: list, available_slots: list) -> str:
    monday = week["monday"] if isinstance(week["monday"], date) else date.fromisoformat(week["monday"])
    cnt = sum(
        1 for s in selected
        if s.split("|")[0] in available_slots
        and _same_week(datetime.fromtimestamp(int(s.split("|")[1])).date(), monday)
    )
    return (
        f"📅 <b>{week['label']}</b>\n\n"
        f"Выберите <b>{per_week} занятия</b> на эту неделю.\n"
        f"Выбрано: <b>{cnt}/{per_week}</b>\n"
        f"Всего выбрано: <b>{len(selected)}</b>\n\n"
        f"<i>Нажмите на день чтобы выбрать/снять.</i>"
    )


async def _check_reg(cb: CallbackQuery) -> bool:
    s = await db_get_student(cb.from_user.id)
    if not s or not s["first_name"]:
        await cb.message.edit_text("🪬 Пожалуйста, сначала зарегистрируйтесь — нажмите /start")
        await cb.answer()
        return False
    return True

# ══════════════════════════════════════════════════════════════════════════════
#  КЛАВИАТУРЫ
# ══════════════════════════════════════════════════════════════════════════════

def kb_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🫂 Абонемент", callback_data="menu_sub")],
        [InlineKeyboardButton(text="👀 Разовое занятие", callback_data="menu_single")],
        [InlineKeyboardButton(text="🕊 Личный кабинет", callback_data="cabinet")],
        [InlineKeyboardButton(text="📍 Где студия", callback_data="menu_location")],
        [InlineKeyboardButton(text="💬 Частные вопросы", callback_data="menu_private")],
    ])


def kb_back_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=" Главное меню", callback_data="back_main")]
    ])


def kb_after_booking() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🕊 Личный кабинет", callback_data="cabinet")],
        [InlineKeyboardButton(text=" Главное меню", callback_data="back_main")],
    ])


def kb_sub_groups() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🌞 Утренняя группа", callback_data="sub_group:morning")],
        [InlineKeyboardButton(text="🌙 Вечерняя группа", callback_data="sub_group:evening")],
        [InlineKeyboardButton(text="💫 Смешанные занятия", callback_data="sub_group:mixed")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back_main")],
    ])


def kb_morning_classes() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="12 занятий — 2.200.000 VND", callback_data="sub_pick:morning:12")],
        [InlineKeyboardButton(text="8 занятий  — 1.600.000 VND", callback_data="sub_pick:morning:8")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu_sub")],
    ])


def kb_evening_classes() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="8 занятий — 1.600.000 VND", callback_data="sub_pick:evening:8")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu_sub")],
    ])


def kb_mixed_classes() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="12 занятий — 2.200.000 VND", callback_data="sub_pick:mixed:12")],
        [InlineKeyboardButton(text="8 занятий  — 1.600.000 VND", callback_data="sub_pick:mixed:8")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu_sub")],
    ])


def kb_single_groups() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🌞 Утренняя практика (Вт/Чт/Сб 09:00)", callback_data="single:morning")],
        [InlineKeyboardButton(text="🌙 Вечерняя практика (Ср/Пт 19:00)", callback_data="single:evening")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back_main")],
    ])


def kb_single_dates(dates: list) -> InlineKeyboardMarkup:
    buttons = []
    for d in dates:
        slot = SLOTS[d["slot_key"]]
        ts = int(d["date"].timestamp())
        wd = WEEKDAY_RU[d["date"].weekday()]
        lbl = f" {wd} {d['date'].strftime('%d.%m')} — {slot['label']}"
        buttons.append([InlineKeyboardButton(
            text=lbl,
            callback_data=f"single_date:{d['slot_key']}:{ts}"
        )])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="menu_single")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def kb_attend(slot_key: str, ts: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✔️ Приду!", callback_data=f"attend_yes:{slot_key}:{ts}"),
        InlineKeyboardButton(text="✖️ Не приду", callback_data=f"attend_no:{slot_key}:{ts}"),
    ]])


def kb_cancel_list(enrollments: list) -> InlineKeyboardMarkup:
    buttons = []
    for e in enrollments:
        slot = SLOTS.get(e["slot_key"], {})
        ts = int(e["class_date"].timestamp())
        wd = WEEKDAY_RU[e["class_date"].weekday()]
        buttons.append([InlineKeyboardButton(
            text=f"✖️ {wd} {e['class_date'].strftime('%d.%m.%Y')} — {slot.get('label','')}",
            callback_data=f"do_cancel:{e['slot_key']}:{ts}",
        )])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="cabinet")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def kb_admin_request(req_id: int, tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✔️ Подтвердить", callback_data=f"admin_approve:{req_id}:{tid}"),
        InlineKeyboardButton(text="✖️ Отклонить", callback_data=f"admin_reject:{req_id}:{tid}"),
    ]])


def kb_admin() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика", callback_data="adm:stats")],
        [InlineKeyboardButton(text="📋 Список на сегодня", callback_data="adm:today")],
        [InlineKeyboardButton(text="🫂 Все студенты", callback_data="adm:students")],
        [InlineKeyboardButton(text="➕ Добавить текущего студента", callback_data="adm:add_current_student")],
        [InlineKeyboardButton(text="🗑 Удалить пользователя", callback_data="adm:delete_user")],
        [InlineKeyboardButton(text="⏳ Заявки", callback_data="adm:requests")],
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="adm:broadcast")],
    ])


def kb_admin_group_pick() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🌞 Утренняя", callback_data="adm_group:morning")],
        [InlineKeyboardButton(text="🌙 Вечерняя", callback_data="adm_group:evening")],
        [InlineKeyboardButton(text="💫 Смешанная", callback_data="adm_group:mixed")],
        [InlineKeyboardButton(text=" Отмена", callback_data="adm:back")],
    ])


def kb_admin_subtype_pick(group: str) -> InlineKeyboardMarkup:
    rows = []
    if group == "morning":
        rows = [
            [InlineKeyboardButton(text="morning_8", callback_data="adm_subtype:morning_8")],
            [InlineKeyboardButton(text="morning_12", callback_data="adm_subtype:morning_12")],
        ]
    elif group == "evening":
        rows = [
            [InlineKeyboardButton(text="evening_8", callback_data="adm_subtype:evening_8")],
        ]
    elif group == "mixed":
        rows = [
            [InlineKeyboardButton(text="mixed_8", callback_data="adm_subtype:mixed_8")],
            [InlineKeyboardButton(text="mixed_12", callback_data="adm_subtype:mixed_12")],
        ]
    rows.append([InlineKeyboardButton(text=" Отмена", callback_data="adm:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_calendar_week(
    week: dict,
    available_slots: list,
    selected: list,
    week_idx: int,
    per_week: int,
    total_weeks: int = 4,
) -> InlineKeyboardMarkup:
    buttons = []
    week_selected = sum(
        1 for s in selected
        if s.split("|")[0] in available_slots
        and _same_week(datetime.fromtimestamp(int(s.split("|")[1])).date(), week["monday"])
    )

    for sk in available_slots:
        slot = SLOTS[sk]
        days_ahead = (slot["weekday"] - week["monday"].weekday()) % 7
        d = week["monday"] + timedelta(days=days_ahead)
        dt = datetime(d.year, d.month, d.day, slot["hour"], slot["minute"])
        ts = int(dt.timestamp())
        key = f"{sk}|{ts}"
        wd = WEEKDAY_RU[d.weekday()]

        if key in selected:
            label = f"✔️ {wd} {d.strftime('%d.%m')} {slot['label']}"
            cb = f"cal_toggle:{key}"
        else:
            label = f"☐ {wd} {d.strftime('%d.%m')} {slot['label']}"
            cb = f"cal_toggle:{key}" if week_selected < per_week else "cal_limit"
        buttons.append([InlineKeyboardButton(text=label, callback_data=cb)])

    nav = []
    if week_idx > 0:
        nav.append(InlineKeyboardButton(text="◀️ Пред", callback_data=f"cal_week:{week_idx-1}"))
    if week_idx < total_weeks - 1:
        nav.append(InlineKeyboardButton(text="След ▶️", callback_data=f"cal_week:{week_idx+1}"))
    if nav:
        buttons.append(nav)

    buttons.append([InlineKeyboardButton(text=" Готово", callback_data="cal_done")])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="menu_sub")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# ══════════════════════════════════════════════════════════════════════════════
#  FSM
# ══════════════════════════════════════════════════════════════════════════════

class RegState(StatesGroup):
    name = State()
    phone = State()


class CalendarState(StatesGroup):
    picking = State()


class SingleState(StatesGroup):
    confirm = State()


class AdminAddStudentState(StatesGroup):
    telegram_id = State()
    first_name = State()
    phone = State()
    classes_total = State()
    classes_left = State()
    expires_date = State()


class AdminDeleteStudentState(StatesGroup):
    telegram_id = State()

# ══════════════════════════════════════════════════════════════════════════════
#  ЗАЩИТА ОТ ДВОЙНОГО НАЖАТИЯ
# ══════════════════════════════════════════════════════════════════════════════

_processing: set[int] = set()

def lock(uid: int) -> bool:
    if uid in _processing:
        return False
    _processing.add(uid)
    return True

def unlock(uid: int):
    _processing.discard(uid)

# ══════════════════════════════════════════════════════════════════════════════
#  ХЭНДЛЕРЫ
# ══════════════════════════════════════════════════════════════════════════════

router = Router()
_broadcast_mode: set[int] = set()

# ── /start ────────────────────────────────────────────────────────────────────

@router.message(CommandStart())
async def cmd_start(msg: Message, state: FSMContext):
    await state.clear()
    student = await db_get_student(msg.from_user.id)

    if student and student["first_name"]:
        await msg.answer(
            f" С возвращением, <b>{student['first_name']}</b>!\n\n"
            "🌿 Здравствуйте! Я рада что вы выбираете себя и выбираете практиковать йогу!\n\n"
            "Выберите вариант занятий:",
            reply_markup=kb_main(),
        )
    else:
        await msg.answer(
            "🌿 Здравствуйте! Я рада что вы выбираете себя и выбираете практиковать йогу!\n\n"
            "Для начала давайте познакомимся.\n"
            "Как вас зовут? Напишите своё <b>имя</b>:"
        )
        await state.set_state(RegState.name)


@router.message(RegState.name)
async def reg_name(msg: Message, state: FSMContext):
    name = msg.text.strip()
    if not 2 <= len(name) <= 50:
        await msg.answer("👀 Имя от 2 до 50 символов. Попробуй ещё раз:")
        return
    await state.update_data(first_name=name)
    await msg.answer(
        f"Приятно познакомиться, <b>{name}</b>! 🫂\n\n"
        "Напишите свой <b>номер телефона</b>:",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="📱 Поделиться номером", request_contact=True)]],
            resize_keyboard=True, one_time_keyboard=True,
        )
    )
    await state.set_state(RegState.phone)


@router.message(RegState.phone, F.contact)
async def reg_phone_contact(msg: Message, state: FSMContext):
    await _finish_reg(msg, state, msg.contact.phone_number)


@router.message(RegState.phone, F.text)
async def reg_phone_text(msg: Message, state: FSMContext):
    phone = msg.text.strip().replace(" ", "").replace("-", "")
    if len(phone) < 8:
        await msg.answer(" Введите корректный номер:")
        return
    await _finish_reg(msg, state, phone)


async def _finish_reg(msg: Message, state: FSMContext, phone: str):
    data = await state.get_data()
    name = data["first_name"]
    await db_create_student(msg.from_user.id, msg.from_user.username or "", name, phone)
    await state.clear()
    await msg.answer(
        f" <b>Регистрация завершена!</b>\n\n"
        f"🧘🏻‍♀️ Имя: {name}\n📱 Телефон: {phone}\n\n"
        "Теперь выберите вариант занятий:",
        reply_markup=ReplyKeyboardRemove(),
    )
    await msg.answer("Выбери действие:", reply_markup=kb_main())

# ── Главное меню ──────────────────────────────────────────────────────────────

@router.callback_query(F.data == "back_main")
async def back_main(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    s = await db_get_student(cb.from_user.id)
    name = s["first_name"] if s else "друг"
    await cb.message.edit_text(
        f"Здравствуй, <b>{name}</b>! 🧘🏻‍♀️\nВыберите вариант занятий:",
        reply_markup=kb_main(),
    )
    await cb.answer()


@router.callback_query(F.data == "menu_location")
async def menu_location(cb: CallbackQuery):
    await cb.message.edit_text(
        f"📍 <b>Где студия</b>\n\n  {STUDIO_ADDR}\n\n"
        f"<a href='{STUDIO_MAP}'> Открыть на Google Maps</a>",
        reply_markup=kb_back_main(),
    )
    await cb.answer()


@router.callback_query(F.data == "menu_private")
async def menu_private(cb: CallbackQuery):
    await cb.message.edit_text(
        "💬 <b>Частные вопросы</b>\n\nПо всем вопросам обращайтесь к администратору. 🫂",
        reply_markup=kb_back_main(),
    )
    await cb.answer()

# ── Личный кабинет ────────────────────────────────────────────────────────────

@router.callback_query(F.data == "cabinet")
async def cabinet(cb: CallbackQuery):
    await db_complete_past_enrollments()
    s = await db_get_student(cb.from_user.id)

    if not s or not s["first_name"]:
        await cb.message.edit_text(
            "👀 Сначала зарегистрируйтесь — нажмите /start",
            reply_markup=kb_back_main()
        )
        await cb.answer()
        return

    enrs = await db_future_enrollments(cb.from_user.id)
    left = s.get("classes_left") or 0

    status_map = {
        "active": "✔️ Активен",
        "pending": " Ожидает подтверждения",
        "expired": "✖️ Истёк",
        "none": "—",
    }
    sub_status = status_map.get(s.get("sub_status") or "none", "—")
    group_map  = {"morning": "🌞 Утренняя", "evening": "🌙 Вечерняя", "mixed": "💫 Смешанная", "single": "👀 Разовое"}
    group_name = group_map.get(s.get("group_type") or "", "—")

    text = (
        f"🕊 <b>Личный кабинет</b>\n\n"
        f"🫂 <b>{s['first_name']}</b>\n"
        f"📱 {s.get('phone') or '—'}\n"
        f"🟤 @{s.get('username') or '—'}\n\n"
        f"💳 Абонемент: {sub_status}\n"
        f"🧘🏻‍♀️ Группа: {group_name}\n"
        f"🖇 Тип: {s.get('sub_type') or '—'}\n"
        f"🔔 Осталось занятий: <b>{left}</b>\n"
        f"🌀 До: {s['sub_expires'].strftime('%d.%m.%Y') if s.get('sub_expires') else '—'}\n\n"
    )

    if enrs:
        text += " <b>Предстоящие занятия:</b>\n\n"
        for e in enrs:
            slot = SLOTS.get(e["slot_key"], {})
            wd = WEEKDAY_RU[e["class_date"].weekday()]
            emoji = "🌞" if slot.get("time_type") == "morning" else "🌙"
            text += f"{emoji} <b>{wd}, {e['class_date'].strftime('%d.%m.%Y')}</b> — {slot.get('label','')}\n"
    else:
        text += "Предстоящих занятий нет."

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✖️ Отменить запись", callback_data="cancel_class")],
        [InlineKeyboardButton(text="✔️Продлить абонемент", callback_data="menu_sub")],
        [InlineKeyboardButton(text="🕊 Главное меню", callback_data="back_main")],
    ])
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()

# ── Абонемент ─────────────────────────────────────────────────────────────────

@router.callback_query(F.data == "menu_sub")
async def menu_sub(cb: CallbackQuery):
    if not await _check_reg(cb):
        return
    await cb.message.edit_text("💳 <b>Абонемент</b>\n\nВыберите группу:", reply_markup=kb_sub_groups())
    await cb.answer()


@router.callback_query(F.data == "sub_group:morning")
async def sub_morning(cb: CallbackQuery):
    await cb.message.edit_text(
        "🌞 <b>Утренняя группа</b>\n\n"
        "3 раза в неделю — Вт / Чт / Сб с <b>09:00 до 10:15</b>\n\n"
        "При выборе 12 занятий — все 3 дня автоматически.\n"
        "При выборе 8 занятий — вы выбираете 2 из 3 дней на каждую неделю.\n\n"
        "Выберите количество занятий:",
        reply_markup=kb_morning_classes(),
    )
    await cb.answer()


@router.callback_query(F.data == "sub_group:evening")
async def sub_evening(cb: CallbackQuery):
    await cb.message.edit_text(
        "🌙 <b>Вечерняя группа</b>\n\n"
        "2 раза в неделю — Ср / Пт с <b>19:00 до 20:15</b>\n\n"
        "8 занятий (4 недели) — Ср и Пт бронируются автоматически.\n\n"
        " Стоимость: <b>1.600.000 VND</b>",
        reply_markup=kb_evening_classes(),
    )
    await cb.answer()


@router.callback_query(F.data == "sub_group:mixed")
async def sub_mixed(cb: CallbackQuery):
    await cb.message.edit_text(
        "💫 <b>Смешанные занятия</b>\n\n"
        "Любые дни из доступных:\n"
        " Утро: Вт / Чт / Сб — 09:00\n"
        " Вечер: Ср / Пт — 19:00\n\n"
        "Выберите количество занятий:",
        reply_markup=kb_mixed_classes(),
    )
    await cb.answer()


@router.callback_query(F.data.startswith("sub_pick:"))
async def sub_pick(cb: CallbackQuery, state: FSMContext):
    _, group, n_str = cb.data.split(":")
    classes = int(n_str)
    rule = RULES[(group, classes)]

    await state.update_data(group=group, classes=classes, rule=rule)

    if rule["calendar"]:
        weeks = _get_weeks(4)
        await state.update_data(
            weeks=[{"num": w["num"], "monday": w["monday"].isoformat(), "label": w["label"]} for w in weeks],
            selected=[],
            current_week=0,
        )
        await state.set_state(CalendarState.picking)
        week = weeks[0]
        await cb.message.edit_text(
            _cal_text(week, rule["per_week"], [], rule["slots"]),
            reply_markup=kb_calendar_week(week, rule["slots"], [], 0, rule["per_week"]),
        )
    else:
        to_book = _auto_book_dates(rule["slots"], classes)
        lines = _format_book_lines(to_book)
        price = "1.600.000" if classes == 8 else "2.200.000"
        g_label = _group_label(group)

        await cb.message.edit_text(
            f"🖇 <b>Подтвердите выбор</b>\n\n"
            f"{g_label} — <b>{classes} занятий</b> ({price} VND)\n\n"
            f"<b>Занятия будут забронированы:</b>\n" + "\n".join(lines) +
            "\n\n<i>После подтверждения заявка уйдёт администратору. "
            "Занятия начислятся после оплаты.</i>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✔️ Отправить заявку", callback_data="sub_send_request")],
                [InlineKeyboardButton(text="◀️ Назад", callback_data="menu_sub")],
            ]),
        )
    await cb.answer()

# ── Календарь выбора дней ─────────────────────────────────────────────────────

@router.callback_query(F.data == "cal_limit")
async def cal_limit(cb: CallbackQuery):
    await cb.answer("🛎 На эту неделю уже выбрано максимум!", show_alert=True)


@router.callback_query(CalendarState.picking, F.data.startswith("cal_week:"))
async def cal_week(cb: CallbackQuery, state: FSMContext):
    week_idx = int(cb.data.split(":")[1])
    data = await state.get_data()
    rule = data["rule"]
    weeks = data["weeks"]
    selected = data.get("selected", [])
    week_raw = weeks[week_idx]
    week = {**week_raw, "monday": date.fromisoformat(week_raw["monday"])}
    await state.update_data(current_week=week_idx)
    await cb.message.edit_text(
        _cal_text(week, rule["per_week"], selected, rule["slots"]),
        reply_markup=kb_calendar_week(week, rule["slots"], selected, week_idx, rule["per_week"]),
    )
    await cb.answer()


@router.callback_query(CalendarState.picking, F.data.startswith("cal_toggle:"))
async def cal_toggle(cb: CallbackQuery, state: FSMContext):
    key = cb.data.split(":", 1)[1]
    data = await state.get_data()
    rule = data["rule"]
    selected = list(data.get("selected", []))
    weeks = data["weeks"]
    week_idx = data.get("current_week", 0)
    week_raw = weeks[week_idx]
    week = {**week_raw, "monday": date.fromisoformat(week_raw["monday"])}

    if key in selected:
        selected.remove(key)
    else:
        selected.append(key)

    await state.update_data(selected=selected)
    await cb.message.edit_text(
        _cal_text(week, rule["per_week"], selected, rule["slots"]),
        reply_markup=kb_calendar_week(week, rule["slots"], selected, week_idx, rule["per_week"]),
    )
    await cb.answer()


@router.callback_query(CalendarState.picking, F.data == "cal_done")
async def cal_done(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    rule = data["rule"]
    selected = data.get("selected", [])
    weeks = data["weeks"]
    per_week = rule["per_week"]

    for w in weeks:
        monday = date.fromisoformat(w["monday"])
        cnt = sum(
            1 for s in selected
            if s.split("|")[0] in rule["slots"]
            and _same_week(datetime.fromtimestamp(int(s.split("|")[1])).date(), monday)
        )
        if cnt < per_week:
            await cb.answer(f" Неделя {w['num']}: выбрано {cnt} из {per_week}!", show_alert=True)
            return

    group = data["group"]
    classes = data["classes"]
    price = "1.600.000" if classes == 8 else "2.200.000"
    g_label = _group_label(group)

    parsed = sorted(
        [(sk, datetime.fromtimestamp(int(ts))) for item in selected for sk, ts in [item.split("|")]],
        key=lambda x: x[1]
    )
    lines = _format_book_lines(parsed)

    await cb.message.edit_text(
        f" <b>Подтвердите выбор</b>\n\n"
        f"{g_label} — <b>{classes} занятий</b> ({price} VND)\n\n"
        f"<b>Выбранные занятия:</b>\n" + "\n".join(lines) +
        "\n\n<i>После подтверждения заявка уйдёт администратору. "
        "Занятия начислятся после оплаты.</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✔️ Отправить заявку", callback_data="sub_send_request")],
            [InlineKeyboardButton(text="🌀 Изменить", callback_data="cal_back")],
        ]),
    )
    await cb.answer()


@router.callback_query(F.data == "cal_back")
async def cal_back(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    rule = data["rule"]
    weeks = data["weeks"]
    selected = data.get("selected", [])
    week_idx = data.get("current_week", 0)
    week_raw = weeks[week_idx]
    week = {**week_raw, "monday": date.fromisoformat(week_raw["monday"])}
    await state.set_state(CalendarState.picking)
    await cb.message.edit_text(
        _cal_text(week, rule["per_week"], selected, rule["slots"]),
        reply_markup=kb_calendar_week(week, rule["slots"], selected, week_idx, rule["per_week"]),
    )
    await cb.answer()


@router.callback_query(F.data == "sub_send_request")
async def sub_send_request(cb: CallbackQuery, state: FSMContext):
    if not lock(cb.from_user.id):
        await cb.answer("⏳ Подождите...", show_alert=True)
        return
    try:
        data = await state.get_data()
        group = data.get("group", "morning")
        classes = data.get("classes", 8)
        rule = data.get("rule", RULES.get((group, classes), {}))
        selected = data.get("selected", [])

        if selected:
            to_book = sorted(
                [(sk, datetime.fromtimestamp(int(ts))) for item in selected for sk, ts in [item.split("|")]],
                key=lambda x: x[1]
            )
        else:
            to_book = _auto_book_dates(rule.get("slots", MORNING_SLOTS), classes)

        selected_json = json.dumps([(sk, dt.isoformat()) for sk, dt in to_book])

        sub_key = f"{group}_{classes}"
        req_id = await db_create_request(cb.from_user.id, group, sub_key, classes, selected_json)

        async with pool.acquire() as con:
            await con.execute(
                "UPDATE students SET sub_status='pending' WHERE telegram_id=$1",
                cb.from_user.id
            )

        await state.clear()

        s = await db_get_student(cb.from_user.id)
        g_label = _group_label(group)
        price = "1.200.000" if classes == 8 else "1.800.000"
        lines = _format_book_lines(to_book)

        try:
            await cb.bot.send_message(
                ADMIN_CHANNEL_ID,
                f"🆕 <b>Новая заявка на абонемент!</b>\n\n"
                f"🫂 {s['first_name']} (@{s.get('username') or '—'})\n"
                f"📱 {s.get('phone') or '—'}\n\n"
                f"{g_label} — <b>{classes} занятий</b> ({price} VND)\n\n"
                f"<b>Занятия:</b>\n" + "\n".join(lines) +
                f"\n\n Заявка #{req_id}",
                reply_markup=kb_admin_request(req_id, cb.from_user.id),
            )
        except Exception as e:
            log.warning(f"Не удалось отправить в канал: {e}")
            for admin_id in ADMIN_IDS:
                try:
                    await cb.bot.send_message(
                        admin_id,
                        f"🆕 <b>Новая заявка #{req_id}</b>\n\n"
                        f"🫂 {s['first_name']} (@{s.get('username') or '—'})\n"
                        f"{g_label} — {classes} зан.\n" + "\n".join(lines),
                        reply_markup=kb_admin_request(req_id, cb.from_user.id),
                    )
                except Exception:
                    pass

        await cb.message.edit_text(
            f"✔️ <b>Заявка отправлена!</b>\n\n"
            f"{g_label} — <b>{classes} занятий</b>\n\n"
            "Администратор получил уведомление.\n"
            "После подтверждения оплаты занятия будут активированы и вы получите сообщение. 💌",
            reply_markup=kb_after_booking(),
        )
        await cb.answer()
    finally:
        unlock(cb.from_user.id)

# ── Подтверждение/отклонение от админа ───────────────────────────────────────

@router.callback_query(F.data.startswith("admin_approve:"))
async def admin_approve(cb: CallbackQuery):
    _, req_id_str, tid_str = cb.data.split(":")
    req_id = int(req_id_str)
    tid = int(tid_str)

    req = await db_get_request(req_id)
    if not req:
        await cb.answer("✖️ Заявка не найдена", show_alert=True)
        return
    if req["status"] != "pending":
        await cb.answer("👀 Заявка уже обработана", show_alert=True)
        return

    to_book = [(sk, datetime.fromisoformat(dt_str)) for sk, dt_str in json.loads(req["selected_days"])]

    await db_activate_subscription(tid, req["group_type"], req["sub_key"], req["classes"])

    booked = 0
    for sk, dt in to_book:
        count = await db_count_enrolled(sk, dt)
        if count < MAX_STUDENTS:
            await db_enroll(tid, sk, dt)
            booked += 1
        else:
            await db_increment_classes(tid)

    await db_resolve_request(req_id, "approved")

    g_label = _group_label(req["group_type"])
    try:
        await cb.bot.send_message(
            tid,
            f"🪬 <b>Абонемент активирован!</b>\n\n"
            f"{g_label} — <b>{req['classes']} занятий</b>\n"
            f" Забронировано занятий: <b>{booked}</b>\n\n"
            f"Жду вас в студии:\n  {STUDIO_ADDR}\n"
            f"<a href='{STUDIO_MAP}'> 📌 Google Maps</a>\n\n"
            "До встречи на ковре! ",
            reply_markup=kb_after_booking(),
        )
    except Exception as e:
        log.warning(f"Уведомление студенту {tid}: {e}")

    await cb.message.edit_text(
        cb.message.text + f"\n\n✔️ <b>Подтверждено</b> (@{cb.from_user.username})",
    )
    await cb.answer("✔️ Абонемент активирован!")


@router.callback_query(F.data.startswith("admin_reject:"))
async def admin_reject(cb: CallbackQuery):
    _, req_id_str, tid_str = cb.data.split(":")
    req_id = int(req_id_str)
    tid = int(tid_str)

    req = await db_get_request(req_id)
    if not req:
        await cb.answer("✖️ Заявка не найдена", show_alert=True)
        return
    if req["status"] != "pending":
        await cb.answer("🛎 Заявка уже обработана", show_alert=True)
        return

    await db_resolve_request(req_id, "rejected")
    async with pool.acquire() as con:
        await con.execute("UPDATE students SET sub_status='none' WHERE telegram_id=$1", tid)

    try:
        await cb.bot.send_message(
            tid,
            "✖️ <b>Заявка отклонена.</b>\n\n"
            "К сожалению, ваша заявка была отклонена администратором.\n"
            "Если есть вопросы — свяжитесь с нами. 🫂",
            reply_markup=kb_main(),
        )
    except Exception:
        pass

    await cb.message.edit_text(
        cb.message.text + f"\n\n✖️ <b>Отклонено</b> (@{cb.from_user.username})",
    )
    await cb.answer("✖️ Заявка отклонена")

# ── Разовое занятие ───────────────────────────────────────────────────────────

@router.callback_query(F.data == "menu_single")
async def menu_single(cb: CallbackQuery):
    if not await _check_reg(cb):
        return
    await cb.message.edit_text(
        "👀 <b>Разовое занятие</b>\n\n"
        "🌞 Утро: Вт / Чт / Сб — 09:00–10:15\n"
        "🌙 Вечер: Ср / Пт — 19:00–20:15\n\n"
        "Выберите группу:",
        reply_markup=kb_single_groups(),
    )
    await cb.answer()


@router.callback_query(F.data.startswith("single:"))
async def single_group(cb: CallbackQuery):
    group_type = cb.data.split(":")[1]
    slot_keys = MORNING_SLOTS if group_type == "morning" else EVENING_SLOTS
    dates = await _get_single_dates(slot_keys, weeks_ahead=2)

    if not dates:
        await cb.message.edit_text(
            "🧿 Свободных мест в ближайшие 2 недели нет.",
            reply_markup=kb_back_main(),
        )
        await cb.answer()
        return

    emoji = "🌞" if group_type == "morning" else "🌙"
    name = "Утренняя" if group_type == "morning" else "Вечерняя"
    await cb.message.edit_text(
        f"{emoji} <b>{name} практика</b>\n\nВыберите дату занятия:",
        reply_markup=kb_single_dates(dates),
    )
    await cb.answer()


@router.callback_query(F.data.startswith("single_date:"))
async def single_date(cb: CallbackQuery, state: FSMContext):
    _, sk, ts = cb.data.split(":")
    class_dt = datetime.fromtimestamp(int(ts))
    slot = SLOTS[sk]
    wd = WEEKDAY_RU[class_dt.weekday()]

    enrolled = await db_count_enrolled(sk, class_dt)
    free = MAX_STUDENTS - enrolled
    emoji = "🌞" if slot["time_type"] == "morning" else "🌙"

    if free <= 0:
        await cb.answer("✖️ Мест нет! Пожалуйста, выберите другую дату.", show_alert=True)
        return

    await state.update_data(slot_key=sk, ts=ts)
    await cb.message.edit_text(
        f"✔️ <b>Подтвердите выбор</b>\n\n"
        f"{emoji} <b>{slot['label']}</b>\n"
        f" {wd}, {class_dt.strftime('%d.%m.%Y')}\n\n"
        f"🫂 Занято мест: <b>{enrolled}</b> из {MAX_STUDENTS}\n"
        f"🧘🏻‍♀️ Свободно: <b>{free}</b>\n\n"
        "<i>Стоимость разового занятия уточняйте у администратора.</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✔️ Подтвердить", callback_data=f"single_confirm:{sk}:{ts}")],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="menu_single")],
        ]),
    )
    await cb.answer()


@router.callback_query(F.data.startswith("single_confirm:"))
async def single_confirm(cb: CallbackQuery, state: FSMContext):
    if not lock(cb.from_user.id):
        await cb.answer("⏳ Подождите...", show_alert=True)
        return
    try:
        _, sk, ts = cb.data.split(":")
        class_dt = datetime.fromtimestamp(int(ts))
        slot = SLOTS[sk]
        wd = WEEKDAY_RU[class_dt.weekday()]

        enrolled = await db_count_enrolled(sk, class_dt)
        if enrolled >= MAX_STUDENTS:
            await cb.answer("👀 Место только что заняли!", show_alert=True)
            return

        s = await db_get_student(cb.from_user.id)

        async with pool.acquire() as con:
            await con.execute("""
                UPDATE students SET
                    group_type='single', sub_type='single',
                    sub_status='active',
                    classes_total=COALESCE(classes_total,0)+1,
                    classes_left=COALESCE(classes_left,0)+1
                WHERE telegram_id=$1
            """, cb.from_user.id)

        await db_enroll(cb.from_user.id, sk, class_dt)
        await db_decrement_classes(cb.from_user.id)
        await state.clear()

        try:
            await cb.bot.send_message(
                ADMIN_CHANNEL_ID,
                f"👀 <b>Разовая запись!</b>\n\n"
                f"🫂 {s['first_name']} (@{s.get('username') or '—'})\n"
                f"📱 {s.get('phone') or '—'}\n\n"
                f"{'🌞' if slot['time_type']=='morning' else '🌙'} {slot['label']}\n"
                f" {wd}, {class_dt.strftime('%d.%m.%Y')}"
            )
        except Exception:
            for admin_id in ADMIN_IDS:
                try:
                    await cb.bot.send_message(
                        admin_id,
                        f"👀 Разовая запись!\n{s['first_name']} (@{s.get('username') or '—'})\n"
                        f"{slot['label']} {class_dt.strftime('%d.%m.%Y')}"
                    )
                except Exception:
                    pass

        await cb.message.edit_text(
            f"✔️ <b>Место забронировано!</b>\n\n"
            f"{'🌞' if slot['time_type']=='morning' else '🌙'} <b>{slot['label']}</b>\n"
            f" {wd}, {class_dt.strftime('%d.%m.%Y')}\n\n"
            f"Жду тебя в студии:\n  {STUDIO_ADDR}\n"
            f"<a href='{STUDIO_MAP}'>📌 Google Maps</a>\n\n"
            " До встречи на ковре! ",
            reply_markup=kb_after_booking(),
            disable_web_page_preview=False,
        )
        await cb.answer()
    finally:
        unlock(cb.from_user.id)

# ── Отмена записи ─────────────────────────────────────────────────────────────

@router.callback_query(F.data == "cancel_class")
async def cancel_class(cb: CallbackQuery):
    enrs = await db_future_enrollments(cb.from_user.id)
    if not enrs:
        await cb.message.edit_text("✖️ Нет активных записей.", reply_markup=kb_back_main())
        await cb.answer()
        return
    await cb.message.edit_text("✖️ Выберите занятие для отмены:", reply_markup=kb_cancel_list(enrs))
    await cb.answer()


@router.callback_query(F.data.startswith("do_cancel:"))
async def do_cancel(cb: CallbackQuery):
    _, sk, ts = cb.data.split(":")
    class_dt = datetime.fromtimestamp(int(ts))
    slot = SLOTS.get(sk, {})
    await db_cancel_enrollment(cb.from_user.id, sk, class_dt)
    await db_increment_classes(cb.from_user.id)
    wd = WEEKDAY_RU[class_dt.weekday()]
    await cb.message.edit_text(
        f"✔️ Запись отменена.\n"
        f"{slot.get('label','')} — {wd}, {class_dt.strftime('%d.%m.%Y')}\n"
        "Занятие возвращено на счёт.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🕊 Личный кабинет", callback_data="cabinet")],
            [InlineKeyboardButton(text=" 🪬 Главное меню", callback_data="back_main")],
        ]),
    )
    await cb.answer()

# ── Напоминания ───────────────────────────────────────────────────────────────

@router.callback_query(F.data.startswith("attend_yes:"))
async def attend_yes(cb: CallbackQuery):
    _, sk, ts = cb.data.split(":")
    slot = SLOTS.get(sk, {})
    await cb.message.edit_text(f"✔️ Отлично! Ждём вас!\n{slot.get('label','')} 🧘🏻‍♀️")
    await cb.answer("✔️")


@router.callback_query(F.data.startswith("attend_no:"))
async def attend_no(cb: CallbackQuery):
    _, sk, ts = cb.data.split(":")
    class_dt = datetime.fromtimestamp(int(ts))
    await db_cancel_enrollment(cb.from_user.id, sk, class_dt)
    await db_increment_classes(cb.from_user.id)
    await cb.message.edit_text("✖️ Понятно, занятие возвращено на счёт. До встречи! 🫂")
    await cb.answer()

# ── Администратор ─────────────────────────────────────────────────────────────

@router.message(Command("admin"))
async def cmd_admin(msg: Message):
    if msg.from_user.id not in ADMIN_IDS:
        await msg.answer("✖️ Нет доступа.")
        return
    await msg.answer("🔧 <b>Панель администратора</b>", reply_markup=kb_admin())


@router.callback_query(F.data == "adm:stats")
async def adm_stats(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        return
    students = await db_all_students()
    total = len(students)
    active = sum(1 for s in students if s.get("sub_status") == "active")
    pending = sum(1 for s in students if s.get("sub_status") == "pending")
    await cb.message.edit_text(
        f"📊 <b>Статистика</b>\n\n"
        f"🫂 Всего студентов: <b>{total}</b>\n"
        f"✔️ Активных абонементов: <b>{active}</b>\n"
        f"⏳ Ждут подтверждения: <b>{pending}</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="adm:back")]
        ]),
    )
    await cb.answer()


@router.callback_query(F.data == "adm:today")
async def adm_today(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        return
    today = datetime.now()
    wd = today.weekday()
    text = f"📋 <b>Сегодня ({today.strftime('%d.%m.%Y')})</b>\n\n"
    found = False
    for sk, slot in SLOTS.items():
        if slot["weekday"] == wd:
            found = True
            class_dt = today.replace(hour=slot["hour"], minute=slot["minute"], second=0, microsecond=0)
            recs = await db_enrollments_for_slot(sk, class_dt)
            emoji = "🌞" if slot["time_type"] == "morning" else "🌙"
            text += f"{emoji} <b>{slot['label']}</b> — {len(recs)}/{MAX_STUDENTS}\n"
            for i, r in enumerate(recs, 1):
                sub = "🌿" if r.get("sub_type") and r["sub_type"] != "single" else "👀"
                text += f"  {i}. {sub} {r['first_name']} (@{r.get('username') or '—'})\n"
            text += "\n"
    if not found:
        text += "Сегодня занятий нет."
    await cb.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="adm:back")]
        ]),
    )
    await cb.answer()


@router.callback_query(F.data == "adm:students")
async def adm_students(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        return
    students = await db_all_students()
    text = f"👥 <b>Студенты ({len(students)})</b>\n\n"
    for s in students[:25]:
        status = {"active": "✔️", "pending": "⏳", "expired": "✖️", "none": "—"}.get(s.get("sub_status", ""), "—")
        left = s.get("classes_left") or 0
        exp = s["sub_expires"].strftime("%d.%m") if s.get("sub_expires") else "—"
        g = {"morning": "🌞", "evening": "🌙", "mixed": "💫", "single": "👀"}.get(s.get("group_type", ""), "")
        text += f"{status} {g} {s['first_name']} | {s.get('phone','—')} | {left} зан. до {exp}\n"
    if len(students) > 25:
        text += f"\n<i>...ещё {len(students)-25}</i>"
    await cb.message.edit_text(
        text or "Студентов пока нет.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="adm:back")]
        ]),
    )
    await cb.answer()


@router.callback_query(F.data == "adm:requests")
async def adm_requests(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        return
    requests = await db_pending_requests()
    if not requests:
        await cb.message.edit_text(
            "✔️ Нет заявок, ожидающих подтверждения.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Назад", callback_data="adm:back")]
            ]),
        )
        await cb.answer()
        return

    text = f"⏳ <b>Заявки ({len(requests)})</b>\n\n"
    for r in requests:
        g_label = _group_label(r["group_type"])
        text += (
            f"#{r['id']} — {r['first_name']} (@{r.get('username') or '—'})\n"
            f"  {g_label} — {r['classes']} зан.\n\n"
        )
    await cb.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="adm:back")]
        ]),
    )
    await cb.answer()

# ── Админ: добавить текущего студента ────────────────────────────────────────

@router.callback_query(F.data == "adm:add_current_student")
async def adm_add_current_student(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id not in ADMIN_IDS:
        return
    await state.clear()
    await cb.message.edit_text(
        "➕ <b>Добавление текущего студента</b>\n\n"
        "Введите <b>Telegram ID</b> пользователя:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Отмена", callback_data="adm:back")]
        ]),
    )
    await state.set_state(AdminAddStudentState.telegram_id)
    await cb.answer()


@router.message(AdminAddStudentState.telegram_id)
async def adm_add_tid(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS:
        return

    value = msg.text.strip()
    if not value.isdigit():
        await msg.answer("🛎 Telegram ID должен быть числом. Введите ещё раз:")
        return

    await state.update_data(telegram_id=int(value))
    await msg.answer("Введите <b>имя</b> студента:")
    await state.set_state(AdminAddStudentState.first_name)


@router.message(AdminAddStudentState.first_name)
async def adm_add_name(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS:
        return

    name = msg.text.strip()
    if len(name) < 2:
        await msg.answer("🛎 Имя слишком короткое. Введите ещё раз:")
        return

    await state.update_data(first_name=name)
    await msg.answer("Введите <b>телефон</b> студента:")
    await state.set_state(AdminAddStudentState.phone)


@router.message(AdminAddStudentState.phone)
async def adm_add_phone(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS:
        return

    phone = msg.text.strip()
    await state.update_data(phone=phone)
    await msg.answer(
        "Выберите <b>тип группы</b>:",
        reply_markup=kb_admin_group_pick()
    )


@router.callback_query(F.data.startswith("adm_group:"))
async def adm_pick_group(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id not in ADMIN_IDS:
        return

    group = cb.data.split(":")[1]
    await state.update_data(group_type=group)

    await cb.message.edit_text(
        f"Выберите <b>тип абонемента</b> для группы <b>{group}</b>:",
        reply_markup=kb_admin_subtype_pick(group)
    )
    await cb.answer()


@router.callback_query(F.data.startswith("adm_subtype:"))
async def adm_pick_subtype(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id not in ADMIN_IDS:
        return

    subtype = cb.data.split(":")[1]
    await state.update_data(sub_type=subtype)

    await cb.message.edit_text("Введите <b>сколько занятий всего</b>:")
    await state.set_state(AdminAddStudentState.classes_total)
    await cb.answer()


@router.message(AdminAddStudentState.classes_total)
async def adm_add_total(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS:
        return

    value = msg.text.strip()
    if not value.isdigit():
        await msg.answer("🛎 Введите число. Сколько занятий всего?")
        return

    await state.update_data(classes_total=int(value))
    await msg.answer("Введите <b>сколько занятий осталось</b>:")
    await state.set_state(AdminAddStudentState.classes_left)


@router.message(AdminAddStudentState.classes_left)
async def adm_add_left(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS:
        return

    value = msg.text.strip()
    if not value.isdigit():
        await msg.answer("🛎 Введите число. Сколько занятий осталось?")
        return

    await state.update_data(classes_left=int(value))
    await msg.answer("Введите <b>дату окончания</b> в формате <code>ДД.ММ.ГГГГ</code>:")
    await state.set_state(AdminAddStudentState.expires_date)


@router.message(AdminAddStudentState.expires_date)
async def adm_add_expire(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS:
        return

    raw = msg.text.strip()
    try:
        expires = datetime.strptime(raw, "%d.%m.%Y").date()
    except ValueError:
        await msg.answer("🛎 Неверный формат даты. Используй <code>ДД.ММ.ГГГГ</code>")
        return

    data = await state.get_data()

    tid = data["telegram_id"]
    first_name = data["first_name"]
    phone = data["phone"]
    group_type = data["group_type"]
    sub_type = data["sub_type"]
    classes_total = data["classes_total"]
    classes_left = data["classes_left"]

    if classes_left > classes_total:
        await msg.answer("🛎 Осталось занятий не может быть больше общего количества.")
        return

    sub_start = expires - timedelta(days=30)

    await db_upsert_existing_student(
        tid=tid,
        first_name=first_name,
        phone=phone,
        group_type=group_type,
        sub_type=sub_type,
        classes_total=classes_total,
        classes_left=classes_left,
        sub_start=sub_start,
        sub_expires=expires,
    )

    await state.clear()

    await msg.answer(
        f"✔️ <b>Студент добавлен</b>\n\n"
        f"🫂 {first_name}\n"
        f"🖇 <code>{tid}</code>\n"
        f"📱 {phone}\n"
        f"🧘🏻‍♀️ Группа: {_group_label(group_type)}\n"
        f"💳 Абонемент: {sub_type}\n"
        f"🟤 Осталось: <b>{classes_left}</b> из <b>{classes_total}</b>\n"
        f"🧿 До: {expires.strftime('%d.%m.%Y')}",
        reply_markup=kb_admin()
    )

    try:
        await msg.bot.send_message(
            tid,
            f"🌿 <b>Ваш абонемент добавлен в систему</b>\n\n"
            f"🧘🏻‍♀️ Группа: {_group_label(group_type)}\n"
            f"💳 Тип абонемента: <b>{sub_type}</b>\n"
            f"🟤 Осталось занятий: <b>{classes_left}</b> из <b>{classes_total}</b>\n"
            f"🕊 Действует до: <b>{expires.strftime('%d.%m.%Y')}</b>\n\n"
            f"Теперь вы можете открыть личный кабинет в боте.",
            reply_markup=kb_main()
        )
    except Exception as e:
        log.warning(f"Не удалось отправить уведомление студенту {tid}: {e}")
        await msg.answer(
            "🛎 Студент добавлен в базу, но сообщение в Telegram не доставлено.\n"
            "Скорее всего, пользователь ещё не нажимал /start у бота."
        )

# ── Админ: удалить пользователя ───────────────────────────────────────────────

@router.callback_query(F.data == "adm:delete_user")
async def adm_delete_user(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id not in ADMIN_IDS:
        return
    await state.clear()
    await cb.message.edit_text(
        "🗑 <b>Удаление пользователя</b>\n\n"
        "Введите <b>Telegram ID</b> пользователя, которого нужно удалить:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Отмена", callback_data="adm:back")]
        ]),
    )
    await state.set_state(AdminDeleteStudentState.telegram_id)
    await cb.answer()


@router.message(AdminDeleteStudentState.telegram_id)
async def adm_delete_user_input(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS:
        return

    value = msg.text.strip()
    if not value.isdigit():
        await msg.answer("🛎 Telegram ID должен быть числом. Введите ещё раз:")
        return

    tid = int(value)
    student = await db_get_student(tid)
    if not student:
        await msg.answer("🛎 Пользователь не найден в базе.", reply_markup=kb_admin())
        await state.clear()
        return

    await db_delete_student(tid)
    await state.clear()

    await msg.answer(
        f"✔️ Пользователь удалён:\n\n"
        f"🫂 {student.get('first_name') or '—'}\n"
        f"🟤 <code>{tid}</code>\n"
        f"📱 {student.get('phone') or '—'}",
        reply_markup=kb_admin()
    )

# ── Админ: рассылка ───────────────────────────────────────────────────────────

@router.callback_query(F.data == "adm:broadcast")
async def adm_broadcast(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        return
    _broadcast_mode.add(cb.from_user.id)
    await cb.message.edit_text("📢 Напишите текст рассылки:")
    await cb.answer()


@router.callback_query(F.data == "adm:back")
async def adm_back(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id not in ADMIN_IDS:
        return
    await state.clear()
    await cb.message.edit_text("🔧 <b>Панель администратора</b>", reply_markup=kb_admin())
    await cb.answer()

# ВАЖНО: этот хендлер должен идти после FSM-хендлеров админки и регистрации
@router.message(F.text)
async def handle_text(msg: Message):
    if msg.from_user.id in _broadcast_mode:
        _broadcast_mode.discard(msg.from_user.id)
        students = await db_all_students()
        sent, fail = 0, 0
        for s in students:
            try:
                await msg.bot.send_message(int(s["telegram_id"]), msg.text, parse_mode="HTML")
                sent += 1
            except Exception:
                fail += 1
        await msg.answer(f"🧘 Готово! ✔️ {sent} / ✖️ {fail}")

# ══════════════════════════════════════════════════════════════════════════════
#  ПЛАНИРОВЩИК
# ══════════════════════════════════════════════════════════════════════════════

def setup_scheduler(bot: Bot):
    sch = AsyncIOScheduler(timezone=TZ)
    sch.add_job(_job_complete_past, CronTrigger(hour=0, minute=5), args=[bot])
    sch.add_job(_job_day_reminder, CronTrigger(hour=10, minute=0), args=[bot])
    sch.add_job(_job_auto_cancel, CronTrigger(hour=10, minute=5), args=[bot])
    sch.add_job(_job_sub_expiry, CronTrigger(hour=9, minute=0), args=[bot])
    sch.add_job(_job_remind_2h, CronTrigger(hour=6, minute=50, day_of_week="tue,thu,sat"),
                args=[bot, ["tue_morning", "thu_morning", "sat_morning"]])
    sch.add_job(_job_remind_2h, CronTrigger(hour=16, minute=50, day_of_week="wed,fri"),
                args=[bot, ["wed_evening", "fri_evening"]])
    sch.start()
    log.info("✔️✖️ Планировщик запущен")


async def _job_complete_past(_bot: Bot):
    await db_complete_past_enrollments()


async def _job_day_reminder(bot: Bot):
    tomorrow = datetime.now() + timedelta(days=1)
    wd = tomorrow.weekday()
    for sk, slot in SLOTS.items():
        if slot["weekday"] != wd:
            continue
        class_dt = tomorrow.replace(hour=slot["hour"], minute=slot["minute"], second=0, microsecond=0)
        ts = int(class_dt.timestamp())
        for e in await db_pending_day_reminder(class_dt):
            try:
                await bot.send_message(
                    int(e["student_id"]),
                    f"🌀 <b>Напоминание!</b>\n"
                    f"{slot['label']} — завтра {class_dt.strftime('%d.%m.%Y')}\n\n"
                    "Вы будете на практике?",
                    reply_markup=kb_attend(sk, ts),
                )
                await db_mark_day_reminder(e["id"])
            except Exception as ex:
                log.warning(f"day_reminder {e['student_id']}: {ex}")


async def _job_auto_cancel(bot: Bot):
    tomorrow = datetime.now() + timedelta(days=1)
    wd = tomorrow.weekday()
    for sk, slot in SLOTS.items():
        if slot["weekday"] != wd:
            continue
        class_dt = tomorrow.replace(hour=slot["hour"], minute=slot["minute"], second=0, microsecond=0)
        async with pool.acquire() as con:
            rows = await con.fetch("""
                UPDATE enrollments SET status='cancelled'
                WHERE class_date=$1 AND slot_key=$2 AND status='confirmed'
                  AND day_reminder_sent=TRUE
                RETURNING student_id
            """, class_dt, sk)
        for r in rows:
            uid = int(r["student_id"])
            await db_increment_classes(uid)
            try:
                await bot.send_message(
                    uid,
                    f"ℹ️ Вы не подтвердили {slot['label']} "
                    f"{class_dt.strftime('%d.%m')}.\n"
                    "Запись отменена, занятие возвращено на счёт."
                )
            except Exception:
                pass


async def _job_remind_2h(bot: Bot, slot_keys: list):
    today = datetime.now()
    wd = today.weekday()
    for sk in slot_keys:
        slot = SLOTS[sk]
        if slot["weekday"] != wd:
            continue
        class_dt = today.replace(hour=slot["hour"], minute=slot["minute"], second=0, microsecond=0)
        for e in await db_pending_hour_reminder(class_dt):
            try:
                await bot.send_message(
                    int(e["student_id"]),
                    f"🪬 <b>Занятие через 2 часа!</b>\n{slot['label']}\nЖдём вас! 🫂"
                )
                await db_mark_hour_reminder(e["id"])
            except Exception as ex:
                log.warning(f"remind_2h: {ex}")


async def _job_sub_expiry(bot: Bot):
    for s in await db_expiring_soon():
        try:
            await bot.send_message(
                int(s["telegram_id"]),
                f"🛎 <b>Абонемент заканчивается через {SUB_WARN_DAYS} дней!</b>\n"
                f" До: {s['sub_expires'].strftime('%d.%m.%Y')}\n"
                f"🕊 Осталось: {s['classes_left']} занятий\n\n"
                "Оформите новый абонемент!",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="🔄 Продлить", callback_data="menu_sub")
                ]]),
            )
        except Exception as ex:
            log.warning(f"sub_expiry: {ex}")

# ══════════════════════════════════════════════════════════════════════════════
#  ЗАПУСК
# ══════════════════════════════════════════════════════════════════════════════

async def main():
    await init_db()
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    setup_scheduler(bot)
    log.info("🤖 Бот запущен!")
    await dp.start_polling(bot, allowed_updates=["message", "callback_query"])


if __name__ == "__main__":
    asyncio.run(main())
