# file: bot/main.py
# -*- coding: utf-8 -*-
# """
# Бот для платного канала (стейдж/прод):

# — Таблица со строкой заголовков (РУ):
#   user_id | username | дата_оплаты | дата_окончания | notified | статус | full_name | phone_number | in_channel

# — Функционал:
#   • Апрув оплаты → UPSERT по user_id в таблицу (без дублей) + персональная join-request ссылка (на 1 час)
#   • Gatekeeper: в канал пускаем только если оплата активна на текущий период
#   • Ручная чистка /clean: кик у кого макс. «дата_окончания» < первое число текущего месяца + чистка дублей
#   • Аудит /audit: сверка таблицы и канала, обновление in_channel столбца, отчёт
#   • Восстановление оплаченных, кто не в канале /restore_paid_absent
#   • Удаление дублей строк /purge_dups
#   • Рассылки: /broadcast, /broadcast_paid_absent, /broadcast_link (единая ссылка на 2 часа)
#   • Напоминания к дате удаления: /set_delete_date, /set_reminder_text, /remind_unpaid, /remind_all
# """
import os
import json
import logging
import threading
import asyncio
from collections import defaultdict
from datetime import datetime, timedelta, date, timezone

import nest_asyncio
from dotenv import load_dotenv
from aiohttp import web

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ChatJoinRequestHandler,
    ContextTypes,
    filters,
)
from telegram.error import Forbidden, TelegramError

import gspread
from gspread.utils import rowcol_to_a1
from google.oauth2.service_account import Credentials

# ---- patch ----
nest_asyncio.apply()

# ---- runtime state ----
waiting_for_receipt = set()
users_waiting = {}

# --- reminder state (живой текст + дата удаления)
reminder_state = {
    "delete_date": None,   # date
    "text": ("⚠️ Удаление из канала {delete_date} (через {days_left} дн.). "
            "Не потеряй доступ: оформи подписку и подай заявку: {link}"),
    "link": None,          # актуальная join-request ссылка на 2 часа
    "link_expire_ts": 0,   # unix ts
}

# --- subscription config (конечный день месяца по умолчанию) ---
subscription_config = {
    "end_day": 20  # по умолчанию до 20 числа месяца
}


def _calc_end_date(today: date) -> date:
    """
    Дата окончания подписки:
    всегда end_day следующего месяца.
    """
    end_day = subscription_config.get("end_day", 20)

    # ограничение на день, чтобы не было проблем с короткими месяцами
    if end_day < 1:
        end_day = 1
    if end_day > 28:
        end_day = 28

    # 👉 ВСЕГДА следующий месяц
    if today.month == 12:
        target_year = today.year + 1
        target_month = 1
    else:
        target_year = today.year
        target_month = today.month + 1

    return date(target_year, target_month, end_day)
# ---- env ----
load_dotenv()
TOKEN = os.getenv("TOKEN")
ADMIN_ID = os.getenv("ADMIN_ID")
CHANNEL_ID = os.getenv("CHANNEL_ID")  # канал/супергруппа
SPREADSHEET_NAME = os.getenv("SPREADSHEET_NAME")
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON")  # либо creds.json на диске

required = {
    "TOKEN": TOKEN,
    "ADMIN_ID": ADMIN_ID,
    "CHANNEL_ID": CHANNEL_ID,
    "SPREADSHEET_NAME": SPREADSHEET_NAME,
}
missing = [k for k, v in required.items() if not v]
if missing:
    raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

ADMIN_ID = int(ADMIN_ID)
CHANNEL_ID = int(CHANNEL_ID)

# ---- logging ----
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram.ext").setLevel(logging.INFO)
log = logging.getLogger("style-bot")

# ---- google sheets ----
scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
try:
    if GOOGLE_CREDS_JSON:
        creds = Credentials.from_service_account_info(json.loads(GOOGLE_CREDS_JSON), scopes=scope)
    else:
        creds = Credentials.from_service_account_file("creds.json", scopes=scope)
    client = gspread.authorize(creds)
    sheet = client.open(SPREADSHEET_NAME).sheet1
except Exception:
    log.exception("Failed to initialize Google Sheets client")
    raise

# ===================== TABLE HEADERS (RU + алиасы) =====================
WANTED_HEADERS_RU = [
    "user_id", "username", "дата_оплаты", "дата_окончания",
    "notified", "статус", "full_name", "phone_number", "in_channel"
]

HEADER_ALIASES = {
    "user_id": ["user_id", "id", "userid"],
    "username": ["username", "user", "name"],
    "дата_оплаты": ["дата_оплаты", "paid_at", "дата оплаты"],
    "дата_окончания": ["дата_окончания", "paid_until", "end_date", "дата окончания"],
    "notified": ["notified"],
    "статус": ["статус", "status"],
    "full_name": ["full_name", "fullname", "fio"],
    "phone_number": ["phone_number", "phone", "телефон"],
    "in_channel": ["in_channel"]
}


def _ensure_headers_ru():
    headers = sheet.row_values(1)
    if not headers:
        sheet.update(f"A1:{rowcol_to_a1(1, len(WANTED_HEADERS_RU))}", [WANTED_HEADERS_RU])
        return
    # дозаполним недостающие справа
    changed = False
    for h in WANTED_HEADERS_RU:
        if h not in headers:
            headers.append(h)
            changed = True
    if changed:
        sheet.update(f"A1:{rowcol_to_a1(1, len(headers))}", [headers])


def _find_col(header_name: str) -> int | None:
    """Возвращает индекс (0-based) столбца по рус/англ синонимам."""
    headers = sheet.row_values(1)
    aliases = HEADER_ALIASES.get(header_name, [header_name])
    for a in aliases:
        if a in headers:
            return headers.index(a)
    return None


def _write_row_by_headers(row_dict: dict) -> list[str]:
    """
    Формирует массив значений по текущим заголовкам листа (строка для update/append).
    row_dict = { 'user_id': '...', 'username': '...', ... }
    """
    headers = sheet.row_values(1)
    return [row_dict.get(h, "") for h in headers]

# ===================== HELPERS =====================
def _nice(d: date | datetime | None) -> str:
    if not d:
        return "-"
    if isinstance(d, datetime):
        d = d.date()
    return d.strftime("%d.%m.%Y")


def _parse_sheet_date(val) -> date | None:
    """Любой формат даты из таблицы -> date | None."""
    if val is None or str(val).strip() == "":
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, (int, float)):
        # Google serial date (Excel epoch)
        try:
            base = datetime(1899, 12, 30).date()
            return base + timedelta(days=float(val))
        except Exception:
            return None
    s = str(val).strip().replace("\u00A0", " ")
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _days_left(delete_date: date) -> int:
    return max(0, (delete_date - datetime.utcnow().date()).days)



async def send_invite_link_safely(context: ContextTypes.DEFAULT_TYPE, target_id: int, link: str) -> bool:
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔗 Kanala katıl", url=link)]])
    try:
        msg = await context.bot.send_message(
            chat_id=target_id,
            text=
                "❣️ Ödemen onaylandı!\n"
    "Ödemen onaylandıktan sonra kanal katıl butonuna basmayı unutma.🌿",
            reply_markup=kb,
            disable_web_page_preview=True,
        )
        log.info("Invite button sent to user_id=%s, msg_id=%s", target_id, msg.message_id)
        return True
    except Forbidden:
        log.warning("Cannot DM user_id=%s (hasn't started the bot or blocked)", target_id)
        return False
    except TelegramError:
        log.exception("Failed to deliver invite link to %s", target_id)
        return False

# ===================== USER FLOW =====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("💳 Abonelik Satın Al", callback_data="pay")],
        [InlineKeyboardButton("🔁 Aboneliğimi Uzatmak İstiyorum", callback_data="pay")],
    ]
    if update.message:
        await update.message.reply_text(
            "✨ Stil dünyasına hoş geldin! 👠\n\n"
            "📌 Özel stil kanalımıza erişmek veya aboneliğini uzatmak için "
            "aşağıdaki butonlardan birine tıkla:",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    username = user.username or f"id{user_id}"
    full_name = f"{user.first_name or ''} {user.last_name or ''}".strip()
    language = user.language_code or ""

    users_waiting[user_id] = {"username": username, "full_name": full_name, "language": language}
    await query.answer()

    if query.data == "pay":
        keyboard = [[InlineKeyboardButton("✅ Ödeme Yaptım", callback_data="paid")]]
        await query.message.reply_text(
            "💸 Lütfen 200₺ şu karta gönderin:\n\n"
            "Gülden Koçkirli\n"
            "TR 2500 0100 2571 9458 6967 5002\n\n"
            "Ödeme yaptıktan sonra ✅ÖDEME YAPTIM butonuna basmanız lazım. "
            "📌 Kanal aboneliğiniz 19.12’ye kadar geçerlidir. 🤍\n\n"
            "📅 Hangi gün katıldığınız önemli değil — tüm eski içeriklere erişebilirsiniz. ✨",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

    elif query.data == "paid":
        waiting_for_receipt.add(user_id)
        await query.message.reply_text(
            "🧾 Harika! Lütfen dekontun ekran görüntüsünü veya PDF belgesini buraya gönder — "
            "Dekont yöneticimize iletilecek."
        )


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    m = update.effective_message
    u = update.effective_user
    if not (u and m and m.photo):
        return
    user_id = u.id
    file_id = m.photo[-1].file_id
    username = users_waiting.get(user_id, {}).get("username", u.username or f"id{user_id}")
    suffix = "" if user_id in waiting_for_receipt else " (state lost / after restart)"

    try:
        await context.bot.send_photo(
            chat_id=ADMIN_ID,
            photo=file_id,
            caption=f"📅 @{username} (ID {user_id}) dekont gönderdi{suffix}.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("✅ Approve", callback_data=f"approve:{user_id}")]]
            ),
        )
    except Exception as e:
        log.exception("Failed to forward receipt to admin: %s", e)
    finally:
        await m.reply_text("✅ Dekont alındı! Yönetici onayı bekleniyor. 🔎")
        if user_id in waiting_for_receipt:
            waiting_for_receipt.remove(user_id)


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    m = update.effective_message
    u = update.effective_user
    if not (u and m and m.document):
        return
    document = m.document
    if document.mime_type != "application/pdf":
        return
    user_id = u.id
    file_id = document.file_id
    username = users_waiting.get(user_id, {}).get("username", u.username or f"id{user_id}")
    suffix = "" if user_id in waiting_for_receipt else " (state lost / after restart)"

    try:
        await context.bot.send_document(
            chat_id=ADMIN_ID,
            document=file_id,
            caption=f"📅 @{username} (ID {user_id}) PDF dekont gönderdi{suffix}.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("✅ Approve", callback_data=f"approve:{user_id}")]]
            ),
        )
    except Exception as e:
        log.exception("Failed to forward PDF: %s", e)
    finally:
        await m.reply_text("✅ Dekont alındı! Yönetici onayı bekleniyor. 🔎")
        if user_id in waiting_for_receipt:
            waiting_for_receipt.remove(user_id)

# ===================== APPROVAL & UPSERT =====================
async def _approve_user(context: ContextTypes.DEFAULT_TYPE, target_id: int, reply_chat_id: int | None = None):
    """UPSERT в таблицу (русские заголовки) + выдача join-request ссылки на 1 час."""
    now = datetime.now(timezone.utc)
    today = now.date()
    end_date = _calc_end_date(today)

    user_info = users_waiting.get(target_id, {})
    raw_username = user_info.get("username") or ""
    username = f"@{raw_username}" if raw_username else f"id{target_id}"
    full_name = user_info.get("full_name", "")

    _ensure_headers_ru()

    # UPSERT по user_id
    try:
        matches = sheet.findall(str(target_id))
        row_dict = {
            "user_id": str(target_id),
            "username": username,
            "дата_оплаты": today.isoformat(),
            "дата_окончания": end_date.isoformat(),
            "notified": "no",
            "статус": "active",
            "full_name": full_name,
            "phone_number": ""
        }
        values = _write_row_by_headers(row_dict)

        headers = sheet.row_values(1)
        last_col = len(headers)

        if matches:
            r = matches[0].row
            start_a1 = rowcol_to_a1(r, 1)
            end_a1 = rowcol_to_a1(r, last_col)
            rng = f"{start_a1}:{end_a1}"
            sheet.update(rng, [values])
        else:
            sheet.append_row(values, value_input_option="USER_ENTERED")

        log.info("UPSERT ok RU for user %s until %s", target_id, row_dict["дата_окончания"])
    except Exception as e:
        log.exception("Failed to upsert subscriber RU: %s", e)
        if reply_chat_id:
            await context.bot.send_message(reply_chat_id, "⚠️ Не удалось сохранить запись в таблицу.")
        return

    # создать join-request ссылку (1 час)
    try:
        inv = await context.bot.create_chat_invite_link(
            chat_id=CHANNEL_ID,
            creates_join_request=True,  # КЛЮЧ — канал одобряет заявку только после проверки
            expire_date=int(now.timestamp()) + 3600,
            name=f"approve_{target_id}_{now.isoformat(timespec='seconds')}",
        )
        ok = await send_invite_link_safely(context, target_id, inv.invite_link)
    except TelegramError:
        log.exception("Failed to create invite link dynamically")
        ok = False

    # уведомление админа
    if reply_chat_id:
        nice_end = _nice(end_date)
        if ok:
            await context.bot.send_message(reply_chat_id, f"✅ {username} одобрен и получил доступ до {nice_end}.")
        else:
            await context.bot.send_message(reply_chat_id, f"⚠️ {username} одобрен, но ссылку не удалось доставить.")


async def admin_approve_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if update.effective_user.id != ADMIN_ID:
        await query.answer("⛔️ Только для администратора.", show_alert=True)
        return
    await query.answer()
    try:
        user_id = int(query.data.split(":")[1])
    except Exception:
        await query.message.reply_text("Некорректный ID.")
        return
    await _approve_user(context, user_id, reply_chat_id=query.message.chat_id)


async def approve_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.effective_message.reply_text("ℹ️ Использование: /approve <user_id>")
        return
    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.effective_message.reply_text("⛔️ Неверный user_id.")
        return
    await _approve_user(context, target_id, reply_chat_id=update.effective_message.chat_id)

# ===================== JOIN-REQUEST GATEKEEPER =====================
def _row_is_paid(row_vals: list[str]) -> bool:
    # ищем индекс колонки "дата_окончания" по алиасам
    idx = _find_col("дата_окончания")
    if idx is None or idx >= len(row_vals):
        return False
    pu = _parse_sheet_date(row_vals[idx])
    return bool(pu and pu >= datetime.utcnow().date())


async def on_join_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    req = update.chat_join_request
    uid = req.from_user.id
    try:
        cells = sheet.findall(str(uid))
        eligible = False
        for c in cells:
            row = sheet.row_values(c.row)
            if _row_is_paid(row):
                eligible = True
                break
        if eligible:
            await context.bot.approve_chat_join_request(req.chat.id, uid)
        else:
            await context.bot.decline_chat_join_request(req.chat.id, uid)
    except Exception as e:
        log.exception("join_request check failed: %s", e)
        try:
            await context.bot.decline_chat_join_request(req.chat.id, uid)
        except Exception:
            pass

# ===================== CLEANUP (ручной) =====================
async def remove_expired_subscribers(context: ContextTypes.DEFAULT_TYPE):
    """
    Агрегация по user_id, защита от дублей:
    кикаем только тех, у кого МАКС. «дата_окончания» < первое число текущего месяца,
    остальные дубли (с более ранней датой) — удаляем.
    """
    try:
        records = sheet.get_all_records()
    except Exception as e:
        log.exception("Failed to read sheet: %s", e)
        return

    today = datetime.utcnow().date()
    month_start = today.replace(day=19)

    by_user: dict[int, list[tuple[int, date]]] = {}
    for idx, row in enumerate(records, start=2):
        # user_id
        raw_uid = None
        for k in HEADER_ALIASES["user_id"]:
            if k in row and str(row[k]).strip():
                raw_uid = row[k]
                break
        try:
            uid = int(str(raw_uid).strip()) if raw_uid not in (None, "") else None
        except Exception:
            uid = None
        if uid is None:
            continue

        # дата_окончания
        raw_end = None
        for k in HEADER_ALIASES["дата_окончания"]:
            if k in row and str(row[k]).strip():
                raw_end = row[k]
                break
        end_dt = _parse_sheet_date(raw_end)
        if end_dt is None:
            continue

        by_user.setdefault(uid, []).append((idx, end_dt))

    users_to_kick = set()
    rows_to_delete = []

    for uid, rows_u in by_user.items():
        rows_sorted = sorted(rows_u, key=lambda x: (x[1], x[0]))  # (row_idx, end_dt)
        max_end = max(dt_ for _, dt_ in rows_sorted)

        if max_end < month_start:
            users_to_kick.add(uid)
            rows_to_delete.extend([row_idx for row_idx, _ in rows_sorted])
        else:
            for row_idx, end_dt in rows_sorted:
                if end_dt < max_end:
                    rows_to_delete.append(row_idx)

    async def safe_kick(user_id: int):
        from asyncio import sleep
        try:
            try:
                member = await context.bot.get_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
                status = getattr(member, "status", "")
                if status in ("creator", "administrator", "left", "kicked"):
                    return
            except TelegramError as e:
                log.debug("get_chat_member(%s) failed: %s", user_id, e)

            await context.bot.ban_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
            await context.bot.unban_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
            log.info("Kicked user %s from channel %s", user_id, CHANNEL_ID)
        except TelegramError as e:
            log.warning("kick %s failed: %s", user_id, e)
        finally:
            await sleep(0.25)

    for uid in users_to_kick:
        await safe_kick(uid)

    deleted = 0
    if rows_to_delete:
        to_delete = sorted(set(rows_to_delete), reverse=True)
        rngs = []
        start = endr = to_delete[0]
        for r in to_delete[1:]:
            if r == endr - 1:
                endr = r
            else:
                rngs.append((endr, start))  # (from, to)
                start = endr = r
        rngs.append((endr, start))

        for r_from, r_to in rngs:
            try:
                sheet.delete_rows(r_to, r_from)
                deleted += (r_from - r_to + 1)
            except Exception as e:
                log.exception("Failed batch delete rows %s-%s: %s", r_to, r_from, e)

    await context.bot.send_message(
        ADMIN_ID,
        f"🧹 Чистка завершена:\n"
        f"— кикнули пользователей: {len(users_to_kick)}\n"
        f"— удалили строк (дубликаты/устаревшие): {deleted}\n"
        f"ℹ️ Кикаем только если макс. дата < {month_start:%Y-%m-%d}; при активной — удаляем только дубли."
    )


async def clean_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await update.message.reply_text("⏳ Запускаю чистку…")
    await remove_expired_subscribers(context)
    await update.message.reply_text("✅ Готово.")

# ===================== AUDIT =====================
async def audit_subscribers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    from asyncio import sleep
    try:
        tg_count = await context.bot.get_chat_member_count(chat_id=CHANNEL_ID)
    except Exception:
        tg_count = None

    try:
        rows = sheet.get_all_values()
    except Exception as e:
        log.exception("Failed to read sheet for audit: %s", e)
        await update.effective_message.reply_text("⛔️ Не удалось прочитать таблицу.")
        return

    if not rows:
        await update.message.reply_text("Пустая таблица.")
        return

    headers, data = rows[0], rows[1:]

    def col(*candidates):
        for n in candidates:
            if n in headers:
                return headers.index(n)
        return None

    i_uid = col(*HEADER_ALIASES["user_id"])
    i_un = col(*HEADER_ALIASES["username"])
    i_fn = col(*HEADER_ALIASES["full_name"])
    i_pu = col(*HEADER_ALIASES["дата_окончания"])

    if i_uid is None:
        await update.message.reply_text("Нет колонки user_id (или её алиаса).")
        return

    by_uid = defaultdict(list)  # uid -> list[(row_num, paid_until, username, full_name)]
    for idx, r in enumerate(data, start=2):
        raw_uid = (r[i_uid] if i_uid is not None and i_uid < len(r) else "").strip()
        if not raw_uid:
            continue
        try:
            uid = int(str(raw_uid))
        except Exception:
            continue
        pu = _parse_sheet_date(r[i_pu]) if i_pu is not None and i_pu < len(r) else None
        un = r[i_un] if (i_un is not None and i_un < len(r)) else ""
        fn = r[i_fn] if (i_fn is not None and i_fn < len(r)) else ""
        by_uid[uid].append((idx, pu, un, fn))

    unique_present, unique_absent = set(), set()
    dups = []

    for uid, items in by_uid.items():
        items_sorted = sorted(items, key=lambda x: ((x[1] or date.min), x[0]), reverse=True)
        if len(items_sorted) > 1:
            dups.append((uid, [it[0] for it in items_sorted[1:]]))
        try:
            member = await context.bot.get_chat_member(chat_id=CHANNEL_ID, user_id=uid)
            in_chat = member.status in ("member", "administrator", "creator")
        except TelegramError:
            in_chat = False
        if in_chat:
            unique_present.add(uid)
        else:
            unique_absent.add(uid)
        await sleep(0.12)

    today = datetime.utcnow().date()
    to_restore = []
    for uid, items in by_uid.items():
        max_pu = max([x[1] for x in items if x[1] is not None], default=None)
        if uid in unique_absent and max_pu and max_pu >= today:
            to_restore.append(uid)

    uniq_total = len(by_uid)
    msg = []
    msg.append("📊 Аудит (уникальные пользователи)")
    if tg_count is not None:
        msg.append(f"— В канале по данным Telegram: {tg_count}")
    msg.append(f"— Всего строк в таблице: {len(data)}")
    msg.append(f"— Уникальных user_id в таблице: {uniq_total}")
    msg.append(f"— В канале (уникальные): {len(unique_present)}")
    msg.append(f"— Отсутствуют (уникальные): {len(unique_absent)}")
    msg.append(f"— Дубликаты (user_id с лишними строками): {len([1 for uid, r in dups if r])}")
    msg.append(f"— Оплачены, но отсутствуют (к восстановлению): {len(to_restore)}")

    if to_restore:
        show = ", ".join(map(str, to_restore[:20]))
        more = f" …(+{len(to_restore) - 20})" if len(to_restore) > 20 else ""
        msg.append(f"\n🔄 К восстановлению (первые 20): {show}{more}")

    if dups:
        d_show = ", ".join(f"{uid}→{rows_}" for uid, rows_ in dups[:10])
        more = f" …(+{len(dups) - 10})" if len(dups) > 10 else ""
        msg.append(f"\n🧹 Дубликаты (user_id→строки к удалению): {d_show}{more}")

    # ---- обновляем столбец in_channel одним запросом ----
    try:
        # найдём/создадим заголовок 'in_channel' в ПЕРВОЙ строке
        headers = sheet.row_values(1)
        if "in_channel" in headers:
            col_idx = headers.index("in_channel") + 1
        else:
            col_idx = len(headers) + 1
            sheet.update_cell(1, col_idx, "in_channel")

        # карта: номер строки -> статус
        status_map = {}
        for uid, items in by_uid.items():
            status_value = "yes" if uid in unique_present else "no"
            for r_num, pu, un, fn in items:
                status_map[r_num] = status_value

        total_rows = len(data)
        values = [[status_map.get(row_num, "")] for row_num in range(2, 2 + total_rows)]
        start_a1 = rowcol_to_a1(2, col_idx)
        end_a1 = rowcol_to_a1(1 + total_rows, col_idx)
        rng = f"{start_a1}:{end_a1}"
        sheet.update(rng, values, value_input_option="USER_ENTERED")
    except Exception as e:
        log.exception("Failed to write in_channel column: %s", e)

    await context.bot.send_message(ADMIN_ID, "\n".join(msg))

# ===================== ВОССТАНОВЛЕНИЕ ОПЛАЧЕННЫХ НЕ В КАНАЛЕ =====================
async def restore_paid_absent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        rows = sheet.get_all_values()
    except Exception as e:
        log.exception("sheet read failed: %s", e)
        await update.message.reply_text("⛔️ Не удалось прочитать таблицу.")
        return

    headers, data = rows[0], rows[1:]
    i_uid = _find_col("user_id")
    i_pu = _find_col("дата_окончания")
    if i_uid is None or i_pu is None:
        await update.message.reply_text("Нужны колонки: user_id и дата_окончания.")
        return

    today = datetime.utcnow().date()
    max_pu = defaultdict(lambda: None)

    for r in data:
        uid_raw = (r[i_uid] or "").strip() if i_uid < len(r) else ""
        if not uid_raw:
            continue
        try:
            uid = int(uid_raw)
        except Exception:
            continue
        pu = _parse_sheet_date(r[i_pu]) if i_pu < len(r) else None
        if pu and (max_pu[uid] is None or pu > max_pu[uid]):
            max_pu[uid] = pu

    sent, skipped = 0, 0
    for uid, pu in max_pu.items():
        try:
            m = await context.bot.get_chat_member(CHANNEL_ID, uid)
            in_chat = m.status in ("member", "administrator", "creator")
        except Exception:
            in_chat = False
        if in_chat:
            continue
        if pu and pu >= today:
            try:
                inv = await context.bot.create_chat_invite_link(
                    chat_id=CHANNEL_ID,
                    creates_join_request=True,
                    expire_date=int(datetime.utcnow().timestamp()) + 3600,
                    name=f"restore_{uid}_{int(datetime.utcnow().timestamp())}"
                )
                await context.bot.send_message(uid, f"Ваша подписка активна. Ссылка (1ч): {inv.invite_link}")
                sent += 1
            except Exception as e:
                log.warning("restore send failed for %s: %s", uid, e)
        else:
            skipped += 1

    await update.message.reply_text(f"🔄 Восстановлено отправкой ссылки: {sent}. Пропущено: {skipped}.")

# ===================== УДАЛЕНИЕ ДУБЛЕЙ СТРОК =====================
async def purge_duplicate_rows(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    try:
        rows = sheet.get_all_values()
    except Exception as e:
        log.exception("sheet read failed: %s", e)
        await update.message.reply_text("⛔️ Не удалось прочитать таблицу.")
        return

    headers, data = rows[0], rows[1:]
    i_uid = _find_col("user_id")
    i_pu = _find_col("дата_окончания")
    if i_uid is None or i_pu is None:
        await update.message.reply_text("Нужны колонки user_id и дата_окончания.")
        return

    per_uid = defaultdict(list)
    for idx, r in enumerate(data, start=2):
        if i_uid >= len(r) or not r[i_uid]:
            continue
        try:
            uid = int(r[i_uid])
        except Exception:
            continue
        pu = _parse_sheet_date(r[i_pu]) if i_pu < len(r) else None
        per_uid[uid].append((idx, pu))

    to_delete = []
    for uid, items in per_uid.items():
        if len(items) <= 1:
            continue
        items_sorted = sorted(items, key=lambda x: (x[1] or date.min, x[0]), reverse=True)
        for row_num, _ in items_sorted[1:]:
            to_delete.append(row_num)

    if not to_delete:
        await update.message.reply_text("Дубликатов не найдено.")
        return

    to_delete = sorted(set(to_delete), reverse=True)
    deleted = 0
    for r in to_delete:
        try:
            sheet.delete_rows(r)
            deleted += 1
        except Exception as e:
            log.warning("delete row %s failed: %s", r, e)

    await update.message.reply_text(f"🧹 Удалено дублей-строк: {deleted}.")

# ===================== РАССЫЛКИ =====================
async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    text = update.message.text.partition(' ')[2].strip()
    if not text:
        await update.message.reply_text("Синтаксис:\n/broadcast Текст сообщения")
        return

    try:
        rows = sheet.get_all_values()
    except Exception as e:
        log.exception("broadcast: sheet read failed: %s", e)
        await update.message.reply_text("⛔️ Не удалось прочитать таблицу.")
        return

    headers, data = rows[0], rows[1:]
    i_uid = _find_col("user_id")
    if i_uid is None:
        await update.message.reply_text("Нужна колонка user_id.")
        return

    seen = set()
    targets = []
    for r in data:
        raw = (r[i_uid] or "").strip() if i_uid < len(r) else ""
        if not raw:
            continue
        try:
            uid = int(raw)
        except Exception:
            continue
        if uid in seen:
            continue
        seen.add(uid)
        targets.append(uid)

    if not targets:
        await update.message.reply_text("Нет получателей.")
        return

    await update.message.reply_text(f"🚀 Стартую рассылку. Получателей: {len(targets)}")

    sent = failed = 0
    from asyncio import sleep
    for idx, uid in enumerate(targets, start=1):
        try:
            await context.bot.send_message(uid, text, disable_web_page_preview=True)
            sent += 1
        except Forbidden:
            failed += 1
        except TelegramError as e:
            log.warning("broadcast to %s failed: %s", uid, e)
            failed += 1
        if idx % 12 == 0:
            await sleep(1.0)

    await context.bot.send_message(
        ADMIN_ID,
        f"✅ Рассылка завершена.\nОтправлено: {sent}\nОшибок: {failed}\nВсего: {len(targets)}"
    )


async def broadcast_paid_absent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    text = update.message.text.partition(' ')[2].strip()
    if not text:
        await update.message.reply_text("Синтаксис:\n/broadcast_paid_absent Текст сообщения")
        return

    try:
        rows = sheet.get_all_values()
    except Exception as e:
        log.exception("sheet read failed: %s", e)
        await update.message.reply_text("⛔️ Не удалось прочитать таблицу.")
        return

    headers, data = rows[0], rows[1:]
    i_uid = _find_col("user_id")
    i_pu = _find_col("дата_окончания")
    i_ch = _find_col("in_channel")
    if i_uid is None or i_pu is None or i_ch is None:
        await update.message.reply_text("Нужны колонки: user_id, дата_окончания, in_channel.")
        return

    today = datetime.utcnow().date()
    seen = set()
    targets = []
    for r in data:
        uid_raw = (r[i_uid] or "").strip() if i_uid < len(r) else ""
        if not uid_raw:
            continue
        try:
            uid = int(uid_raw)
        except Exception:
            continue
        if uid in seen:
            continue
        seen.add(uid)

        pu = _parse_sheet_date(r[i_pu]) if i_pu < len(r) else None
        in_ch = (r[i_ch] or "").strip().lower() if i_ch < len(r) else ""
        if pu and pu >= today and in_ch != "yes":
            targets.append(uid)

    if not targets:
        await update.message.reply_text("Нет подходящих получателей (либо не оплачен(ы), либо уже в канале).")
        return

    await update.message.reply_text(f"🚀 Рассылка сегменту (оплачены, но отсутствуют): {len(targets)}")

    sent = failed = 0
    from asyncio import sleep
    for idx, uid in enumerate(targets, start=1):
        try:
            await context.bot.send_message(uid, text, disable_web_page_preview=True)
            sent += 1
        except Forbidden:
            failed += 1
        except TelegramError as e:
            log.warning("segment send to %s failed: %s", uid, e)
            failed += 1
        if idx % 12 == 0:
            await sleep(1.0)

    await context.bot.send_message(ADMIN_ID, f"Готово. Отправлено: {sent}, ошибок: {failed}, всего: {len(targets)}")


async def broadcast_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    text = update.message.text.partition(' ')[2].strip()
    if not text:
        await update.message.reply_text("Синтаксис:\n/broadcast_link Текст сообщения")
        return

    # создаём одну join-request ссылку на кампанию (2 часа)
    try:
        inv = await context.bot.create_chat_invite_link(
            chat_id=CHANNEL_ID,
            creates_join_request=True,
            expire_date=int(datetime.utcnow().timestamp()) + 2 * 3600,
            name=f"broadcast_{int(datetime.utcnow().timestamp())}"
        )
        link = inv.invite_link
    except TelegramError as e:
        log.exception("invite link create failed: %s", e)
        await update.message.reply_text("⛔️ Не удалось создать ссылку.")
        return

    try:
        rows = sheet.get_all_values()
    except Exception as e:
        log.exception("sheet read failed: %s", e)
        await update.message.reply_text("⛔️ Не удалось прочитать таблицу.")
        return

    headers, data = rows[0], rows[1:]
    i_uid = _find_col("user_id")
    if i_uid is None:
        await update.message.reply_text("Нужна колонка user_id.")
        return

    seen = set()
    targets = []
    for r in data:
        raw = (r[i_uid] or "").strip() if i_uid < len(r) else ""
        if not raw:
            continue
        try:
            uid = int(raw)
        except Exception:
            continue
        if uid in seen:
            continue
        seen.add(uid)
        targets.append(uid)

    if not targets:
        await update.message.reply_text("Нет получателей.")
        return

    await update.message.reply_text(f"🚀 Рассылка с ссылкой. Получателей: {len(targets)}")

    sent = failed = 0
    from asyncio import sleep
    for idx, uid in enumerate(targets, start=1):
        try:
            await context.bot.send_message(
                uid,
                f"{text}\n\n🔗 Заявка на вход (2 часа): {link}",
                disable_web_page_preview=True
            )
            sent += 1
        except Forbidden:
            failed += 1
        except TelegramError as e:
            log.warning("broadcast_link to %s failed: %s", uid, e)
            failed += 1
        if idx % 12 == 0:
            await sleep(1.0)

    await context.bot.send_message(ADMIN_ID, f"Готово. Отправлено: {sent}, ошибок: {failed}, всего: {len(targets)}")

# ===================== REMINDERS (ручные) =====================
async def set_delete_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Синтаксис: /set_delete_date YYYY-MM-DD\nНапр.: /set_delete_date 2025-12-11")
        return
    try:
        d = datetime.strptime(context.args[0], "%Y-%m-%d").date()
    except ValueError:
        await update.message.reply_text("Неверная дата. Ожидаю формат YYYY-MM-DD.")
        return
    reminder_state["delete_date"] = d
    await update.message.reply_text(f"🗓 Дата удаления установлена: {d:%Y-%m-%d} "
                                    f"(через {_days_left(d)} дн.).")


async def set_reminder_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    text = update.message.text.partition(' ')[2].strip()
    if not text:
        await update.message.reply_text(
            "Синтаксис: /set_reminder_text Текст\n"
            "Доступные плейсхолдеры: {delete_date}, {days_left}, {link}"
        )
        return
    reminder_state["text"] = text
    await update.message.reply_text("✍️ Текст напоминания обновлён.")


async def set_end_day(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Установка дня окончания подписки (1–28 число месяца)."""
    if update.effective_user.id != ADMIN_ID:
        return

    if not context.args:
        await update.message.reply_text(
            "Синтаксис: /set_end_day N\n"
            "Например: /set_end_day 20\n"
            "Подписка будет действовать до N-го числа текущего или следующего месяца."
        )
        return

    try:
        day = int(context.args[0])
    except ValueError:
        await update.message.reply_text("⛔️ День должен быть числом от 1 до 28.")
        return

    if not (1 <= day <= 28):
        await update.message.reply_text("⛔️ Разрешено только 1–28 (для всех месяцев корректно).")
        return

    subscription_config["end_day"] = day
    await update.message.reply_text(
        f"🗓 День окончания подписки установлен: {day}-е число месяца.\n"
        f"Все новые одобрения через /approve и кнопку Approve будут до {day}-го."
    )


async def remind_unpaid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Рассылка ТОЛЬКО тем, чья оплата не покрывает месяц удаления."""
    if update.effective_user.id != ADMIN_ID:
        return
    if not reminder_state["delete_date"]:
        await update.message.reply_text("Сначала установи дату: /set_delete_date YYYY-MM-DD")
        return

    delete_date = reminder_state["delete_date"]
    days_left = _days_left(delete_date)
    link = await _ensure_campaign_link(context.bot, ttl_seconds=2 * 3600)

    try:
        rows = sheet.get_all_values()
    except Exception as e:
        log.exception("remind_unpaid: sheet read failed: %s", e)
        await update.message.reply_text("⛔️ Не удалось прочитать таблицу.")
        return

    headers, data = rows[0], rows[1:]
    i_uid = _find_col("user_id")_ensure_campaign_link
    i_pu = _find_col("дата_окончания")
    if i_uid is None or i_pu is None:
        await update.message.reply_text("Нужны колонки: user_id, дата_окончания.")
        return

    month_start = delete_date.replace(day=1)

    seen = set()
    targets = []
    for r in data:
        raw = (r[i_uid] or "").strip() if i_uid < len(r) else ""
        if not raw:
            continue
        try:
            uid = int(raw)
        except Exception:
            continue
        if uid in seen:
            continue
        seen.add(uid)
        pu = _parse_sheet_date(r[i_pu]) if i_pu < len(r) else None
        if not pu or pu < month_start:
            targets.append(uid)

    if not targets:
        await update.message.reply_text("Получателей нет: все актуально оплачены на период удаления.")
        return

    base = reminder_state["text"]
    msg_text = base.format(
        delete_date=f"{delete_date:%d.%m.%Y}",
        days_left=days_left,
        link=link
    )

    await update.message.reply_text(
        f"🚀 Напоминание (НЕоплаченные на дату {delete_date:%Y-%m-%d}): {len(targets)} получателей.\n"
        f"days_left={days_left}, ссылка действует 2 часа."
    )

    sent = failed = 0
    from asyncio import sleep
    for idx, uid in enumerate(targets, start=1):
        try:
            await context.bot.send_message(
                uid,
                f"{msg_text}\n\n🔗 Заявка на вход: {link}",
                disable_web_page_preview=True
            )
            sent += 1
        except Forbidden:
            failed += 1
        except TelegramError as e:
            log.warning("reminder to %s failed: %s", uid, e)
            failed += 1
        if idx % 12 == 0:
            await sleep(1.0)

    await context.bot.send_message(
        ADMIN_ID,
        f"✅ Напоминание отправлено.\nОтправлено: {sent}\nОшибок: {failed}\nВсего: {len(targets)}"
    )


async def remind_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Рассылка ВСЕМ (общий анонс/эфир + предупреждение об удалении)."""
    if update.effective_user.id != ADMIN_ID:
        return
    if not reminder_state["delete_date"]:
        await update.message.reply_text("Сначала установи дату: /set_delete_date YYYY-MM-DD")
        return

    delete_date = reminder_state["delete_date"]
    days_left = _days_left(delete_date)
    link = await _ensure_campaign_link(context.bot, ttl_seconds=2 * 3600)

    try:
        rows = sheet.get_all_values()
    except Exception as e:
        log.exception("remind_all: sheet read failed: %s", e)
        await update.message.reply_text("⛔️ Не удалось прочитать таблицу.")
        return

    headers, data = rows[0], rows[1:]
    i_uid = _find_col("user_id")
    if i_uid is None:
        await update.message.reply_text("Нужна колонка user_id.")
        return

    seen = set()
    targets = []
    for r in data:
        raw = (r[i_uid] or "").strip() if i_uid < len(r) else ""
        if not raw:
            continue
        try:
            uid = int(raw)
        except Exception:
            continue
        if uid in seen:
            continue
        seen.add(uid)
        targets.append(uid)

    if not targets:
        await update.message.reply_text("Нет получателей.")
        return

    base = reminder_state["text"]
    msg_text = base.format(
        delete_date=f"{delete_date:%d.%m.%Y}",
        days_left=days_left,
        link=link
    )

    await update.message.reply_text(
        f"🚀 Напоминание (ВСЕМ): {len(targets)} получателей. days_left={days_left}, ссылка 2 часа."
    )

    sent = failed = 0
    from asyncio import sleep
    for idx, uid in enumerate(targets, start=1):
        try:
            await context.bot.send_message(
                uid,
                f"{msg_text}\n\n🔗 Заявка на вход: {link}",
                disable_web_page_preview=True
            )
            sent += 1
        except Forbidden:
            failed += 1
        except TelegramError as e:
            log.warning("reminder(all) to %s failed: %s", uid, e)
            failed += 1
        if idx % 12 == 0:
            await sleep(1.0)

    await context.bot.send_message(
        ADMIN_ID,
        f"✅ Напоминание всем отправлено.\nОтправлено: {sent}\nОшибок: {failed}\nВсего: {len(targets)}"
    )

# ===================== ERROR & HEALTH =====================
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    log.exception("Unhandled exception", exc_info=context.error)
    try:
        await context.bot.send_message(ADMIN_ID, f"⚠️ Exception: {context.error}")
    except Exception:
        pass


async def _health(_request):
    return web.Response(text="Bot is running!")


def run_web_server():
    app_http = web.Application()
    app_http.router.add_get("/", _health)
    app_http.router.add_get("/healthz", _health)
    port = int(os.environ.get("PORT", 10000))
    web.run_app(app_http, port=port, handle_signals=False)

# ===================== WIRING =====================
app = ApplicationBuilder().token(TOKEN).build()

# user UX
app.add_handler(CommandHandler("start", start))
app.add_handler(CallbackQueryHandler(button_handler, pattern=r"^(pay|paid)$"))
app.add_handler(MessageHandler(filters.PHOTO & filters.ChatType.PRIVATE, handle_photo))
app.add_handler(MessageHandler(filters.Document.PDF & filters.ChatType.PRIVATE, handle_document))

# admin approvals
app.add_handler(CallbackQueryHandler(admin_approve_button, pattern=r"^approve:\d+$"))
app.add_handler(CommandHandler("approve", approve_cmd))

# join-request gatekeeper
app.add_handler(ChatJoinRequestHandler(on_join_request))

# audit & cleanup
app.add_handler(CommandHandler("audit", audit_subscribers))
app.add_handler(CommandHandler("clean", clean_cmd))
app.add_handler(CommandHandler("restore_paid_absent", restore_paid_absent))
app.add_handler(CommandHandler("purge_dups", purge_duplicate_rows))

# broadcasts
app.add_handler(CommandHandler("broadcast", broadcast))
app.add_handler(CommandHandler("broadcast_paid_absent", broadcast_paid_absent))
app.add_handler(CommandHandler("broadcast_link", broadcast_link))

# reminders
app.add_handler(CommandHandler("set_delete_date", set_delete_date))
app.add_handler(CommandHandler("set_reminder_text", set_reminder_text))
app.add_handler(CommandHandler("set_end_day", set_end_day))
app.add_handler(CommandHandler("remind_unpaid", remind_unpaid))
app.add_handler(CommandHandler("remind_all", remind_all))

# errors
app.add_error_handler(error_handler)

# ===================== ENTRY =====================
def main():
    log.info("🔥 Bot started and listening!")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    threading.Thread(target=run_web_server, daemon=True).start()
    _ensure_headers_ru()
    main()
