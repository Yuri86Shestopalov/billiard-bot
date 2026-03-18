import logging
import sqlite3
import re
import math
import os
import tempfile
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import (
    Application, CommandHandler, MessageHandler, filters,
    ContextTypes, CallbackQueryHandler
)
from dotenv import load_dotenv
import openpyxl
from openpyxl.styles import Font

from database import (
    init_db, get_last_game, add_game, get_stats,
    create_session, get_active_session, set_active_session, clear_active_session,
    get_active_game, set_active_game, clear_active_game,
    get_session_games, end_session, get_year_totals, recalc_session_cumulatives,
    update_game_costs, get_recent_sessions, get_games_by_session, delete_session
)

load_dotenv()

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ========== БЕЗОПАСНОСТЬ ==========
allowed_users_str = os.getenv('ALLOWED_USERS', '')
ALLOWED_USER_IDS = []
if allowed_users_str:
    for part in allowed_users_str.split(','):
        part = part.strip()
        if part.isdigit():
            ALLOWED_USER_IDS.append(int(part))

def is_user_allowed(update: Update) -> bool:
    return update.effective_user.id in ALLOWED_USER_IDS

def restricted(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not is_user_allowed(update):
            await update.message.reply_text("❌ У вас нет доступа к этому боту.")
            return
        return await func(update, context)
    return wrapper
# ====================================

# ------------------ Команда /start ------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [KeyboardButton("🎱 Новая игра")],
        [KeyboardButton("📊 Статистика")],
        [KeyboardButton("📁 Экспорт Excel")],
        [KeyboardButton("📝 Редактировать историю")],
        [KeyboardButton("❌ Отмена")]
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text(
        "🎱 **Бильярдный бот**\n\n"
        "Используйте кнопки ниже для управления:",
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

# ------------------ Обработчик текстовых кнопок меню ------------------
async def handle_menu_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if text == "🎱 Новая игра":
        await newgame(update, context)
    elif text == "📊 Статистика":
        await stats(update, context)
    elif text == "📁 Экспорт Excel":
        await export_stats(update, context)
    elif text == "📝 Редактировать историю":
        await edit_history(update, context)
    elif text == "❌ Отмена":
        await cancel(update, context)
    else:
        await handle_text(update, context)

# ------------------ Команда /newgame ------------------
@restricted
async def newgame(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    # Проверяем, не начата ли уже игра
    active_game = get_active_game(chat_id)
    if active_game:
        await update.message.reply_text("У вас уже есть начатая партия. Сначала завершите её.")
        return

    # Проверяем активную сессию
    active_session = get_active_session(chat_id)
    if not active_session:
        session_id = create_session(chat_id)
        start_time = datetime.now().isoformat()
        set_active_session(chat_id, session_id, start_time)
        await update.message.reply_text("🆕 Начат новый поход в бильярд!")
    else:
        session_id = active_session[0]

    # Запускаем игру
    game_start = datetime.now().isoformat()
    set_active_game(chat_id, game_start)

    keyboard = [[InlineKeyboardButton("⏹ Конец партии", callback_data="end_game")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("⏱ Партия начата! Когда закончите, нажмите кнопку.", reply_markup=reply_markup)

# ------------------ Кнопка "Конец партии" ------------------
async def end_game_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = query.message.chat_id

    active_game_start = get_active_game(chat_id)
    if not active_game_start:
        await query.edit_message_text("Нет активной партии.")
        return

    # Сохраняем время начала и удаляем активную игру
    context.user_data['game_start'] = active_game_start
    clear_active_game(chat_id)

    # Устанавливаем состояние ожидания ввода счёта Юрия
    context.user_data['state'] = 'awaiting_score_yuri'
    await query.edit_message_text("Сколько шаров забил Юрий? (введите число)")

# ------------------ Обработка ввода счёта Юрия ------------------
async def handle_score_yuri(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if not text.isdigit():
        await update.message.reply_text("Пожалуйста, введите целое число.")
        return
    context.user_data['score_yuri'] = int(text)
    context.user_data['state'] = 'awaiting_score_rinat'
    await update.message.reply_text("Сколько шаров забил Ринат?")

# ------------------ Обработка ввода счёта Рината ------------------
async def handle_score_rinat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if not text.isdigit():
        await update.message.reply_text("Пожалуйста, введите целое число.")
        return
    rinat_score = int(text)
    yuri_score = context.user_data.get('score_yuri')
    if yuri_score is None:
        await update.message.reply_text("Ошибка: не найден счёт Юрия. Начните заново.")
        context.user_data.clear()
        return

    if yuri_score == rinat_score:
        await update.message.reply_text("Ничья невозможна. Попробуйте снова.")
        context.user_data.clear()
        await update.message.reply_text("Начните заново командой /newgame")
        return

    context.user_data['score_rinat'] = rinat_score
    context.user_data['state'] = 'awaiting_game_type'

    keyboard = [
        [InlineKeyboardButton("🎱 Американка", callback_data="type_american")],
        [InlineKeyboardButton("🏆 Московская пирамида", callback_data="type_moscow")],
        [InlineKeyboardButton("✏️ Другой...", callback_data="type_other")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Выберите тип игры:", reply_markup=reply_markup)

# ------------------ Выбор типа игры ------------------
async def game_type_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "type_other":
        context.user_data['state'] = 'awaiting_game_type_manual'
        await query.edit_message_text("Введите название игры вручную:")
    else:
        game_type = "Американка" if data == "type_american" else "Московская пирамида"
        context.user_data['game_type'] = game_type
        await finish_game(query.message, context)

# ------------------ Ручной ввод типа игры ------------------
async def handle_game_type_manual(update: Update, context: ContextTypes.DEFAULT_TYPE):
    game_type = update.message.text.strip()
    if not game_type:
        await update.message.reply_text("Название не может быть пустым. Попробуйте ещё раз.")
        return
    context.user_data['game_type'] = game_type
    await finish_game(update.message, context)

# ------------------ Завершение игры ------------------
async def finish_game(message, context):
    chat_id = message.chat_id
    yuri_score = context.user_data['score_yuri']
    rinat_score = context.user_data['score_rinat']
    game_type = context.user_data['game_type']
    game_start_str = context.user_data['game_start']

    end_dt = datetime.now()
    start_dt = datetime.fromisoformat(game_start_str)
    duration = end_dt - start_dt
    minutes = int(duration.total_seconds() // 60)
    seconds = int(duration.total_seconds() % 60)

    last_game = get_last_game()
    if last_game:
        last_cum_wins_yuri = last_game['cum_wins_yuri']
        last_cum_wins_rinat = last_game['cum_wins_rinat']
        last_cum_balls_yuri = last_game['cum_balls_yuri']
        last_cum_balls_rinat = last_game['cum_balls_rinat']
    else:
        last_cum_wins_yuri = last_cum_wins_rinat = last_cum_balls_yuri = last_cum_balls_rinat = 0

    winner = 'Юрий' if yuri_score > rinat_score else 'Ринат'
    cum_wins_yuri = last_cum_wins_yuri + (1 if winner == 'Юрий' else 0)
    cum_wins_rinat = last_cum_wins_rinat + (1 if winner == 'Ринат' else 0)
    cum_balls_yuri = last_cum_balls_yuri + yuri_score
    cum_balls_rinat = last_cum_balls_rinat + rinat_score

    total_balls = yuri_score + rinat_score
    if total_balls > 0:
        avg_seconds = duration.total_seconds() / total_balls
        avg_time = f"{int(avg_seconds//60):02d}:{int(avg_seconds%60):02d}"
    else:
        avg_time = "00:00"

    today = datetime.now().strftime('%Y-%m-%d')
    start_str = start_dt.strftime('%H:%M')
    end_str = end_dt.strftime('%H:%M')

    active_session = get_active_session(chat_id)
    if not active_session:
        session_id = create_session(chat_id)
        set_active_session(chat_id, session_id, datetime.now().isoformat())
    else:
        session_id = active_session[0]

    add_game((
        session_id, today, game_type, yuri_score, rinat_score,
        cum_wins_yuri, cum_wins_rinat,
        cum_balls_yuri, cum_balls_rinat,
        start_str, end_str, minutes, 0, avg_time
    ))

    result_text = (
        f"✅ Партия завершена!\n"
        f"📊 Счёт: Юрий {yuri_score} – {rinat_score} Ринат\n"
        f"⏱ Время партии: {minutes} мин {seconds} сек\n"
        f"🎯 Среднее время на шар: {avg_time}\n\n"
        f"🏆 Текущий счёт побед: Юрий {cum_wins_yuri} – {cum_wins_rinat} Ринат"
    )

    keyboard = [
        [InlineKeyboardButton("🔄 Новая партия", callback_data="new_game_in_session"),
         InlineKeyboardButton("🏁 Завершить сессию", callback_data="end_session")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await message.reply_text(result_text, reply_markup=reply_markup)
    context.user_data.clear()

# ------------------ Кнопка "Новая партия" внутри сессии ------------------
async def new_game_in_session(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = query.message.chat_id

    active_session = get_active_session(chat_id)
    if not active_session:
        await query.edit_message_text("Нет активной сессии. Начните с /newgame")
        return

    game_start = datetime.now().isoformat()
    set_active_game(chat_id, game_start)

    keyboard = [[InlineKeyboardButton("⏹ Конец партии", callback_data="end_game")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text("⏱ Новая партия начата! Когда закончите, нажмите кнопку.", reply_markup=reply_markup)

# ------------------ Кнопка "Завершить сессию" ------------------
async def end_session_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = query.message.chat_id

    active_session = get_active_session(chat_id)
    if not active_session:
        await query.edit_message_text("Нет активной сессии.")
        return

    session_id, session_start_str = active_session
    total_minutes, cost = end_session(chat_id, session_id)

    games = get_session_games(session_id)
    num_games = len(games)
    yuri_wins_session = sum(1 for g in games if g['score_yuri'] > g['score_rinat'])
    rinat_wins_session = num_games - yuri_wins_session
    yuri_balls_session = sum(g['score_yuri'] for g in games)
    rinat_balls_session = sum(g['score_rinat'] for g in games)

    total_balls_session = yuri_balls_session + rinat_balls_session
    total_duration_seconds = sum(g['duration_minutes'] for g in games) * 60

    if total_balls_session > 0:
        avg_seconds_session = total_duration_seconds / total_balls_session
        avg_time_session = f"{int(avg_seconds_session//60):02d}:{int(avg_seconds_session%60):02d}"
    else:
        avg_time_session = "00:00"

    if num_games > 0:
        avg_game_duration_min = total_duration_seconds / 60 / num_games
        avg_game_duration = f"{int(avg_game_duration_min)} мин {int((avg_game_duration_min % 1) * 60)} сек"
    else:
        avg_game_duration = "0 мин"

    year = datetime.now().year
    year_totals = get_year_totals(year)

    result = (
        f"🏁 **Сессия завершена!**\n\n"
        f"📅 Начало: {session_start_str[:10]} {session_start_str[11:16]}\n"
        f"🎱 Всего партий: {num_games}\n"
        f"⏱ Общее время: {total_minutes} мин ({total_minutes/60:.1f} ч)\n"
        f"📊 Средняя длительность партии: {avg_game_duration}\n"
        f"💰 Стоимость сессии: {cost} руб\n\n"
        f"**В этой сессии:**\n"
        f"Юрий: побед {yuri_wins_session}, шаров {yuri_balls_session}\n"
        f"Ринат: побед {rinat_wins_session}, шаров {rinat_balls_session}\n"
        f"Среднее время на шар: {avg_time_session}\n\n"
        f"**Накоплено с начала {year} года:**\n"
        f"Юрий: побед {year_totals['wins_yuri']}, шаров {year_totals['balls_yuri']}\n"
        f"Ринат: побед {year_totals['wins_rinat']}, шаров {year_totals['balls_rinat']}"
    )

    await query.edit_message_text(result, parse_mode='Markdown')

# ------------------ Команда /cancel ------------------
@restricted
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    clear_active_game(chat_id)
    context.user_data.clear()
    await update.message.reply_text("Текущая партия отменена. Сессия продолжается.")

# ------------------ Команда /stats ------------------
@restricted
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("День", callback_data="period_day"),
         InlineKeyboardButton("Неделя", callback_data="period_week")],
        [InlineKeyboardButton("Месяц", callback_data="period_month"),
         InlineKeyboardButton("Год", callback_data="period_year")],
        [InlineKeyboardButton("Весь период", callback_data="period_all"),
         InlineKeyboardButton("Ручной ввод", callback_data="period_manual")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Выберите период:", reply_markup=reply_markup)

# ------------------ Обработчик кнопок статистики ------------------
async def stats_button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    today = datetime.now().date()

    if data == "period_day":
        date_from = date_to = today.strftime('%Y-%m-%d')
        await send_stats(query.message, date_from, date_to)
    elif data == "period_week":
        start_week = today - timedelta(days=today.weekday())
        date_from = start_week.strftime('%Y-%m-%d')
        date_to = today.strftime('%Y-%m-%d')
        await send_stats(query.message, date_from, date_to)
    elif data == "period_month":
        date_from = today.replace(day=1).strftime('%Y-%m-%d')
        date_to = today.strftime('%Y-%m-%d')
        await send_stats(query.message, date_from, date_to)
    elif data == "period_year":
        date_from = today.replace(month=1, day=1).strftime('%Y-%m-%d')
        date_to = today.strftime('%Y-%m-%d')
        await send_stats(query.message, date_from, date_to)
    elif data == "period_all":
        date_from = "2020-01-01"
        date_to = today.strftime('%Y-%m-%d')
        await send_stats(query.message, date_from, date_to)
    elif data == "period_manual":
        await query.message.reply_text("Введите даты в формате ДД.ММ.ГГГГ-ДД.ММ.ГГГГ (например, 01.01.2023-31.01.2023)")
        context.user_data['state'] = 'awaiting_manual_period'
    else:
        await query.message.reply_text("Неизвестная команда.")

# ------------------ Отправка статистики ------------------
async def send_stats(message, date_from, date_to):
    rows = get_stats(date_from, date_to)
    if not rows:
        await message.reply_text("Нет данных за этот период.")
        return

    total_games = len(rows)
    yuri_wins = sum(1 for r in rows if r['score_yuri'] > r['score_rinat'])
    rinat_wins = sum(1 for r in rows if r['score_rinat'] > r['score_yuri'])

    yuri_balls = sum(r['score_yuri'] for r in rows)
    rinat_balls = sum(r['score_rinat'] for r in rows)

    total_balls = yuri_balls + rinat_balls
    total_duration_seconds = sum(r['duration_minutes'] for r in rows) * 60
    if total_balls > 0:
        avg_seconds_total = total_duration_seconds / total_balls
        avg_time_total = f"{int(avg_seconds_total//60):02d}:{int(avg_seconds_total%60):02d}"
    else:
        avg_time_total = "00:00"

    if total_games > 0:
        avg_duration_minutes = total_duration_seconds / 60 / total_games
        avg_duration_str = f"{int(avg_duration_minutes)} мин {int((avg_duration_minutes % 1) * 60)} сек"
    else:
        avg_duration_str = "0 мин"

    total_cost = sum(r['cost_rub'] for r in rows)

    yuri_bar = "🟦" * min(yuri_wins, 20) + (f" ({yuri_wins})" if yuri_wins > 20 else "")
    rinat_bar = "🟥" * min(rinat_wins, 20) + (f" ({rinat_wins})" if rinat_wins > 20 else "")

    await message.reply_text(
        f"📊 **Статистика за период {date_from} - {date_to}**\n\n"
        f"Всего игр: {total_games}\n"
        f"Побед: Юрий {yuri_wins} – {rinat_wins} Ринат\n"
        f"{yuri_bar}\n{rinat_bar}\n\n"
        f"Забито шаров: Юрий {yuri_balls} – {rinat_balls} Ринат\n"
        f"⏱ Среднее время на шар: {avg_time_total}\n"
        f"📅 Средняя длительность партии: {avg_duration_str}\n"
        f"💰 Общие расходы: {total_cost} руб",
        parse_mode='Markdown'
    )

# ------------------ Обработчик текстовых сообщений ------------------
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = context.user_data.get('state')

    if state == 'awaiting_score_yuri':
        await handle_score_yuri(update, context)
    elif state == 'awaiting_score_rinat':
        await handle_score_rinat(update, context)
    elif state == 'awaiting_game_type_manual':
        await handle_game_type_manual(update, context)
    elif state == 'awaiting_manual_period':
        text = update.message.text.strip()
        match = re.match(r'^(\d{2})\.(\d{2})\.(\d{4})-(\d{2})\.(\d{2})\.(\d{4})$', text)
        if match:
            d1, m1, y1, d2, m2, y2 = match.groups()
            date_from = f"{y1}-{m1}-{d1}"
            date_to = f"{y2}-{m2}-{d2}"
            context.user_data['state'] = None
            await send_stats(update.message, date_from, date_to)
        else:
            await update.message.reply_text("Неверный формат. Попробуйте ещё раз или отмените командой /stats")
    elif state == 'awaiting_edit_score':
        text = update.message.text.strip()
        pattern = r'^Юрий\s+(\d+)\s+Ринат\s+(\d+)$'
        match = re.match(pattern, text)
        if not match:
            await update.message.reply_text("Неверный формат. Используйте: Юрий 8 Ринат 7")
            return
        yuri_score = int(match.group(1))
        rinat_score = int(match.group(2))
        if yuri_score == rinat_score:
            await update.message.reply_text("Ничья невозможна.")
            return
        game_id = context.user_data.get('edit_game_id')
        session_id = context.user_data.get('edit_session_id')
        if not game_id or not session_id:
            await update.message.reply_text("Ошибка: данные не найдены.")
            return
        conn = sqlite3.connect('billiard.db')
        cur = conn.cursor()
        cur.execute("SELECT duration_minutes FROM games WHERE id=?", (game_id,))
        row = cur.fetchone()
        if not row:
            await update.message.reply_text("Игра не найдена.")
            conn.close()
            return
        duration_min = row[0]
        total_balls = yuri_score + rinat_score
        if total_balls > 0:
            avg_seconds = (duration_min * 60) / total_balls
            avg_time = f"{int(avg_seconds//60):02d}:{int(avg_seconds%60):02d}"
        else:
            avg_time = "00:00"
        cur.execute("""
            UPDATE games SET score_yuri=?, score_rinat=?, avg_time_per_ball=?
            WHERE id=?
        """, (yuri_score, rinat_score, avg_time, game_id))
        conn.commit()
        conn.close()
        recalc_session_cumulatives(session_id)
        await update.message.reply_text("Счёт обновлён. Накопленные итоги пересчитаны.")
        context.user_data.clear()
    else:
        await update.message.reply_text("Я не понимаю эту команду. Используйте кнопки меню.")

# ------------------ Команда /export (Excel) ------------------
def create_excel_file():
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Статистика игр"

    headers = [
        "ID", "Сессия ID", "Номер игры", "Дата", "Тип игры",
        "Рез Юрий", "Рез Ринат", "Накопленные победы Юрий", "Накопленные победы Ринат",
        "Очки Юрий", "Очки Ринат", "Накопленные шары Юрий", "Накопленные шары Ринат",
        "Начало", "Конец", "Длительность (мин)", "Стоимость", "Ср. время на шар"
    ]
    ws.append(headers)
    for col in range(1, len(headers)+1):
        ws.cell(row=1, column=col).font = Font(bold=True)

    conn = sqlite3.connect('billiard.db')
    cur = conn.cursor()
    cur.execute("SELECT * FROM games ORDER BY date, start_time")
    rows = cur.fetchall()
    conn.close()

    for row in rows:
        ws.append(row)

    total_row = ["ИТОГО"] + [""] * (len(headers)-1)
    sum_cols = [9, 10, 15, 16]  # индексы колонок для суммирования (начиная с 1)
    for i, row in enumerate(rows, start=2):
        for col_idx in sum_cols:
            try:
                val = row[col_idx-1]
                if val is None:
                    val = 0
                if total_row[col_idx] == "":
                    total_row[col_idx] = val
                else:
                    total_row[col_idx] += val
            except:
                pass
    for col_idx in sum_cols:
        if total_row[col_idx] != "":
            total_row[col_idx] = str(total_row[col_idx])
    ws.append(total_row)

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    wb.save(tmp.name)
    return tmp.name

@restricted
async def export_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⏳ Формирую Excel файл...")
    file_path = create_excel_file()
    with open(file_path, 'rb') as f:
        await update.message.reply_document(document=f, filename='billiard_stats.xlsx')
    os.unlink(file_path)

# ------------------ Редактирование истории (выбор сессии) ------------------
@restricted
async def edit_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    sessions = get_recent_sessions(None, limit=5)
    if not sessions:
        await update.message.reply_text("История пуста.")
        return

    keyboard = []
    for s in sessions:
        start_str = s['start_time'][:10] if s['start_time'] else "неизвестно"
        btn_text = f"{start_str} ({s['games_count']} игр, {s['total_cost']} руб)"
        keyboard.append([InlineKeyboardButton(btn_text, callback_data=f"session_{s['id']}")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="edit_back")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Выберите сессию для редактирования:", reply_markup=reply_markup)

# ------------------ Обработчик выбора сессии ------------------
async def session_choice_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "edit_back":
        await query.edit_message_text("Возврат в главное меню.")
        return

    if data.startswith("session_"):
        try:
            session_id = int(data.split("_")[1])
        except (IndexError, ValueError):
            await query.edit_message_text("Ошибка: некорректный идентификатор сессии.")
            return

        games = get_games_by_session(session_id)
        if not games:
            # Сессия пуста – предлагаем удалить
            keyboard = [
                [InlineKeyboardButton("🗑 Удалить сессию", callback_data=f"del_session_{session_id}")],
                [InlineKeyboardButton("🔙 Назад к сессиям", callback_data="back_to_sessions")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(
                "В этой сессии нет игр. Хотите удалить её?",
                reply_markup=reply_markup
            )
            return

        # Формируем текст с информацией о сессии
        first_game_date = games[0]['date'] if games else "неизвестно"
        text = f"📅 **Сессия от {first_game_date}**\n\n"
        for g in games:
            text += f"🕐 {g['start_time']} – Юрий {g['score_yuri']} : {g['score_rinat']} Ринат\n"

        keyboard = []
        for g in games:
            btn_edit = InlineKeyboardButton(f"✏️ Игра {g['id']} ({g['score_yuri']}:{g['score_rinat']})", callback_data=f"edit_game_{g['id']}")
            btn_del = InlineKeyboardButton(f"🗑 Удалить {g['id']}", callback_data=f"del_game_{g['id']}")
            keyboard.append([btn_edit, btn_del])
        keyboard.append([InlineKeyboardButton("🗑 Удалить всю сессию", callback_data=f"del_session_{session_id}")])
        keyboard.append([InlineKeyboardButton("🔙 Назад к сессиям", callback_data="back_to_sessions")])
        reply_markup = InlineKeyboardMarkup(keyboard)

        try:
            await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
        except Exception as e:
            print(f"Ошибка при редактировании сообщения: {e}", flush=True)
            await query.message.reply_text("Произошла ошибка при отображении сессии.")

# ------------------ Обработчик действий с играми/сессией ------------------
async def edit_game_actions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "back_to_sessions":
        await edit_history(query.message, context)
        return

    if data.startswith("edit_game_"):
        try:
            game_id = int(data.split("_")[2])
        except (IndexError, ValueError):
            await query.edit_message_text("Ошибка: некорректный идентификатор игры.")
            return

        try:
            conn = sqlite3.connect('billiard.db')
            cur = conn.cursor()
            cur.execute("SELECT session_id FROM games WHERE id=?", (game_id,))
            row = cur.fetchone()
            conn.close()
        except Exception as e:
            print(f"Ошибка БД при получении session_id для игры {game_id}: {e}", flush=True)
            await query.edit_message_text("Ошибка базы данных.")
            return

        if not row:
            await query.edit_message_text("Игра не найдена.")
            return

        session_id = row[0]
        context.user_data['edit_game_id'] = game_id
        context.user_data['edit_session_id'] = session_id
        context.user_data['state'] = 'awaiting_edit_score'
        await query.edit_message_text("Введите новый счёт в формате: Юрий 8 Ринат 7")

    elif data.startswith("del_game_"):
        try:
            game_id = int(data.split("_")[2])
        except (IndexError, ValueError):
            await query.edit_message_text("Ошибка: некорректный идентификатор игры.")
            return

        try:
            conn = sqlite3.connect('billiard.db')
            cur = conn.cursor()
            cur.execute("SELECT session_id FROM games WHERE id=?", (game_id,))
            row = cur.fetchone()
            if not row:
                await query.edit_message_text("Игра не найдена.")
                conn.close()
                return
            session_id = row[0]
            cur.execute("DELETE FROM games WHERE id=?", (game_id,))
            conn.commit()
            conn.close()
            recalc_session_cumulatives(session_id)
        except Exception as e:
            print(f"Ошибка при удалении игры {game_id}: {e}", flush=True)
            await query.edit_message_text("Ошибка при удалении игры.")
            return

        await query.edit_message_text("Игра удалена. Накопленные итоги пересчитаны.")
        # Возвращаемся к списку игр этой сессии
        games = get_games_by_session(session_id)
        if games:
            text = f"📅 **Сессия от {games[0]['date']}**\n\n"
            for g in games:
                text += f"🕐 {g['start_time']} – Юрий {g['score_yuri']} : {g['score_rinat']} Ринат\n"
            keyboard = []
            for g in games:
                btn_edit = InlineKeyboardButton(f"✏️ Игра {g['id']} ({g['score_yuri']}:{g['score_rinat']})", callback_data=f"edit_game_{g['id']}")
                btn_del = InlineKeyboardButton(f"🗑 Удалить {g['id']}", callback_data=f"del_game_{g['id']}")
                keyboard.append([btn_edit, btn_del])
            keyboard.append([InlineKeyboardButton("🗑 Удалить всю сессию", callback_data=f"del_session_{session_id}")])
            keyboard.append([InlineKeyboardButton("🔙 Назад к сессиям", callback_data="back_to_sessions")])
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
        else:
            await query.edit_message_text("В сессии больше нет игр.")
            await edit_history(query.message, context)

    elif data.startswith("del_session_"):
        try:
            session_id = int(data.split("_")[2])
        except (IndexError, ValueError):
            await query.edit_message_text("Ошибка: некорректный идентификатор сессии.")
            return

        keyboard = [
            [InlineKeyboardButton("✅ Да, удалить", callback_data=f"confirm_del_session_{session_id}"),
             InlineKeyboardButton("❌ Нет", callback_data="back_to_sessions")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text("Вы уверены, что хотите удалить всю сессию и все её игры?", reply_markup=reply_markup)

    elif data.startswith("confirm_del_session_"):
        try:
            session_id = int(data.split("_")[3])
        except (IndexError, ValueError):
            await query.edit_message_text("Ошибка: некорректный идентификатор сессии.")
            return

        try:
            delete_session(session_id)
        except Exception as e:
            print(f"Ошибка при удалении сессии {session_id}: {e}", flush=True)
            await query.edit_message_text("Ошибка при удалении сессии.")
            return

        await query.edit_message_text("Сессия и все её игры удалены.")
        await edit_history(query.message, context)

# ------------------ Команда /edit_last ------------------
@restricted
async def edit_last(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    active_session = get_active_session(chat_id)
    if not active_session:
        await update.message.reply_text("Нет активной сессии.")
        return
    session_id = active_session[0]
    games = get_session_games(session_id)
    if not games:
        await update.message.reply_text("В этой сессии ещё нет завершённых партий.")
        return
    last_game = games[-1]
    context.user_data['edit_game_id'] = last_game['id']
    context.user_data['edit_session_id'] = session_id
    keyboard = [
        [InlineKeyboardButton("✏️ Изменить счёт", callback_data="edit_score")],
        [InlineKeyboardButton("🗑 Удалить партию", callback_data="delete_game")],
        [InlineKeyboardButton("❌ Отмена", callback_data="edit_cancel")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        f"Последняя партия:\n"
        f"Юрий {last_game['score_yuri']} – {last_game['score_rinat']} Ринат\n"
        f"Тип: {last_game['game_type']}\n\n"
        f"Что хотите сделать?",
        reply_markup=reply_markup
    )

async def edit_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "edit_cancel":
        await query.edit_message_text("Действие отменено.")
        context.user_data.clear()
    elif data == "edit_score":
        context.user_data['state'] = 'awaiting_edit_score'
        await query.edit_message_text("Введите новый счёт в формате: Юрий 8 Ринат 7")
    elif data == "delete_game":
        game_id = context.user_data.get('edit_game_id')
        session_id = context.user_data.get('edit_session_id')
        if not game_id or not session_id:
            await query.edit_message_text("Ошибка: данные не найдены.")
            return
        conn = sqlite3.connect('billiard.db')
        cur = conn.cursor()
        cur.execute("DELETE FROM games WHERE id=?", (game_id,))
        conn.commit()
        conn.close()
        recalc_session_cumulatives(session_id)
        await query.edit_message_text("Партия удалена. Накопленные итоги пересчитаны.")
        context.user_data.clear()
    else:
        await query.edit_message_text("Неизвестная команда.")

# ------------------ Универсальный обработчик всех колбэков ------------------
async def universal_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = update.callback_query.data
    print(f"🔍 Получен callback: {data}", flush=True)

    # Определяем, какой обработчик вызвать
    if data == "end_game":
        await end_game_callback(update, context)
    elif data.startswith("type_"):
        await game_type_callback(update, context)
    elif data == "new_game_in_session":
        await new_game_in_session(update, context)
    elif data == "end_session":
        await end_session_callback(update, context)
    elif data.startswith("period_"):
        await stats_button_handler(update, context)
    elif data in ("edit_score", "delete_game", "edit_cancel"):
        await edit_callback(update, context)
    elif data.startswith("session_") or data == "edit_back":
        await session_choice_callback(update, context)
    elif data.startswith(("edit_game_", "del_game_", "del_session_", "confirm_del_session_", "back_to_sessions")):
        await edit_game_actions(update, context)
    else:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text("Неизвестная команда.")

# ------------------ Главная функция ------------------
def main():
    init_db()
    TOKEN = os.getenv('BOT_TOKEN')
    if not TOKEN:
        logger.error("Не задан BOT_TOKEN в файле .env")
        return

    proxy_url = os.getenv('PROXY_URL')
    if proxy_url:
        from telegram.request import HTTPXRequest
        request = HTTPXRequest(proxy=proxy_url)
        application = Application.builder().token(TOKEN).request(request).build()
        logger.info(f"Используется прокси: {proxy_url}")
    else:
        application = Application.builder().token(TOKEN).build()

    # Универсальный обработчик всех колбэков
    application.add_handler(CallbackQueryHandler(universal_callback))

    # Команды и текстовые кнопки
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex("^(🎱 Новая игра|📊 Статистика|📁 Экспорт Excel|📝 Редактировать историю|❌ Отмена)$"), handle_menu_buttons))
    application.add_handler(CommandHandler("stats", stats))
    application.add_handler(CommandHandler("export", export_stats))
    application.add_handler(CommandHandler("edit_last", edit_last))
    application.add_handler(CommandHandler("edit_history", edit_history))
    application.add_handler(CommandHandler("cancel", cancel))

    # Общий обработчик текста (для ввода чисел, названий и т.д.)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    application.run_polling()

if __name__ == '__main__':
    main()