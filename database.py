import sqlite3
import math
from datetime import datetime

import os
DB_NAME = os.getenv('DB_PATH', 'billiard.db')

def init_db():
    """Создаёт таблицы, если их нет."""
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER,
            start_time TEXT,
            end_time TEXT,
            total_duration INTEGER,
            total_cost INTEGER
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS games (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER,
            game_number INTEGER,
            date TEXT,
            game_type TEXT,
            result_yuri INTEGER,
            result_rinat INTEGER,
            cum_wins_yuri INTEGER,
            cum_wins_rinat INTEGER,
            score_yuri INTEGER,
            score_rinat INTEGER,
            cum_balls_yuri INTEGER,
            cum_balls_rinat INTEGER,
            start_time TEXT,
            end_time TEXT,
            duration_minutes INTEGER,
            cost_rub INTEGER,
            avg_time_per_ball TEXT,
            FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS active_sessions (
            chat_id INTEGER PRIMARY KEY,
            session_id INTEGER,
            start_time TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS active_game (
            chat_id INTEGER PRIMARY KEY,
            game_start_time TEXT
        )
    """)

    conn.commit()
    conn.close()

def get_last_game():
    """Возвращает последнюю запись из таблицы games (самую свежую игру) или None."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM games ORDER BY id DESC LIMIT 1")
    row = cur.fetchone()
    conn.close()
    return row

def add_game(data):
    """
    Добавляет новую игру в таблицу games.
    data - кортеж из 14 значений в порядке:
    (session_id, date, game_type, score_yuri, score_rinat,
     cum_wins_yuri, cum_wins_rinat,
     cum_balls_yuri, cum_balls_rinat,
     start_time, end_time, duration_minutes, cost_rub, avg_time_per_ball)
    """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO games (
            session_id, date, game_type, score_yuri, score_rinat,
            cum_wins_yuri, cum_wins_rinat,
            cum_balls_yuri, cum_balls_rinat,
            start_time, end_time, duration_minutes, cost_rub, avg_time_per_ball
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, data)
    conn.commit()
    conn.close()

def get_stats(date_from, date_to):
    """Возвращает все игры за указанный период (даты в формате YYYY-MM-DD)."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("""
        SELECT * FROM games 
        WHERE date BETWEEN ? AND ? 
        ORDER BY date, start_time
    """, (date_from, date_to))
    rows = cur.fetchall()
    conn.close()
    return rows

def create_session(chat_id):
    """Создаёт новую сессию и возвращает её ID."""
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    start_time = datetime.now().isoformat()
    cur.execute("INSERT INTO sessions (chat_id, start_time) VALUES (?, ?)", (chat_id, start_time))
    session_id = cur.lastrowid
    conn.commit()
    conn.close()
    return session_id

def get_active_session(chat_id):
    """Возвращает session_id и start_time активной сессии или None."""
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT session_id, start_time FROM active_sessions WHERE chat_id=?", (chat_id,))
    row = cur.fetchone()
    conn.close()
    return row

def set_active_session(chat_id, session_id, start_time):
    """Устанавливает активную сессию."""
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("REPLACE INTO active_sessions (chat_id, session_id, start_time) VALUES (?, ?, ?)",
                (chat_id, session_id, start_time))
    conn.commit()
    conn.close()

def clear_active_session(chat_id):
    """Удаляет запись активной сессии."""
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("DELETE FROM active_sessions WHERE chat_id=?", (chat_id,))
    conn.commit()
    conn.close()

def get_active_game(chat_id):
    """Возвращает время начала активной игры или None."""
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT game_start_time FROM active_game WHERE chat_id=?", (chat_id,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None

def set_active_game(chat_id, start_time):
    """Устанавливает активную игру."""
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("REPLACE INTO active_game (chat_id, game_start_time) VALUES (?, ?)", (chat_id, start_time))
    conn.commit()
    conn.close()

def clear_active_game(chat_id):
    """Удаляет запись активной игры."""
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("DELETE FROM active_game WHERE chat_id=?", (chat_id,))
    conn.commit()
    conn.close()

def get_session_games(session_id):
    """Возвращает все игры в указанной сессии."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM games WHERE session_id=? ORDER BY start_time", (session_id,))
    rows = cur.fetchall()
    conn.close()
    return rows

def update_game_costs(session_id, total_cost):
    """Распределяет стоимость сессии по играм пропорционально длительности."""
    games = get_session_games(session_id)
    total_duration = sum(g['duration_minutes'] for g in games)
    if total_duration == 0:
        return
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    for game in games:
        game_cost = int(round(game['duration_minutes'] / total_duration * total_cost))
        cur.execute("UPDATE games SET cost_rub=? WHERE id=?", (game_cost, game['id']))
    conn.commit()
    conn.close()

def end_session(chat_id, session_id):
    """Завершает сессию: вычисляет общую длительность и стоимость, обновляет запись сессии,
    распределяет стоимость по играм."""
    games = get_session_games(session_id)
    total_minutes = sum(g['duration_minutes'] for g in games)
    hours = total_minutes / 60
    billable_half_hours = math.ceil(hours * 2) / 2
    cost = int(billable_half_hours * 500)

    # Распределяем стоимость по играм
    update_game_costs(session_id, cost)

    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    end_time = datetime.now().isoformat()
    cur.execute("UPDATE sessions SET end_time=?, total_duration=?, total_cost=? WHERE id=?",
                (end_time, total_minutes, cost, session_id))
    conn.commit()
    conn.close()

    clear_active_session(chat_id)
    return total_minutes, cost

def get_year_totals(year):
    """Возвращает суммарные победы и забитые шары за указанный год."""
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            SUM(CASE WHEN score_yuri > score_rinat THEN 1 ELSE 0 END) as wins_yuri,
            SUM(CASE WHEN score_rinat > score_yuri THEN 1 ELSE 0 END) as wins_rinat,
            SUM(score_yuri) as balls_yuri,
            SUM(score_rinat) as balls_rinat
        FROM games
        WHERE strftime('%Y', date) = ?
    """, (str(year),))
    row = cur.fetchone()
    conn.close()
    if row and any(row):
        return {
            'wins_yuri': row[0] or 0,
            'wins_rinat': row[1] or 0,
            'balls_yuri': row[2] or 0,
            'balls_rinat': row[3] or 0
        }
    else:
        return {'wins_yuri': 0, 'wins_rinat': 0, 'balls_yuri': 0, 'balls_rinat': 0}

def recalc_session_cumulatives(session_id):
    """Пересчитывает накопленные итоги для всех игр в сессии после изменений."""
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT id, score_yuri, score_rinat FROM games WHERE session_id=? ORDER BY id", (session_id,))
    games = cur.fetchall()
    cum_wins_y = cum_wins_r = cum_balls_y = cum_balls_r = 0
    for g in games:
        if g[1] > g[2]:
            cum_wins_y += 1
        else:
            cum_wins_r += 1
        cum_balls_y += g[1]
        cum_balls_r += g[2]
        cur.execute("""
            UPDATE games SET cum_wins_yuri=?, cum_wins_rinat=?, cum_balls_yuri=?, cum_balls_rinat=?
            WHERE id=?
        """, (cum_wins_y, cum_wins_r, cum_balls_y, cum_balls_r, g[0]))
    conn.commit()
    conn.close()

def get_recent_sessions(chat_id, limit=5):
    """Возвращает последние сессии (без фильтра по chat_id) с количеством игр, общей стоимостью и временем."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("""
        SELECT s.id, s.start_time, s.end_time, s.total_duration, s.total_cost,
               COUNT(g.id) as games_count
        FROM sessions s
        LEFT JOIN games g ON s.id = g.session_id
        GROUP BY s.id
        ORDER BY s.id DESC
        LIMIT ?
    """, (limit,))
    rows = cur.fetchall()
    conn.close()
    return rows

def get_games_by_session(session_id):
    """Возвращает все игры указанной сессии."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM games WHERE session_id=? ORDER BY start_time", (session_id,))
    rows = cur.fetchall()
    conn.close()
    return rows

def delete_session(session_id):
    """Удаляет сессию и все её игры (каскадно, если настроено внешние ключи)."""
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    # Сначала удаляем игры (на случай, если нет каскадного удаления)
    cur.execute("DELETE FROM games WHERE session_id=?", (session_id,))
    cur.execute("DELETE FROM sessions WHERE id=?", (session_id,))
    conn.commit()
    conn.close()