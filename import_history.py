import csv
import sqlite3
from database import init_db

init_db()

conn = sqlite3.connect('billiard.db')
cur = conn.cursor()

encodings = ['cp1251', 'utf-8-sig', 'utf-8']
for enc in encodings:
    try:
        with open('history.csv', 'r', encoding=enc) as f:
            lines = f.readlines()
            print(f"Файл открыт с кодировкой {enc}")
            print(f"Всего строк в файле: {len(lines)}")
            break
    except UnicodeDecodeError:
        continue
else:
    print("Не удалось открыть файл. Проверьте кодировку и имя файла.")
    exit(1)

# Пропускаем первые 3 строки (заголовки)
data_lines = lines[3:]
print(f"Строк данных после пропуска заголовков: {len(data_lines)}")
if data_lines:
    print("Первая строка данных (сырая):", repr(data_lines[0]))

reader = csv.reader(data_lines, delimiter=';')  # изменено на ';'

inserted = 0
for i, row in enumerate(reader):
    if not row or len(row) < 16:
        print(f"Строка {i+4}: пропущена (недостаточно колонок): {row}")
        continue

    # Очищаем поля от лишних пробелов
    row = [col.strip() for col in row]

    game_number = row[0]
    date_dmy = row[1]
    game_type = row[2]

    # Преобразуем дату в ISO
    try:
        day, month, year = date_dmy.split('.')
        date_iso = f"{year}-{month}-{day}"
    except Exception as e:
        print(f"Ошибка преобразования даты в строке {game_number}: {date_dmy}, ошибка: {e}")
        continue

    try:
        result_yuri = int(row[3])
        result_rinat = int(row[4])
        cum_wins_yuri = int(row[5])
        cum_wins_rinat = int(row[6])
        score_yuri = int(row[7])
        score_rinat = int(row[8])
        cum_balls_yuri = int(row[9])
        cum_balls_rinat = int(row[10])
        start_time = row[11]
        end_time = row[12]
        duration_str = row[13]
        if ':' in duration_str:
            parts = duration_str.split(':')
            if len(parts) == 2:
                duration_minutes = int(parts[0]) * 60 + int(parts[1])
            else:
                duration_minutes = int(parts[0])
        else:
            duration_minutes = int(duration_str)
        cost_rub = int(row[14])
        avg_time = row[15]
    except Exception as e:
        print(f"Ошибка преобразования чисел в строке {game_number}: {e}")
        continue

    cur.execute("""
        INSERT INTO games (
            game_number, date, game_type, result_yuri, result_rinat,
            cum_wins_yuri, cum_wins_rinat, score_yuri, score_rinat,
            cum_balls_yuri, cum_balls_rinat, start_time, end_time,
            duration_minutes, cost_rub, avg_time_per_ball
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        game_number, date_iso, game_type, result_yuri, result_rinat,
        cum_wins_yuri, cum_wins_rinat, score_yuri, score_rinat,
        cum_balls_yuri, cum_balls_rinat, start_time, end_time,
        duration_minutes, cost_rub, avg_time
    ))
    inserted += 1

conn.commit()
conn.close()
print(f"Импорт завершён. Вставлено записей: {inserted}")