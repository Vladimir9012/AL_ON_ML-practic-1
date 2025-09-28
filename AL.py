import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time

# --- Параметры ---
interval = 300  # 5 минут (в секундах)
duration = 60 * 60  # 1 час (в секундах)
csv_file = 'openphish_data.csv'

# --- Инициализация ---
start_time = datetime.now()
print("Начало парсинга:", start_time)

# Загружаем уже существующие данные, если есть
try:
    df = pd.read_csv(csv_file)
except FileNotFoundError:
    df = pd.DataFrame(columns=["URL", "Brand", "Time"])

end_time = start_time + pd.Timedelta(seconds=duration)

while datetime.now() < end_time:
    response = requests.get("https://openphish.com/")
    soup = BeautifulSoup(response.text, "html.parser")

    # Таблица на сайте (3 колонки)
    table = soup.find("table")
    rows = table.find_all("tr")[1:]  # пропускаем заголовок

    data = []
    for row in rows:
        cols = row.find_all("td")
        if len(cols) == 3:
            url = cols[0].text.strip()
            brand = cols[1].text.strip()
            attack_time = cols[2].text.strip()
            data.append([url, brand, attack_time])

    # В DataFrame
    new_df = pd.DataFrame(data, columns=["URL", "Brand", "Time"])

    # Объединяем с предыдущими и убираем дубликаты
    df = pd.concat([df, new_df]).drop_duplicates(subset=["URL"])

    # Сохраняем в CSV
    df.to_csv(csv_file, index=False)

    print(f"[{datetime.now()}] Собрано {len(new_df)} записей. Всего уникальных: {len(df)}")

    # Ждём 5 минут
    time.sleep(interval)

# --- Итог ---
finish_time = datetime.now()
print("Окончание парсинга:", finish_time)

# Аналитика
unique_urls = df["URL"].nunique()
top_brands = df["Brand"].value_counts().head(3)

print("Количество уникальных URL за период:", unique_urls)
print("Топ 3 бренда:")
print(top_brands)

# Сохраняем статистику в отдельный файл (по желанию)
stats = {
    "Время начала парсинга": start_time.strftime("%Y-%m-%d %H:%M:%S"),
    "Время окончания парсинга": finish_time.strftime("%Y-%m-%d %H:%M:%S"),
    "Количество уникальных URL": unique_urls,
    "Топ 3 бренда": top_brands.to_dict()
}

pd.DataFrame([stats]).to_csv('stats.csv', index=False)
