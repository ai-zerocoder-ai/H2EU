import cloudscraper
from bs4 import BeautifulSoup
import csv
from datetime import datetime
import openai
from dotenv import load_dotenv
import os
import logging
import time
import random
from urllib.parse import urlparse, urlunparse
import hashlib

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("parser.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Функция для нормализации URL
def normalize_url(url):
    parsed = urlparse(url)
    normalized = parsed._replace(path=parsed.path.rstrip('/'))
    return urlunparse(normalized)

# Функция для генерации хеша из URL
def generate_data_key(url):
    normalized_url = normalize_url(url)
    return hashlib.sha256(normalized_url.encode('utf-8')).hexdigest()

# Загрузка переменных окружения
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")
if not openai.api_key:
    logging.error("OPENAI_API_KEY не установлена в переменных окружения.")
    exit(1)

# Настраиваем CSV файл
csv_file = 'news.csv'
today_date = datetime.now().strftime('%Y-%m-%d')  # Текущая дата в формате YYYY-MM-DD

# Проверяем, существует ли файл, и добавляем заголовки, если он новый
try:
    with open(csv_file, 'x', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['data_key', 'title', 'translated_title', 'post_url', 'parsed_date'])
        logging.info(f"Создан новый CSV-файл {csv_file} с заголовками.")
except FileExistsError:
    logging.info(f"CSV-файл {csv_file} уже существует.")

# Создаём scraper с использованием cloudscraper
scraper = cloudscraper.create_scraper()
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " \
    "(KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 " \
    "(KHTML, like Gecko) Version/14.0.3 Safari/605.1.15",
    # Добавьте больше User-Agent по необходимости
]
scraper.headers.update({
    "User-Agent": random.choice(user_agents),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
})

# Функция очистки устаревших записей
def clean_old_entries():
    logging.info("Очистка старых записей из CSV-файла.")
    rows_to_keep = []
    try:
        with open(csv_file, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if row['parsed_date'] == today_date:  # Оставляем только сегодняшние записи
                    rows_to_keep.append(row)

        # Перезаписываем файл только с актуальными записями
        with open(csv_file, 'w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=['data_key', 'title', 'translated_title', 'post_url', 'parsed_date'])
            writer.writeheader()
            writer.writerows(rows_to_keep)
        logging.info("Очистка завершена.")
    except Exception as e:
        logging.error(f"Ошибка при очистке старых записей: {e}")

# Функция парсинга новостей
def fetch_news():
    base_url = "https://hydrogeneurope.eu/"
    logging.info(f"Запрос к основному URL: {base_url}")
    try:
        response = scraper.get(base_url, timeout=10)
        if response.status_code != 200:
            logging.error(f"Ошибка загрузки страницы: {response.status_code}")
            return

        soup = BeautifulSoup(response.content, "html.parser")
        news_items = soup.find_all('h6', class_='entry-title ')  # Класс заголовков статей
        logging.info(f"Найдено {len(news_items)} новостей.")

        if not news_items:
            logging.info("Нет новостей для обработки.")
            return

        # Читаем существующие data_key из файла
        existing_keys = set()
        with open(csv_file, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                existing_keys.add(row['data_key'])
        logging.info(f"Загружено {len(existing_keys)} существующих ключей новостей.")

        with open(csv_file, 'a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            for item in news_items:
                link_tag = item.find('a')  # Ссылка на новость
                title = link_tag.text.strip() if link_tag else "Без заголовка"
                post_url = link_tag['href'] if link_tag else ""

                if not post_url:
                    logging.warning("URL статьи отсутствует. Пропуск.")
                    continue

                data_key = generate_data_key(post_url)  # Генерация хеша из нормализованного URL
                logging.debug(f"Обрабатываем data_key: {data_key}")

                if data_key in existing_keys:
                    logging.info(f"Новость {post_url} уже добавлена.")
                    continue

                # Парсим полный текст статьи
                full_text = fetch_full_text(post_url)
                if not full_text:
                    logging.warning(f"Полный текст для статьи {post_url} не был получен. Пропуск.")
                    continue

                # Создаём перевод заголовка
                translated_title = translate_with_gpt(title, full_text)
                if not translated_title:
                    logging.warning(f"Не удалось получить перевод для статьи {post_url}. Пропуск.")
                    continue

                # Записываем данные в CSV
                try:
                    writer.writerow([data_key, title, translated_title, post_url, today_date])
                    logging.info(f"Добавлена новость: {title} (перевод: {translated_title})")
                    existing_keys.add(data_key)  # Добавляем ключ в существующие после записи
                except Exception as e:
                    logging.error(f"Ошибка при записи новости {title} в CSV: {e}")

                # Добавляем случайную задержку, чтобы избежать блокировок
                time.sleep(random.uniform(1, 3))

        logging.info("Все новости обработаны.")
    except Exception as e:
        logging.error(f"Ошибка при парсинге новостей: {e}")

# Функция для извлечения полного текста статьи
def fetch_full_text(url):
    try:
        logging.info(f"Загрузка статьи: {url}")
        response = scraper.get(url, timeout=10)
        if response.status_code != 200:
            logging.error(f"Ошибка загрузки статьи {url}: {response.status_code}")
            return ""

        soup = BeautifulSoup(response.content, "html.parser")

        # Класс текста статьи
        content_div = soup.find('div', class_='the_content_wrapper ')
        paragraphs = content_div.find_all('p') if content_div else []
        if not paragraphs:
            logging.error(f"Нет параграфов в статье {url}.")
            return ""

        full_text = "\n".join(p.get_text(strip=True) for p in paragraphs)
        logging.info(f"Извлечён полный текст статьи {url}, длина: {len(full_text)} символов.")
        return full_text

    except Exception as e:
        logging.error(f"Ошибка при парсинге текста статьи {url}: {e}")
        return ""


# Функция для перевода заголовка
def translate_with_gpt(title, full_text):
    try:
        prompt_translation = f"""
        Ты самый лучший переводчик с английского языка на русский язык. Переведите текст ниже на русский язык.

        Заголовок: {title}
        Текст статьи: {full_text}

        """
        logging.info("Отправка запроса к GPT-4 для перевода...")
        response_translation = openai.ChatCompletion.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt_translation}],
            temperature=0.2,
        )
        logging.info("Ответ от GPT-4 для перевода получен.")

        output = response_translation.choices[0].message.content.strip()
        return output.replace("Перевод:", "").strip()

    except Exception as e:
        logging.error(f"Ошибка при переводе заголовка: {e}")
        return None

if __name__ == "__main__":
    clean_old_entries()
    fetch_news()
