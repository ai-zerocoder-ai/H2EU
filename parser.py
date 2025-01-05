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
import re

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("parser.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def normalize_url(url):
    parsed = urlparse(url)
    normalized = parsed._replace(path=parsed.path.rstrip('/'))
    return urlunparse(normalized)

def generate_data_key(url):
    normalized_url = normalize_url(url)
    return hashlib.sha256(normalized_url.encode('utf-8')).hexdigest()

load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")
if not openai.api_key:
    logging.error("OPENAI_API_KEY не установлена в переменных окружения.")
    exit(1)

csv_file = 'news.csv'
today_date = datetime.now().strftime('%Y-%m-%d')

try:
    with open(csv_file, 'x', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['data_key', 'title', 'translated_title', 'post_url', 'parsed_date'])
except FileExistsError:
    pass

scraper = cloudscraper.create_scraper()
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " \
    "(KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 " \
    "(KHTML, like Gecko) Version/14.0.3 Safari/605.1.15",
]
scraper.headers.update({
    "User-Agent": random.choice(user_agents),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
})

def clean_old_entries():
    rows_to_keep = []
    try:
        with open(csv_file, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if row['parsed_date'] == today_date:
                    rows_to_keep.append(row)

        with open(csv_file, 'w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=['data_key', 'title', 'translated_title', 'post_url', 'parsed_date'])
            writer.writeheader()
            writer.writerows(rows_to_keep)
    except Exception as e:
        logging.error(f"Ошибка при очистке старых записей: {e}")

def fetch_news():
    base_url = "https://hydrogeneurope.eu/"
    try:
        response = scraper.get(base_url, timeout=10)
        if response.status_code != 200:
            return

        soup = BeautifulSoup(response.content, "html.parser")
        news_items = soup.find_all('h6', class_=re.compile(r'\bentry-title\b'))

        existing_keys = set()
        with open(csv_file, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                existing_keys.add(row['data_key'])

        with open(csv_file, 'a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            for item in news_items:
                link_tag = item.find('a')
                title = link_tag.text.strip() if link_tag else "Без заголовка"
                post_url = link_tag['href'] if link_tag else ""

                if not post_url:
                    continue

                data_key = generate_data_key(post_url)

                if data_key in existing_keys:
                    continue

                full_text = fetch_full_text(post_url)
                if not full_text:
                    continue

                translated_title = translate_with_gpt(full_text)
                if not translated_title:
                    continue

                writer.writerow([data_key, title, translated_title, post_url, today_date])
                existing_keys.add(data_key)
                time.sleep(random.uniform(1, 3))
    except Exception as e:
        logging.error(f"Ошибка при парсинге новостей: {e}")

def fetch_full_text(url):
    try:
        response = scraper.get(url, timeout=10)
        if response.status_code != 200:
            return ""

        soup = BeautifulSoup(response.content, "html.parser")
        content_div = soup.find('div', class_=lambda x: x and 'the_content_wrapper' in x)
        if not content_div:
            return ""

        paragraphs = content_div.find_all('p')
        full_text = "\n".join(p.get_text(strip=True) for p in paragraphs)
        return full_text

    except Exception as e:
        logging.error(f"Ошибка при обработке статьи {url}: {e}")
        return ""

def translate_with_gpt(full_text):
    try:
        prompt_translation = f"""
        Ты самый лучший переводчик с английского языка на русский язык. Переведите текст ниже на русский язык.

        {full_text}
        """
        response_translation = openai.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt_translation}],
            temperature=0.2,
        )

        output = response_translation.choices[0].message.content.strip()
        return output

    except Exception as e:
        logging.error(f"Ошибка при переводе текста: {e}")
        return None

if __name__ == "__main__":
    clean_old_entries()
    fetch_news()
