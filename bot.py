import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
import csv
from datetime import datetime
from parser import fetch_news
from dotenv import load_dotenv
import os
import time
import schedule
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bot.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Загрузка токена бота из .env
load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")
GROUP_ID = os.getenv("GROUP_ID")

if not TOKEN or not GROUP_ID:
    logging.error("BOT_TOKEN или GROUP_ID не установлены в .env файле.")
    exit(1)
else:
    logging.info("BOT_TOKEN и GROUP_ID успешно загружены.")

bot = telebot.TeleBot(TOKEN)

csv_file = 'news.csv'
sent_news_file = 'sent_news.txt'

if os.path.exists(sent_news_file):
    try:
        with open(sent_news_file, 'r', encoding='utf-8') as f:
            sent_news = set(line.strip() for line in f if line.strip())
        logging.info(f"Загружено {len(sent_news)} отправленных новостей.")
    except Exception as e:
        logging.error(f"Ошибка чтения {sent_news_file}: {e}")
        sent_news = set()
else:
    sent_news = set()
    logging.info("Файл sent_news.txt не найден. Начинаем с пустого набора отправленных новостей.")

def publish_news():
    global sent_news

    logging.info("🔄 Начало проверки обновлений новостей...")

    try:
        logging.debug("🔍 Получение новых новостей...")
        fetch_news()
        logging.info("🔍 Новые новости получены.")
    except Exception as e:
        logging.error(f"Ошибка при получении новостей: {e}")
        return

    new_news = []
    try:
        with open(csv_file, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if row['data_key'] not in sent_news:
                    new_news.append(row)
    except Exception as e:
        logging.error(f"Ошибка чтения {csv_file}: {e}")
        return

    logging.info(f"Новых новостей для отправки: {len(new_news)}")

    if not new_news:
        logging.info("✅ Новых новостей нет.")
        return

    for news in new_news:
        title = news['title']
        translated_title = news['translated_title']
        post_url = news['post_url']

        message_text = (
            f"📰 <b>{title}</b>\n\n"
            f"{translated_title}\n\n"
            f"<a href='{post_url}'>Читать оригинал</a>"
        )

        markup = InlineKeyboardMarkup()
        webapp_button = InlineKeyboardButton(
            text="🔗 Оригинал статьи",
            url=post_url
        )
        markup.add(webapp_button)

        try:
            logging.debug(f"Попытка отправки новости: {title}")
            bot.send_message(
                GROUP_ID,
                message_text,
                parse_mode='HTML',
                disable_web_page_preview=False,
                reply_markup=markup
            )
            logging.info(f"✅ Новость отправлена: {title}")
            sent_news.add(news['data_key'])

            with open(sent_news_file, 'a', encoding='utf-8') as f:
                f.write(f"{news['data_key']}\n")
        except Exception as e:
            logging.error(f"❌ Ошибка при отправке новости '{title}': {e}")

        time.sleep(3)

    logging.info(f"✅ Сохранены отправленные новости: {len(sent_news)} записей.")

schedule.every(60).minutes.do(publish_news)

if __name__ == "__main__":
    logging.info("🤖 Бот запущен и готов публиковать новости.")
    try:
        bot.send_message(GROUP_ID, "🤖 Бот запущен и начал мониторинг новостей.")
        logging.info("✅ Тестовое сообщение отправлено.")
    except Exception as e:
        logging.error(f"❌ Ошибка отправки тестового сообщения: {e}")

    logging.info("🔧 Выполнение тестового вызова publish_news()...")
    publish_news()

    while True:
        schedule.run_pending()
        time.sleep(1)
