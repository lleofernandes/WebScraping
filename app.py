import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
import sqlite3
import asyncio
from telegram import Bot
import os
from dotenv import load_dotenv
import psycopg2
from sqlalchemy import create_engine

load_dotenv()

TOKEN = os.getenv('TELEGRAM_TOKEN')
CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
bot = Bot(token=TOKEN)


POSTGRES_DB= os.getenv("POSTGRES_DB")
POSTGRES_USER= os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD= os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST= os.getenv("POSTGRES_HOST")
POSTGRES_PORT= os.getenv("POSTGRES_PORT")


DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_DB}:{POSTGRES_PORT}"
engine = create_engine(DATABASE_URL)

#retorna o status da conexão 
def fetch_page():
    url = "https://www.mercadolivre.com.br/apple-iphone-16-pro-256-gb-titnio-deserto-distribuidor-autorizado/p/MLB1040287840#polycard_client=search-nordic&wid=MLB3846027829&sid=search&searchVariation=MLB1040287840&position=1&search_layout=stack&type=product&tracking_id=cb4ff30b-5eb8-40d1-b291-5a8b7e445646"
    response = requests.get(url)
    return response.text

def parse_page(html):
    soup = BeautifulSoup(html, 'html.parser')
    product_name = soup.find('h1', class_='ui-pdp-title').get_text()
    prices: list = soup.find_all('span', class_ = 'andes-money-amount__fraction')
    old_price: int =  int(prices[0].get_text().replace('.', ''))
    new_price: int =  int(prices[1].get_text().replace('.', ''))
    installment_price: int = int(prices[2].get_text().replace('.', ''))
    
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    
    return {
        'product_name': product_name,
        'old_price': old_price,
        'new_price': new_price,
        'installment_price': installment_price,
        'timestamp': timestamp
    }


def create_connection():
    conn = psycopg2.connect(
        db_name=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )
    return conn 
    
    
def setup_database(conn):
    cursor = conn.cursor()
    cursor.execute('''
            CREATE TABLE IF NOT EXISTS prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_name TEXT,
                old_price INTEGER,
                new_price INTEGER,
                installment_price INTEGER,
                timestamp TEXT                                
            )                   
    ''')
    conn.commit()


def save_to_db(conn, product_info):
    new_row = pd.DataFrame([product_info])
    new_row.to_sql('prices', conn,  if_exists='append', index=False)
    
def get_max_price(conn):
    cursor = conn.cursor()
    cursor.execute('''
        SELECT new_price, timestamp FROM prices
        WHERE new_price  = ( SELECT MAX(new_price) FROM prices );                       
    ''')
                   
    result = cursor.fetchone()
    cursor.close()
    if result and result[0] is not None:
        return result[0], result[1]
    return None, None


async def send_telegram_message(text):
    await bot.send_message(chat_id=CHAT_ID, text=text)

 
async def main():
    
    conn = create_connection()
    setup_database(conn)
    
    while True:
        page_content = fetch_page()
        product_info = parse_page(page_content)
        current_price = product_info['new_price']
        
        max_price, max_price_timestamp = get_max_price(conn)
        
    
        if max_price is None or current_price > max_price:            
            await send_telegram_message(f'preço maior detectado: {current_price}')
            max_price = current_price
            max_price_timestamp = product_info['timestamp']
        else:
            await send_telegram_message(f'O maior preço registrado é {max_price} em {max_price_timestamp}')
        
        save_to_db(conn, product_info)
        print('Dados salvos no banco de dados: ', product_info)
        await asyncio.sleep(10)
        
    conn.close()
    
asyncio.run(main())

        
