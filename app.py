import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
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

POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT"))

DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL)

# Função para buscar os dados da página
def fetch_page():
    url = "https://www.mercadolivre.com.br/centrifuga-de-roupas-mueller-15kg-fit-branca-220v/p/MLB24005536#polycard_client=search-nordic&wid=MLB3751124265&sid=search&searchVariation=MLB24005536&position=6&search_layout=stack&type=product&tracking_id=4dc79df7-e4e6-4a1a-bdc5-0d557bcfb4cd"
    response = requests.get(url)
    return response.text

# Função para fazer o parsing do HTML e extrair as informações
def parse_page(html):
    soup = BeautifulSoup(html, 'html.parser')
    product_name = soup.find('h1', class_='ui-pdp-title').get_text()
    prices: list = soup.find_all('span', class_='andes-money-amount__fraction')
    old_price: int = int(prices[0].get_text().replace('.', ''))
    new_price: int = int(prices[1].get_text().replace('.', ''))
    installment_price: int = int(prices[2].get_text().replace('.', ''))
    
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    
    return {
        'product_name': product_name,
        'old_price': old_price,
        'new_price': new_price,
        'installment_price': installment_price,
        'timestamp': timestamp
    }

# Função para criar a conexão com o banco de dados
def create_connection():
    conn = psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )
    return conn

# Função para configurar a tabela no banco de dados
def setup_database(conn):
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
                CREATE TABLE IF NOT EXISTS prices (
                    id SERIAL PRIMARY KEY,
                    product_name TEXT,
                    old_price INTEGER,
                    new_price INTEGER,
                    installment_price INTEGER,
                    timestamp TEXT                                  
                )                   
        ''')
        conn.commit()
    
    except Exception as e:
        print(f"Erro ao criar a tabela: {e}")
        
    finally:
        cursor.close()

# Função para salvar os dados no banco de dados
def save_to_db(engine, product_info):
    new_row = pd.DataFrame([product_info])
    new_row.to_sql('prices', engine, if_exists='append', index=False)

# Função para buscar o último preço registrado no banco de dados
def get_last_price(conn, product_name):
    cursor = conn.cursor()
    cursor.execute('''
        SELECT new_price, TO_CHAR(CAST(timestamp AS timestamp), 'DD/MM/YYYY HH24:MI:SS') AS timestamp 
        FROM prices
        WHERE product_name = %s
        ORDER BY timestamp DESC
        LIMIT 1
    ''', (product_name,))
    
    result = cursor.fetchone()
    cursor.close()
    if result:
        return result[0], result[1]
    return None, None

# Função para enviar a mensagem para o Telegram
async def send_telegram_message(text):
    await bot.send_message(chat_id=CHAT_ID, text=text)

# Função principal de execução
async def main():
    conn = create_connection()
    setup_database(conn)
    
    try:
        while True:
            page_content = fetch_page()
            product_info = parse_page(page_content)
            product_name = product_info['product_name']
            current_price = product_info['new_price']
            
            # Busca o último preço registrado no banco de dados
            last_price, last_price_timestamp = get_last_price(conn, product_name)
            
            # Verifica se houve alteração no preço
            if last_price is None:
                # Se não houver preço registrado, apenas armazena e envia alerta de preço detectado
                await send_telegram_message(f"Preço detectado para o produto {product_name}: R$ {current_price}")
                save_to_db(engine, product_info)
            
            elif current_price > last_price:
                # Se o preço aumentou, envia alerta de aumento
                await send_telegram_message(f"Preço aumentou para o produto {product_name}: R$ {current_price} (anterior era de: R$ {last_price} em {last_price_timestamp}) ")
                save_to_db(engine, product_info)
            
            elif current_price < last_price:
                # Se o preço diminuiu, envia alerta de diminuição
                await send_telegram_message(f"Preço diminuiu para o produto {product_name}: R$ {current_price} (anterior era de : R$ {last_price} em {last_price_timestamp}) ")
                save_to_db(engine, product_info)
            
            print('Dados referente a pesquisa: ', product_info)
            await asyncio.sleep(10)
    
    finally:
        conn.close()

# Executa o processo
asyncio.run(main())
