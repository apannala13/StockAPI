import redis 
import psycopg2 
import logging 
import os 
from dotenv import load_dotenv
from pathlib import Path 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

env_path = Path("../../configs/.env")
success = load_dotenv(dotenv_path=env_path)
postgres_user = os.getenv('POSTGRES_USER')
postgres_pw = os.getenv('POSTGRES_PASSWORD')

class RedisHandler:
    def __init__(self):
        self.pg_conn = psycopg2.connect(
            dbname = 'marketdata', 
            user = postgres_user,
            password = postgres_pw,
            host = 'localhost',
            port = 5433
        )
        self.pg_cursor = self.pg_conn.cursor()

        self.redis_client = redis.Redis(
            host='localhost', 
            port=6379, 
            db=0
        )

    def fetch_ticker_data(self, symbol):
        try:
            query = """
                SELECT * 
                FROM trades 
                WHERE symbol = %s
                LIMIT 10
            """
            self.pg_cursor.execute(query, (symbol,))
            result = self.pg_cursor.fetchall()
            return result 
        except Exception as e:
            logger.error(f'error, {e}')
    
    def push_to_redis(self, symbol, data):
        pass 


if __name__ == '__main__':
    handler = RedisHandler()
    symbol = 'AAPL'
    result = handler.fetch_ticker_data(symbol)
    print(result)


