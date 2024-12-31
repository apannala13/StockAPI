import redis 
import psycopg2 
import logging 
import os 
from dotenv import load_dotenv
from pathlib import Path 
import json 
from decimal import Decimal 
import datetime 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

env_path = Path("../../configs/.env")
success = load_dotenv(dotenv_path=env_path)
postgres_user = os.getenv('POSTGRES_USER')
postgres_pw = os.getenv('POSTGRES_PASSWORD')


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)

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
    def fetch_ticker_data(self):
        try:
            query = """
                WITH daily_prices_agg AS(
                    SELECT 
                        symbol, 
                        price, 
                        volume,
                        timestamp,
                        FIRST_VALUE(price) OVER w as open_price, 
                        LAST_VALUE(price) OVER w as close_price
                    FROM trades 
                    WHERE DATE(timestamp) = CURRENT_DATE
                    WINDOW w AS (PARTITION BY symbol ORDER BY timestamp 
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
                )
                SELECT 
                    symbol, 
                    MAX(price) as high, 
                    MIN(price) as low, 
                    MAX(open_price) as open,
                    MIN(open_price) as close, 
                    SUM(volume) as volume,
                    MAX(timestamp) as timestamp
                FROM daily_prices_agg
                GROUP BY symbol 
            """
            self.pg_cursor.execute(query)
            result = self.pg_cursor.fetchall()
            return result 
        except Exception as e:
            logger.error(f'error, {e}')

    def push_to_redis(self):
        query_result = self.fetch_ticker_data()
        if not query_result:
            logger.error(f'Could not fetch query results')
            return 
        try:
            for row in query_result:
                symbol = row[0]
                trade_data = {
                    'high':row[1], 
                    'low':row[2], 
                    'open':row[3], 
                    'close':row[4], 
                    'volume':row[5], 
                    'timestamp':row[6]
                }
                trade_data = json.dumps(trade_data, cls=JSONEncoder)
                self.redis_client.set(symbol, trade_data)
                logger.info(f"successfully stored data for {symbol} in redis")
            return True 
        except Exception as e:
            logger.error(f'failed to store data in redis: {str(e)}')
            return False 

if __name__ == '__main__':  
    handler = RedisHandler()
    result = handler.push_to_redis()
    print(result)
