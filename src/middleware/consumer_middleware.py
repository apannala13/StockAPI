import json 
from confluent_kafka import Consumer
import logging 
import psycopg2
from datetime import datetime, timedelta 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

postgres_user = 'postgres' 
postgres_pw = 'admin' 

class Middleware:
    def __init__(self):
        self.consumer_config = {
            'bootstrap.servers':'localhost:9092',
            'group.id': 'market-data-group',
            'auto.offset.reset': 'earliest' 
        }
        self.consumer = Consumer(self.consumer_config)
        self.pg_conn = psycopg2.connect(
            dbname = "marketdata",
            user=postgres_user, 
            password = postgres_pw,
            host="localhost", 
            port=5433
        ) 
        self.pg_cursor = self.pg_conn.cursor()
        self.create_table()

    def create_table(self):
        create_table_query = """
            CREATE TABLE IF NOT EXISTS trades(
                id SERIAL PRIMARY KEY, 
                symbol VARCHAR(20) NOT NULL, 
                price DECIMAL(10, 2) NOT NULL, 
                timestamp TIMESTAMP NOT NULL,
                volume DECIMAL(15, 2) NOT NULL, 
                conditions TEXT[]
            )
        """
        try:
            self.pg_cursor.execute(create_table_query)
            self.pg_conn.commit()
            logger.info("Trades table successfully created")
        except Exception as e:
            self.pg_conn.rollback()
            logger.error(f"Error creating table {e}")

    def postgres_push(self, trades):
        try:
            insert_query = """
                INSERT INTO trades (symbol, price, timestamp, volume, conditions)
                VALUES (%s, %s, %s, %s, %s)
            """
            for trade in trades:
                try:
                    self.pg_cursor.execute(insert_query, (
                        trade['symbol'], 
                        trade['price'],
                        datetime.fromtimestamp(trade['timestamp'] / 1000), 
                        trade['volume'],
                        trade['conditions']
                    ))
                except Exception as e:
                    self.pg_conn.rollback()
                    logger.error(f'Error inserting trade: {str(e)}')
                    raise 
            self.pg_conn.commit()
        except Exception as e:
            self.pg_conn.rollback()
            logger.error(f'PG Push error: {e}')

    def transform_data(self, trade_data):
        transformed_data = []

        for trade in trade_data:
            transform_dict = {
                'symbol': trade['s'], 
                'price': float(trade['p']), 
                'timestamp':int(trade['t']), 
                'volume':float(trade['v']), 
                'conditions':trade['c']
            }
            transformed_data.append(transform_dict)
            logger.info('appended to list')
        
        return transformed_data 

    def process_message(self, message):
        try: 
            trade_data = json.loads(message) 
            if not trade_data.get('data'):
                logger.error(f'trade data is missing or empty')
                return 
            if trade_data['type'] == 'trade' and trade_data.get('data'): 
                transformed_trades = self.transform_data(trade_data['data'])
                self.postgres_push(transformed_trades)
                
                logger.info(f'Processed {len(transformed_trades)}')
        except ValueError as e:
            logger.info(f'json parsing error, {e}')
        except Exception as e: 
            raise e

    def run(self):
        self.consumer.subscribe(['market-data'])
        try: 
            while True:
                msg = self.consumer.poll(0.1)
                if msg is None:
                    continue 
                if msg.error():
                    logger.error(f"Consumer error {msg.error()}")
                    continue 
                try: 
                    value = msg.value().decode('utf-8')
                    self.process_message(value)
                    logger.info(f"receieved message {value}")
                except Exception as e: 
                    logger.info(f"erorr processing message {e}")
        except KeyboardInterrupt:
            logger.info("shutting down")
        finally:
            self.consumer.close()
            self.pg_conn.close()
    
if __name__ == "__main__":
    middleware = Middleware()
    middleware.run()
