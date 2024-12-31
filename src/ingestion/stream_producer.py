import logging 
import websockets
import asyncio
import json 
from confluent_kafka import Producer 
from retry import retry 
import os 
from dotenv import load_dotenv
from pathlib import Path
import time 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

env_path = Path("../../configs/.env")
success = load_dotenv(dotenv_path=env_path)
WEBSOCKET_SECRET = os.getenv('WEBSOCKET_SECRET')

KAFKA_CONFIG = {
    'bootstrap.servers':'localhost:9092', 
    'client.id':'finnhub-producer'
}
producer = Producer(KAFKA_CONFIG)
max_messages = 20
symbols = [
    'AAPL', 'TSLA', 'MSFT', 'AMZN', 'GOOGL', 'META', 'NFLX', 'NVDA', 'BRK.B', 'JPM',
    'V', 'JNJ', 'WMT', 'PG', 'DIS', 'MA', 'HD', 'PYPL', 'INTC', 'CSCO',
    'XOM', 'KO', 'PEP', 'NKE', 'PFE', 'MRK', 'T', 'VZ', 'ADBE', 'CRM'
]

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Delivery failed for record {msg.key()}:{str(err)}")
    else:
        logger.info(f"Record successfully produced to {msg.topic()} [partition {msg.partition()}]")

class MessageHandler:
    def __init__(self):
        self.message_count = 0 

    async def cleanup(self, ws):
        for ticker in symbols:
            await ws.send(json.dumps({"type":"unsubscribe", "symbol":f"{ticker}"}))
        logger.info("Unsubscribed from all symbols")
        await ws.close()
        logger.info("WebSocket connection closed")    

    @retry(Exception, tries=3, delay=2, backoff=2)
    async def on_message(self, message, max_messages):
        data = json.loads(message)
        logger.info(data)
        if data['type'] != 'ping':
            try:
                producer.produce(
                    topic='market-data',
                    key=str(data.get('s', '')), #s = stock symbol 
                    value = json.dumps(data), 
                    callback=delivery_report
                )
                producer.flush(timeout=10)
                self.message_count += 1
                logger.info(f'Message pushed to kafka: {data}')
                logger.info(f"Current message count {self.message_count}")
                if self.message_count >= max_messages:
                    return True
            except Exception as e:
                logger.error(f"issue occurred: {str(e)}")
        return False

async def run_websocket():
    handler = MessageHandler()
    url = f'wss://ws.finnhub.io?token={WEBSOCKET_SECRET}'
    ws = await websockets.connect(url)
    try:
        logger.info("Connected to websocket")
        for ticker in symbols:
            await ws.send(json.dumps({"type": "subscribe", "symbol": f"{ticker}"}))
        logger.info(f"Subscribed to {len(symbols)} symbols")
        async for message in ws:
            should_stop = await handler.on_message(message, max_messages)
            if should_stop:
                logger.info("max messages loaded")
                await handler.cleanup(ws)
                return  
            await asyncio.sleep(0.1)
    except websockets.ConnectionClosedError as e:
        logger.info(f"WebSocket closed: {e.code} - {e.reason}")
        await asyncio.sleep(5)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        await asyncio.sleep(5)
    finally: 
        await ws.close()

if __name__ == "__main__":
    logger.info("execution started")
    try:
        asyncio.run(run_websocket())
    except Exception as e:
        logger.info(f"error occured during run, {e}")
    finally:
        logger.info("Application stopped")



 
