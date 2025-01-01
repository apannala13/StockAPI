import redis
import json
import logging
from typing import Dict, List
from models.trades import TradesDaily, TradesList


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RedisAPI:
    def __init__(self):
        self.redis_client = redis.Redis(
            host='localhost', 
            port=6379, 
            db=0
        )   

    def fetch_redis_data(self, symbol):
        try:
            value = self.redis_client.get(symbol)
            if not value:
                logger.error(f'no data found for symbol {symbol}')
                return None 
            trade_data = json.loads(value.decode('utf-8'))
            return trade_data 
        except Exception as e:
            logger.error(f'error fetching data from Redis: {str(e)}')
            raise e 
    
    def fetch_all_trades(self) -> List[TradesList]:
        trades = []
        try:
            keys = self.redis_client.keys('*')
            for key in keys:
                symbol = key.decode('utf-8')
                data = self.fetch_redis_data(symbol)
                if data:
                    trades.append(
                        TradesDaily(
                            symbol=symbol,
                            open=float(data['open']), 
                            close=float(data['close']), 
                            high=float(data['high']), 
                            low=float(data['low']), 
                            volume=float(data['volume']), 
                            timestamp = data['timestamp']
                        )
                    )
            return trades
        except Exception as e:
            logger.error(f'error fetching all trades from Redis: {str(e)}')
            raise e

redis_api = RedisAPI()
