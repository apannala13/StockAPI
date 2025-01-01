from fastapi import FastAPI, HTTPException, APIRouter
import redis 
from pydantic import BaseModel 
from datetime import date 
import logging 
import json
import uvicorn 
from typing import List, Dict 

"""
endpoints:

/GET/api/v1/market/daily
query params: symbol
return: symbol, open, close, high, low, volume, timestamp 

/GET/api/v1/market/all
query params: None
return: symbol, open, close, high, low, volume, timestamp 

"""

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()
router = APIRouter(
    prefix='/api/v1/market', 
    tags=['market']
)

class TradesDaily(BaseModel):
    symbol:str
    open:float
    close:float
    high:float
    low:float
    volume:float
    timestamp:str

class TradesList(BaseModel):
    trades: List[TradesDaily]

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

@router.get("/daily/", response_model=TradesDaily)
async def get_trades_by_symbol(symbol:str):
    try:
        data = redis_api.fetch_redis_data(symbol)
        if not data:
            raise HTTPException(status_code=404, detail=f'No data found for symbol: {symbol}')
        trades_data = TradesDaily(
            symbol=symbol,
            open=float(data['open']), 
            close=float(data['close']), 
            high=float(data['high']), 
            low=float(data['low']), 
            volume=float(data['volume']), 
            timestamp = data['timestamp']
        )
        return trades_data 
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/all/", response_model=TradesList)
async def get_all_trades():
    try:
        trades = redis_api.fetch_all_trades()
        if not trades:
            raise HTTPException(status_code=404, detail=f'No data found')
        return TradesList(trades=trades)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/")
async def root():
    return {'Message': 'Market Data API'}

app.include_router(router)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
