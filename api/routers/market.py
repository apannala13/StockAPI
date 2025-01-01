from fastapi import FastAPI, HTTPException, APIRouter
from models.trades import TradesDaily, TradesList
from services.redis_api import redis_api

router = APIRouter(
    prefix='/api/v1/market', 
    tags=['market']
)

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
    
