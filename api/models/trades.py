from pydantic import BaseModel 
from typing import List 

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
