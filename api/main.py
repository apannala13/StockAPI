from fastapi import FastAPI
import uvicorn
from routers import market

app = FastAPI()
app.include_router(market.router)

@app.get("/")
async def root():
    return {'Message': 'Market Data API'}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
