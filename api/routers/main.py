from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def read_root():
    return {"foo": "bar"}

@app.get("/hello/{name}")
async def say_hello(name:str):
    return {"message":f"hello {name}"}
