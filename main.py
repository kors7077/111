from service import FlightService
from fastapi import FastAPI
from flight import Flight

db_config={
    'host':'postgres',
    'database':'flightsdb',
    'user':'postgres',
    'password':'123Secret_a',
    'port':5432
}
service = FlightService(**db_config)

app = FastAPI(
    title="Flight API"
)

@app.get("/")
async def root():
    return {"message":"Hello from FastAPI"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app,host="0.0.0.0", port=8080)