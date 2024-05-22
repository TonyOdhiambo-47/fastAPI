from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from pydantic import BaseModel
import httpx
import asyncio
from typing import Dict, List, Union

# FastAPI instance
app = FastAPI()

# SQLAlchemy database connection
DATABASE_URL = "postgresql://postgres:postgres@localhost/twitter"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Pydantic model for a single record
class Record(BaseModel):
    timestamp: str
    tweet: str
    likes: str
    replies: str


# Sample JSON data
sample_json_data = [
    {
        "timestamp": "2024-05-21T08:00:00",
        "tweet": "Excited for the upcoming #FIFAWorldCup! Who do you think will win this year?",
        "likes": "100",
        "replies": "20"
    },
    {
        "timestamp": "2024-05-20T12:00:00",
        "tweet": "Just bought tickets for the #FIFAWorldCup finals! Can't wait to experience the atmosphere!",
        "likes": "200",
        "replies": "15"
    },
    {
        "timestamp": "2024-05-19T16:00:00",
        "tweet": "The FIFA World Cup is more than just a tournament; it's a celebration of football culture. #FIFAWorldCup",
        "likes": "150",
        "replies": "30"
    }
]


# FastAPI endpoint to post sample JSON data to the 'tweets' table
@app.post("/tables/tweets/data/")
async def add_data_to_tweets(data: List[Dict[str, str]]):
    try:
        with SessionLocal() as session:
            for record in data:
                columns = ", ".join(record.keys())
                values = ", ".join([f"'{value}'" for value in record.values()])
                query = text(f"INSERT INTO tweets ({columns}) VALUES ({values})")
                session.execute(query)
            
        
        return {"message": "Data added to the table 'tweets' successfully"}

    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))

async def post_sample_data():
    url = "http://localhost:8000/tables/tweets/data/"
    headers = {"Content-Type": "application/json"}
    timeout = httpx.Timeout(10.0, connect=None, read=30.0)  # Increase read timeout to 30 seconds

    async with httpx.AsyncClient(timeout=timeout) as client:
        for tweet in sample_json_data:
            try:
                response = await client.post(url, json=tweet, headers=headers)
                response.raise_for_status()  # Raise an exception for HTTP errors
                print(response.json())
            except httpx.ReadTimeout as e:
                print(f"Read timeout occurred: {e}")
                

async def main():
    await post_sample_data()

if __name__ == "__main__":
    asyncio.run(main())
    print("Success!")

