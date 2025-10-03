from fastapi import FastAPI, Query, HTTPException
from sqlalchemy import text
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os
import uvicorn
from dotenv import load_dotenv
from datetime import datetime
from typing import Annotated

load_dotenv()

app = FastAPI(title="Spotify Listening Stats API")


DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@app.get("/top-artist")
def get_top_artist(
    year: Annotated[int, Query(description="Year of interest (e.g. 2023)")] = datetime.now().year,
    month: Annotated[int, Query(description="Month of interest (1-12)")] = datetime.now().month,
):
    """
    Returns the most listened artist for a given year and month
    """
    query = text("""
        select
            artist_stats.year_played,
            artist_stats.month_played,
            artist_stats.artist_id,
            artist.name,
            artist.images->1->'url'->>0 as image,
            artist_stats.sec_listened/60 as min_listened
        from stats.artists_listened_monthly artist_stats
        inner join prod.artist artist
            on artist_stats.artist_id = artist.id
        where artist_stats.year_played = :year
            and artist_stats.month_played = :month
        order by artist_stats.sec_listened desc
        limit 1;
    """)

    with SessionLocal() as session:
        result = session.execute(query, {"year": year, "month": month}).fetchone()

    if not result:
        raise HTTPException(status_code=404, detail="No data found for given year-month")

    return {
        "year": year,
        "month": month,
        "artist_id": result.artist_id,
        "name": result.name,
        "image": result.image,
        "min_listened": int(result.min_listened)
    }


if __name__ == '__main__':
    uvicorn.run(app=app, host="0.0.0.0", port=8000)