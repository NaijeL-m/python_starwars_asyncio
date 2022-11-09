import requests
import asyncio
import aiohttp
from more_itertools import chunked
from sqlalchemy import Column, Integer, JSON, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base


CHANK_SIZE = 1
PG_DSN = 'postgresql+asyncpg://app:secret@127.0.0.1:5431/app'
engine = create_async_engine(PG_DSN)
Base = declarative_base()

class People(Base):
    __tablename__ = 'people'

    id = Column(Integer, primary_key=True)
    json_t = Column(JSON, nullable=False)

class People_disc(Base):
    __tablename__ = 'people_disc'

    id = Column(Integer, primary_key=True)
    birth_year = Column(String)
    eye_color = Column(String)
    films = Column(String, nullable=False)
    gender = Column(String, nullable=True)
    hair_color = Column(String)
    height = Column(String, nullable=True)
    homeworld = Column(String, nullable=True)
    mass = Column(String, nullable=True)
    name = Column(String, nullable=False)
    skin_color = Column(String, nullable=True)
    species = Column(String, nullable=False)
    starships = Column(String, nullable=False)
    vehicles = Column(String, nullable=False)

async def get_people(session, people_id):
        result = await session.get(f'https://swapi.dev/api/people/{people_id}')
        return await result.json()

async def insert_people(async_session_maker, people):
    people_list = [People(json_t=item) for item in people]
    people_list_di = []
    for i in people:
        if len(i) > 2:
            people_list_di.append(People_disc(
                        birth_year=i.get("birth_year"),
                        eye_color=i.get("eye_color"),
                        films=str(i.get("films")),
                        gender=i.get("gender"),
                        hair_color=i.get("hair_color"),
                        height=str(i.get("height")),
                        homeworld=i.get("homeworld"),
                        mass=i.get("mass"),
                        name=i.get("name"),
                        skin_color=i.get("skin_color"),
                        species=str(i.get("species")),
                        starships=str(i.get("starships")),
                        vehicles =str(i.get("vehicles")),
                        ))
    async with async_session_maker() as orm_session:
        orm_session.add_all(people_list_di)
        await orm_session.commit()

async def main():
    n = requests.get("https://swapi.dev/api/people").json()['count']
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()
    async_session_maker = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with aiohttp.ClientSession() as web_session:
        for chunk_id in chunked(range(1,n), CHANK_SIZE):
            coros = [get_people(web_session, i) for i in chunk_id]
            result = await asyncio.gather(*coros)
            print(result)
            task = asyncio.create_task(insert_people(async_session_maker, people=result))
        await task

asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
asyncio.get_event_loop().run_until_complete(main())
