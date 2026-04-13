import os
from contextlib import asynccontextmanager

import psycopg2
from psycopg2 import sql
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, EmailStr


def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        dbname=os.getenv("POSTGRES_DATABASE", "hr_recruitment"),
    )


def init_db():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS candidates (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            phone VARCHAR(20),
            position VARCHAR(100) NOT NULL,
            status VARCHAR(20) DEFAULT 'applied',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    cur.close()
    conn.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="HR Recruitment System", lifespan=lifespan)


class CandidateCreate(BaseModel):
    name: str
    email: str
    phone: str | None = None
    position: str


class CandidateResponse(BaseModel):
    id: int
    name: str
    email: str
    phone: str | None
    position: str
    status: str
    created_at: str


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/candidates", status_code=201)
def add_candidate(candidate: CandidateCreate):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO candidates (name, email, phone, position) VALUES (%s, %s, %s, %s) RETURNING id",
            (candidate.name, candidate.email, candidate.phone, candidate.position),
        )
        candidate_id = cur.fetchone()[0]
        conn.commit()
        return {"id": candidate_id, "message": f"Candidate '{candidate.name}' added successfully."}
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        raise HTTPException(status_code=409, detail=f"A candidate with email '{candidate.email}' already exists.")
    finally:
        cur.close()
        conn.close()


@app.delete("/candidates/{candidate_id}")
def remove_candidate(candidate_id: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM candidates WHERE id = %s RETURNING id", (candidate_id,))
    deleted = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()

    if not deleted:
        raise HTTPException(status_code=404, detail=f"No candidate found with ID {candidate_id}.")
    return {"message": f"Candidate with ID {candidate_id} removed."}


@app.get("/candidates")
def list_candidates():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, name, email, phone, position, status, created_at FROM candidates ORDER BY id")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return [
        CandidateResponse(
            id=row[0],
            name=row[1],
            email=row[2],
            phone=row[3],
            position=row[4],
            status=row[5],
            created_at=row[6].isoformat(),
        )
        for row in rows
    ]
