from fastapi import FastAPI, HTTPException, BackgroundTasks, Header
from pydantic import BaseModel
from typing import List
import redis.asyncio as redis
import databases
import sqlalchemy

app = FastAPI()

# Database & Redis configurations
DATABASE_URL = "mysql+aiomysql://user:password@localhost:3306/bookmyshow_db"
database = databases.Database(DATABASE_URL)
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

class LockRequest(BaseModel):
    user_id: int
    show_id: int
    seat_ids: List[int]

class WebhookPayload(BaseModel):
    booking_id: int
    payment_status: str
    idempotency_key: str

@app.on_event("startup")
async def startup():
    await database.connect()

@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()
    await redis_client.close()

# ---------------------------------------------------------
# API 1: Lock Seats (High Concurrency Burst Handling)
# ---------------------------------------------------------
@app.post("/lock-seats")
async def lock_seats(req: LockRequest):
    lock_keys = [f"seat_lock:{req.show_id}:{seat_id}" for seat_id in req.seat_ids]
    
    # 1. Try to acquire Redis locks for all requested seats (Atomic pipeline)
    async with redis_client.pipeline(transaction=True) as pipe:
        for key in lock_keys:
            # SETNX: Set only if it does NOT exist. EX: Expire in 600s (10 mins)
            pipe.set(key, req.user_id, ex=600, nx=True)
        results = await pipe.execute()

    # 2. Check if we got ALL locks
    if not all(results):
        # Rollback: release any locks we managed to grab
        for i, locked in enumerate(results):
            if locked:
                await redis_client.delete(lock_keys[i])
        raise HTTPException(status_code=409, detail="One or more seats are currently held by another user.")

    # 3. Create a PENDING booking in DB
    query = """
        INSERT INTO bookings (user_id, show_id, total_amount, status) 
        VALUES (:user_id, :show_id, :amount, 'PENDING')
    """
    # Note: Amount calculation omitted for brevity
    booking_id = await database.execute(query, values={"user_id": req.user_id, "show_id": req.show_id, "amount": 500})

    return {"message": "Seats locked for 10 minutes", "booking_id": booking_id}

# ---------------------------------------------------------
# API 2: Idempotent Payment Webhook
# ---------------------------------------------------------
@app.post("/webhook/payment")
async def payment_webhook(payload: WebhookPayload, x_signature: str = Header(None)):
    # 1. Idempotency Check: Did we already process this exact payment payload?
    # Relies on the UNIQUE constraint on `payment_idempotency_key` in the DB
    check_query = "SELECT id FROM bookings WHERE payment_idempotency_key = :key"
    existing = await database.fetch_one(check_query, values={"key": payload.idempotency_key})
    
    if existing:
        return {"message": "Payment already processed (Idempotent response)"}

    if payload.payment_status == "SUCCESS":
        # 2. Database Pessimistic Lock (SELECT FOR UPDATE) to finalize booking
        async with database.transaction():
            # Lock the exact rows in show_seats
            lock_query = """
                SELECT id FROM show_seats 
                WHERE show_id = :show_id AND seat_id IN (:seat_ids) 
                AND status != 'BOOKED' 
                FOR UPDATE
            """
            # (In a real app, fetch show_id and seat_ids using the booking_id)
            
            # Update seats to BOOKED
            update_seats = """
                UPDATE show_seats SET status = 'BOOKED' 
                WHERE show_id = :show_id AND seat_id IN (:seat_ids)
            """
            
            # Update Booking to CONFIRMED and save idempotency key
            update_booking = """
                UPDATE bookings 
                SET status = 'CONFIRMED', payment_idempotency_key = :key 
                WHERE id = :booking_id
            """
            await database.execute(update_booking, values={
                "key": payload.idempotency_key, 
                "booking_id": payload.booking_id
            })

        return {"message": "Booking confirmed"}
    
    return {"message": "Payment failed, seats will be released when TTL expires"}