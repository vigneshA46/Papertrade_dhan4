from fastapi import FastAPI, WebSocket, APIRouter
import asyncio
from dhanhq import MarketFeed, DhanContext
from dispatcher import publish
from dhan_token import get_access_token
import os

app = FastAPI()
router = APIRouter()

tokens = set()
clients = set()
lock = asyncio.Lock()

feed = None
loop = None
restart_lock = asyncio.Lock()

async def connect(ws: WebSocket):
    await ws.accept()
    async with lock:
        clients.add(ws)

async def disconnect(ws: WebSocket):
    async with lock:
        clients.discard(ws)

async def broadcast(data):
    dead = []
    async with lock:
        for c in clients:
            try:
                await c.send_json(data)
            except:
                dead.append(c)

        for d in dead:
            clients.discard(d)

def is_valid_token(token: str):
    return token.isdigit() and len(token) > 3


def on_message(msg):
    token = str(msg.get("security_id"))

    if msg.get("ltp") is None:
        return

    publish(token, msg)

    try:
        asyncio.run_coroutine_threadsafe(broadcast(msg), loop)
    except Exception as e:
        print("Broadcast error:", e)

def on_connect(msg=None):
    print("WS CONNECTED")

def on_close(msg=None):
    print("WS CLOSED")

def start_dhan_ws():
    global feed

    try:
        access_token = get_access_token()
        client_id = os.getenv("CLIENT_ID")

        dhan_context = DhanContext(client_id, access_token)

        instruments = []
        instruments.append((MarketFeed.IDX, "13", MarketFeed.Quote))

        valid_tokens = []
        for t in list(tokens):
            if is_valid_token(t):
                valid_tokens.append(t)
            else:
                print("Skipping invalid token:", t)

        instruments.extend([
            (MarketFeed.NSE_FNO, t, MarketFeed.Quote)
            for t in valid_tokens
        ])

        print("Starting WS with:", instruments)

        feed = MarketFeed(dhan_context, instruments, "v2")

        feed.on_message = on_message
        feed.on_connect = on_connect
        feed.on_close = on_close

        feed.run_forever()

    except Exception as e:
        print("WS ERROR:", e)

async def restart_ws():
    global feed

    async with restart_lock:

        if feed:
            try:
                print("Closing WS...")
                await feed.disconnect()
            except:
                pass

            # wait for proper cleanup
            await asyncio.sleep(3)

        print("Starting new WS...")
        loop.run_in_executor(None, start_dhan_ws)

@router.post("/add-token")
async def add_token(exchange: str, token: str):

    if token in tokens:
        return {"status": "already exists"}

    if not is_valid_token(token):
        return {"error": "Invalid token"}

    async with lock:
        tokens.add(token)

    await restart_ws()

    return {"status": "added", "tokens": list(tokens)}


@router.post("/clear-tokens")
async def clear_tokens():
    async with lock:
        tokens.clear()

    await restart_ws()

    return {"status": "cleared"}

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await connect(ws)

    try:
        while True:
            await ws.receive_text()
    except:
        await disconnect(ws)

@app.on_event("startup")
async def startup():
    global loop
    loop = asyncio.get_running_loop()

    loop.run_in_executor(None, start_dhan_ws)


app.include_router(router)