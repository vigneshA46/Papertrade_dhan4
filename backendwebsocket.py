import websocket

WS_URL = "wss://dreaminalgo-backend-production.up.railway.app/"

def on_message(ws, message):
    print("📩 Received:", message)

def on_error(ws, error):
    print("❌ Error:", error)

def on_close(ws, close_status_code, close_msg):
    print("🔌 Connection closed")

def on_open(ws):
    print("✅ Connected to WebSocket")

    # If your backend expects something, send here
    # ws.send("Hello from Python")

if __name__ == "__main__":
    ws = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )

    ws.run_forever()