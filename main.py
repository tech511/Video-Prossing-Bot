from pyrogram import Client

from config import API_ID, API_HASH, BOT_TOKEN
from handle.start import register_start
from callback import register_callbacks
from choice import register_choice

app = Client(
    "Zip.py",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

register_start(app)
register_callbacks(app)
register_choice(app)

if __name__ == "__main__":
    app.run()
