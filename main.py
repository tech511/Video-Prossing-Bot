from pyrogram import Client

from config import API_ID, API_HASH, BOT_TOKEN
from handle.start import register_start
from callback import register_callbacks
from choice import register_choice
from handle.stats import register_stats
from handle.ban import register_ban
from handle.unban import register_unban
from handle.broadcast import register_broadcast
from handle.help import register_help
app = Client(
    "Zip.py",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

register_start(app)
register_help(app)
register_callbacks(app)
register_choice(app)
register_ban(app)        # guard + /ban
register_unban(app)
register_stats(app)
register_broadcast(app)

if __name__ == "__main__":
    app.run()
