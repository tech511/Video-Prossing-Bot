import os
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")

START_TEXT = """
ʜᴇʏ, {first_name}! ᴡᴇʟᴄᴏᴍᴇ ᴛᴏ ____ 🌍🚀
ᴘᴏᴡᴇʀᴇᴅ ʙʏ:- @Bot_Arena_Official
"""

# Videos — replace with your own video URLs
HELP_VIDEO = "YOUR_HELP_VIDEO_URL"
QRCODE_VIDEO = "https://graph.org/file/218f412cc841c4f8945b9-4223b8eef03e22ff13.mp4"
TEXT_TOOLKIT_VIDEO = "https://graph.org/file/218f412cc841c4f8945b9-4223b8eef03e22ff13.mp4"
TEXT_TO_SPEECH_VIDEO = "https://graph.org/file/218f412cc841c4f8945b9-4223b8eef03e22ff13.mp4"

ABOUT_CAPTION = """
➤ ɴᴀᴍᴇ : ᴛᴇxᴛ ᴛᴏᴏʟᴋɪᴛ ʙᴏᴛ
➤ ʟᴀɴɢᴜᴀɢᴇ : ᴘʏᴛʜᴏɴ 🐍
➤ ʟɪʙʀᴀʀʏ : ᴘʏʀᴏɢʀᴀᴍ ⚡
➤ ᴅᴇᴠᴇʟᴏᴘᴇʀ : @DA_R_KS_I_DE
✦ ғᴇᴀᴛᴜʀᴇꜱ
  • Qʀ ᴄᴏᴅᴇ ɢᴇɴᴇʀᴀᴛᴏʀ
  • ᴛᴇxᴛ ᴛᴏᴏʟᴋɪᴛ
  • ᴛᴇxᴛ ᴛᴏ ꜱᴘᴇᴇᴄʜ
"""
# Replace these with your COMPLETE video URLs.
START_VIDEO = "https://graph.org/file/218f412cc841c4f8945b9-4223b8eef03e22ff13.mp4"
HELP_VIDEO = "https://graph.org/file/218f412cc841c4f8945b9-4223b8eef03e22ff13.mp4"
ABOUT_VIDEO = "https://graph.org/file/218f412cc841c4f8945b9-4223b8eef03e22ff13.mp4"

MORE_BOTS_URL = "https://t.me/AniWorld_Bots_Hub"
OWNER_URL = "https://t.me/DA_R_KS_I_DE"
