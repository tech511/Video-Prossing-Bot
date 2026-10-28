import os
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))

START_TEXT = """
ʜᴇʏ, {first_name}! ᴡᴇʟᴄᴏᴍᴇ ᴛᴏ ᴀᴅᴠᴀɴᴄᴇ
ᴠɪᴅᴇᴏ ᴘʀᴏꜱꜱɪɴɢ ʙᴏᴛ.🌍🚀

ᴛʜɪꜱ ʙᴏᴛ ʜᴇʟᴘ ʏᴏᴜ ᴛᴏ ᴍᴀᴋᴇ ʏᴏᴜʀ ᴠɪᴅᴇᴏ 
ᴢɪᴘ, ᴛʀɪᴍ, ᴍᴇʀɢᴇ ᴀɴᴅ ᴍᴀɴʏ ᴍᴏʀᴇ. ɪᴛ 
ᴀʟꜱᴏ ꜱᴜᴘᴘᴏʀᴛ ᴇᴠᴇʀʏ ᴍᴇᴅɪᴀ ꜰᴏʀᴍᴀᴛ.

ᴘᴏᴡᴇʀᴇᴅ ʙʏ:- @Bot_Arena_Official
"""

HELP_CAPTION = """

• ꜱᴇɴᴅ ʏᴏᴜʀ ᴠɪᴅᴇᴏ ɪɴᴛᴏ ʙᴏᴛ.
• ᴡᴀɪᴛ ᴀ ꜰᴇᴡ ꜱᴇᴄᴏɴᴅꜱ.
• ʙᴏᴛ ɢɪᴠᴇ ʏᴏᴜ ᴍᴀɴʏ ᴏᴘᴛɪᴏɴꜱ.
• ᴄʜᴏᴏꜱᴇ ʏᴏᴜʀ ᴏᴘᴛɪᴏɴ ᴛʜᴀᴛ ʏᴏᴜ ᴡᴀɴᴛ.

"""
# Videos — replace with your own video URLs
ABOUT_CAPTION = """
✦ ɴᴀᴍᴇ : ᴠɪᴅᴇᴏ-ᴘʀᴏꜱꜱɪɴɢ-ʙᴏᴛ
✦ ʟᴀɴɢᴜᴀɢᴇ : ᴘʏᴛʜᴏɴ 3.11🐍
✦ ʟɪʙʀᴀʀʏ : ᴘʏʀᴏɢʀᴀᴍ ⚡
✦ ᴅᴀᴛᴀʙᴀꜱᴇ : ᴍᴏɴɢᴏ ᴅʙ
✦ ᴅᴇᴠᴇʟᴏᴘᴇʀ : @DA_R_KS_I_DE
"""
# Replace these with your COMPLETE video URLs.
START_VIDEO = "https://graph.org/file/218f412cc841c4f8945b9-4223b8eef03e22ff13.mp4"
HELP_VIDEO = "https://graph.org/file/218f412cc841c4f8945b9-4223b8eef03e22ff13.mp4"
ABOUT_VIDEO = "https://graph.org/file/218f412cc841c4f8945b9-4223b8eef03e22ff13.mp4"

MORE_BOTS_URL = "https://t.me/AniWorld_Bots_Hub"
OWNER_URL = "https://t.me/DA_R_KS_I_DE"
