import asyncio

from pyrogram import filters
from pyrogram.handlers import MessageHandler

from config import HELP_CAPTION, HELP_VIDEO
from database.user_info import user_db
from buttons import back_button

async def start_command(client, message):

    msg = await message.reply_text("!")
    await asyncio.sleep(1)
    await msg.edit("!!")
    await asyncio.sleep(0.5)
    await msg.edit("!!!")
    await asyncio.sleep(0.5)
    await msg.edit("!!!!")
    await asyncio.sleep(0.5)
    await msg.edit("!!!!!")
    await msg.delete()

    await message.reply_video(
        video=HELP_VIDEO,
        caption=HELP_CAPTION,
        reply_markup=back_button()
    )


def register_help(app):

    app.add_handler(
        MessageHandler(
            start_command,
            filters.command("help")
        )
    )

