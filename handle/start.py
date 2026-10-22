from pyrogram import filters
from pyrogram.handlers import MessageHandler

from config import START_TEXT, START_VIDEO
from buttons import start_button


async def start_command(client, message):

    text = START_TEXT.format(
        first_name=message.from_user.first_name
    )

    await message.reply_video(
        video=START_VIDEO,
        caption=text,
        reply_markup=start_button()
    )


def register_start(app):

    app.add_handler(
        MessageHandler(
            start_command,
            filters.command("start")
        )
    )

