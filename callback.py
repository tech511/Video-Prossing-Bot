from pyrogram import filters
from pyrogram.handlers import CallbackQueryHandler
from pyrogram.types import InputMediaVideo

from config import (
    START_TEXT,
    HELP_CAPTION,
    ABOUT_CAPTION,
    START_VIDEO,
    HELP_VIDEO,
    ABOUT_VIDEO
)

from buttons import (
    start_button,
    back_button,
    help_button
)


async def callback_handler(client, query):

    # Help Center
    if query.data == "help":

        await query.answer()

        await query.message.edit_media(
            media=InputMediaVideo(
                media=HELP_VIDEO,
                caption=HELP_CAPTION
            ),
            reply_markup=help_button()
        )

    # About screen 
    elif query.data == "about":

        await query.answer()

        await query.message.edit_media(
            media=InputMediaVideo(
                media=ABOUT_VIDEO,
                caption=ABOUT_CAPTION
            ),
            reply_markup=back_button()
        )
        
    # Return to main menu
    elif query.data == "back":

        await query.answer()

        text = START_TEXT.format(
            first_name=query.from_user.first_name
        )

        await query.message.edit_media(
            media=InputMediaVideo(
                media=START_VIDEO,
                caption=text
            ),
            reply_markup=start_button()
        )

    # Close menu
    elif query.data == "close":

        await query.answer("Menu closed!")
        await query.message.delete()


def register_callbacks(app):

    app.add_handler(
        CallbackQueryHandler(
            callback_handler,
            filters.regex(
                r"^(help|about|back|close)$"
            )
        )
    )

