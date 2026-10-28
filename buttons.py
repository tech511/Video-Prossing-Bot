from pyrogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton
)

from config import MORE_BOTS_URL, OWNER_URL


def start_button():

    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "• ʜᴇʟᴘ •",
                callback_data="help"
            ),
            InlineKeyboardButton(
                "• ᴀʙᴏᴜᴛ •",
                callback_data="about"
            )
        ],
        [
            InlineKeyboardButton(
                "• ᴍᴏʀᴇ ʙᴏᴛꜱ •",
                url=MORE_BOTS_URL
            )
        ],
        [
            InlineKeyboardButton(
                "• ᴏᴡɴᴇʀ •",
                url=OWNER_URL
            ),
            InlineKeyboardButton(
                "• ᴄʟᴏꜱᴇ •",
                callback_data="close"
            )
        ]
    ])


def help_button():
    # The Help screen only needs Back / Close, same as the About screen.
    return back_button()


def back_button():

    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "• ʙᴀᴄᴋ •",
                callback_data="back"
            ),
            InlineKeyboardButton(
                "• ᴄʟᴏꜱᴇ •",
                callback_data="close"
            )
        ]
    ])

  
