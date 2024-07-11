from telegram import User


def with_default(s: str = ""):
    return s


def get_user_familiar(user: User) -> str:
    return f"{with_default(user.first_name)} {with_default(user.last_name)}"
