#main.py

from utils.db import get_db


db = get_db()


if __name__ == "__main__":
    print(db)