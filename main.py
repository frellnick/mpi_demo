#main.py

from utils.db import get_db
from config import *

db = get_db()


if __name__ == "__main__":
    print(db)