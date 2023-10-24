import os
import sqlite3
from sqlite3 import Error

class Connection:
    def __init__(self, db_file):
        self.db_file = db_file

    def create_connection(self, delete_db=False):
        if delete_db and os.path.exists(self.db_file):
            os.remove(self.db_file)

        conn = None
        try:
            conn = sqlite3.connect(self.db_file)
            conn.execute("PRAGMA foreign_keys = 1")
        except Error as e:
            print(e)

        return conn