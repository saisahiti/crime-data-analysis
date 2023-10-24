import os
import sqlite3
from sqlite3 import Error

class Connection:
    def __init__(self):
        pass

    def create_connection(self, db_file, delete_db=False):
        if delete_db and os.path.exists(db_file):
            os.remove(db_file)

        conn = None
        try:
            conn = sqlite3.connect(db_file)
            conn.execute("PRAGMA foreign_keys = 1")
            return conn  # Return the connection if successful
        except Error as e:
            print(e)
            return None  # Return None in case of an error
