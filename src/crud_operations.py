from sqlite3 import Error
import re


class CrudOperations:
    def __init__(self, conn):
        """Initialize the CrudOperations with an SQLite connection."""
        self.conn = conn

    def create_table(self, create_table_sql, drop_table_name=None):
        """Create a new table in the database or replace an existing table.

        Args:
            create_table_sql (str): SQL statement to create the table.
            drop_table_name (str, optional): Name of the table to drop (if it exists).

        Returns:
            None
        """
        try:
            if drop_table_name:
                c = self.conn.cursor()
                c.execute(f"DROP TABLE IF EXISTS {drop_table_name}")

            c = self.conn.cursor()
            c.execute(create_table_sql)
        except Error as e:
            print(f"Error creating table: {e}")

    def execute_sql_statement(self, sql_statement):
        """Execute an SQL statement and return the result rows.

        Args:
            sql_statement (str): SQL statement to execute.

        Returns:
            list: List of result rows.
        """
        cur = self.conn.cursor()
        cur.execute(sql_statement)
        rows = cur.fetchall()
        return rows

    def read_values(self, data_file_path):
        """Read data values from a CSV file and parse them.

        Args:
            data_file_path (str): Path to the CSV data file.

        Returns:
            list: Parsed data values.
        """
        data_values = []

        with open(data_file_path, 'r') as f:
            data = f.read()
            lines = data.split("\n")
            del lines[0]  # Remove header

            for line in lines:
                if not line.strip():
                    continue
                values_between_quotes = re.findall(r'"(.*?)"', line)
                for val in values_between_quotes:
                    rval = val.replace(',', '~')
                    line = line.replace(f'"{val}"', rval)
                line = line.split(',')
                data_values.append(line)

        return data_values
