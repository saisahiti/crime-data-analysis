from sqlite3 import Error
import re

class CrudOperations:
    def __init__(self, conn):
        self.conn = conn

    def create_table(self, create_table_sql, drop_table_name=None):

        if drop_table_name: # You can optionally pass drop_table_name to drop the table.
            try:
                c = self.conn.cursor()
                c.execute("""DROP TABLE IF EXISTS %s""" % (drop_table_name))
            except Error as e:
                print(e)

        try:
            c = self.conn.cursor()
            c.execute(create_table_sql)
        except Error as e:
            print(e)


    def execute_sql_statement(self, sql_statement):
        cur = self.conn.cursor()
        cur.execute(sql_statement)

        rows = cur.fetchall()

        return rows

    def read_values(self, crime_data):

        f = open(crime_data, 'r')
        data = f.read()
        lines = data.split("\n")

        #Removing the header values
        del lines[0]

        data_values = []

        for line in lines:
            if not line.strip():
                continue
            values_between_quotes = re.findall(r'"(.*?)"', line)
            for val in values_between_quotes:
                rval = val.replace(',', '~')
                line = line.replace(f'\"{val}\"', rval)
            line = line.split(',')
            data_values.append(line)

        return data_values