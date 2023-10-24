import sqlite3
import pandas as pd

class GetQueries():

    def __init__(self, db_file):
        self.db_file = db_file

    def get_all_crimecode_data(self):
        conn = sqlite3.connect(self.db_file)
        sql = 'select Description, group_concat(CrimeCode) AS codes from crimecode group by Description ORDER BY codes'
        conn_cur = conn.cursor()
        conn_cur.execute(sql)
        print(conn_cur.fetchall())

    def get_least_crime_committing_race(self):
        conn = sqlite3.connect(self.db_file)
        sql = """select t.Race, count(*) As number_of_crimes From Race as t
                JOIN criminal as ti
                ON t.RaceID = ti.RaceID
                GROUP BY ti.RaceID
                ORDER BY number_of_crimes limit 3"""
        query = pd.read_sql_query(sql, conn)
        df = pd.DataFrame(query)
        return df

    def get_crimes_by_gender(self):
        conn = sqlite3.connect(self.db_file)
        sql = """ SELECT sum(criminal.Gender = 'Male') OR sum(criminal.Gender = 'M') as male, sum(criminal.Gender = 'Female') or sum(criminal.Gender = 'F') as female, criminal.Age
                FROM criminal
                WHERE criminal.Age >= 0 and criminal.age < 90 
                GROUP BY criminal.Age"""
        query = pd.read_sql_query(sql, conn)
        df = pd.DataFrame(query)
        print(df)
