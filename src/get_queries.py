import sqlite3
import pandas as pd

#create_crime_code_table()

#create_crime_code_dict()

#create_location_dict()
    
#create_location_table()

#create_race_table()
#create_race_dict()
# create_criminal_table()

# create_criminal_dict()

#create_crime_table()

def get_all_crimecode_data():
    conn = sqlite3.connect('normal.db')
    sql = 'select Description, group_concat(CrimeCode) AS codes from crimecode group by Description ORDER BY codes'
    conn_cur = conn.cursor()
    conn_cur.execute(sql)
    print(conn_cur.fetchall())

#get_all_crimecode_data()

def get_least_crime_committing_race():
    conn = sqlite3.connect('normal.db')
    sql = """select t.Race, count(*) As number_of_crimes From Race as t
             JOIN criminal as ti
             ON t.RaceID = ti.RaceID
             GROUP BY ti.RaceID
             ORDER BY number_of_crimes limit 3"""
    query = pd.read_sql_query(sql, conn)
    df = pd.DataFrame(query)
    return df

#get_least_crime_committing_race()


def get_crimes_by_gender():
    conn = sqlite3.connect('normal.db')
    sql = """ SELECT sum(criminal.Gender = 'Male') OR sum(criminal.Gender = 'M') as male, sum(criminal.Gender = 'Female') or sum(criminal.Gender = 'F') as female, criminal.Age
              FROM criminal
              WHERE criminal.Age >= 0 and criminal.age < 90 
              GROUP BY criminal.Age"""
    query = pd.read_sql_query(sql, conn)
    df = pd.DataFrame(query)
    print(df)


get_crimes_by_gender()