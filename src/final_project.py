import sqlite3
import pandas as pd



    
#create_crime_code_table()  
    
    

def create_crime_code_dict():

    conn_normalized = create_connection('normal.db')

    select_crime_code_query = 'select CrimeCode, CrimeCodeID from crimecode;'

    crime_code_values = execute_sql_statement(select_crime_code_query, conn_normalized)

    return dict(crime_code_values)

#create_crime_code_dict()

def create_location_table():

    data_values = read_values()

    conn_normalized = create_connection('normal.db')

    create_location_table = """create table location(LocationID integer not null primary key autoincrement,
                                                    Location text not null,
                                                    District text,
                                                    Neighborhood text);"""

    insert_location_query = 'insert into location(Location, District, Neighborhood) values(?, ?, ?)'

    loc_values = {}
    for i in range(len(data_values)):
        location = data_values[i][5]
        district = data_values[i][14]
        neighborhood = data_values[i][15]
        loc_values[location] = ([district, neighborhood])

    create_table(conn_normalized, create_location_table, drop_table_name= 'location')

    for key,value in loc_values.items():
        conn_normalized.execute(insert_location_query, (key, value[0], value[1], ))
    
    conn_normalized.commit()
    
#create_location_table()

def create_location_dict():

    conn_normalized = create_connection('normal.db')

    select_location_query = 'select Location, LocationID from location;'

    location_values = execute_sql_statement(select_location_query, conn_normalized)

    return dict(location_values)

#create_location_dict()

def create_race_table():

    data_values = read_values()

    conn_normalized = create_connection('normal.db')

    create_race_table = """create table race(RaceID integer not null primary key autoincrement,
                                                    Race text not null,
                                                    Ethnicity text);"""

    insert_race_query = 'insert into race(Race, Ethnicity) values(?, ?)'

    eth_race_dict = {}

    for i in range(len(data_values)):
        race = data_values[i][12]
        ethnicity = data_values[i][13]
        if(race == ''):
            eth_race_dict['UNKNOWN_RACE'] = ethnicity
        else:
            eth_race_dict[race] = ethnicity

    create_table(conn_normalized, create_race_table, drop_table_name= 'race')

    for key,value in eth_race_dict.items():
        conn_normalized.execute(insert_race_query, (key, value, ))
    
    conn_normalized.commit()
    
    conn_normalized.commit()

#create_race_table()

def create_race_dict():

    conn_normalized = create_connection('normal.db')

    select_race_query = 'select Race, RaceID from race;'

    race_values = execute_sql_statement(select_race_query, conn_normalized)

    return dict(race_values)

#create_race_dict()


def create_criminal_table():

    data_values = read_values()

    conn_normalized = create_connection('normal.db')

    race_values = create_race_dict()

    create_criminal_table = """create table criminal(CriminalID integer not null primary key autoincrement,
                                                Gender text,
                                                Age text,
                                                RaceID text,
                                                foreign key(RaceID) references race(RaceID));"""

    insert_criminal_query = 'insert into criminal(Gender, Age, RaceID) values(?, ?, ?)'

    create_table(conn_normalized, create_criminal_table, drop_table_name= 'criminal')

    criminal_data = []
    for i in range(len(data_values)):
        gender = data_values[i][10]
        age = data_values[i][11]
        race = data_values[i][12]
        if(race == ''):
            race = 'UNKNOWN_RACE'
        race_id = race_values[race]
        criminal_data.append([gender, age, race_id])

    criminal_data = [tuple(criminal) for criminal in criminal_data]

    with conn_normalized:
        cur = conn_normalized.cursor()
        cur.executemany(insert_criminal_query, criminal_data)
        
#create_criminal_table()
    

def create_criminal_dict():

    conn_normalized = create_connection('normal.db')

    select_criminal_query = 'select Gender, CriminalID from criminal;'

    criminal_values = execute_sql_statement(select_criminal_query, conn_normalized)

    return dict(criminal_values)

#create_criminal_dict()

def create_crime_table():

    data_values = read_values()

    crime_code_values = create_crime_code_dict()

    location_values = create_location_dict()

    criminal_values = create_criminal_dict()

    conn_normalized = create_connection('normal.db')

    create_crime_table = """create table crime(CrimeID integer not null primary key autoincrement, 
                                                CrimeDateTime text not null, 
                                                Inside_Outside text, 
                                                Weapon text,
                                                CrimeCodeID integer not null,
                                                LocationID integer not null,
                                                CriminalID integer not null,
                                                foreign key(CrimeCodeID) references crimecode(CrimeCodeID),
                                                foreign key(LocationID) references location(LocationID),
                                                foreign key(CriminalID) references criminal(CriminalID));"""

    insert_crime_query = 'insert into crime values(null, ?, ?, ?, ?, ?, ?)'

    create_table(conn_normalized, create_crime_table, drop_table_name= 'crime')

    crime_data = []
    for i in range(len(data_values)):
        crime_date_time = data_values[i][3]
        crime_code = data_values[i][4]
        inside_outside = data_values[i][7]
        weapon = data_values[i][8]
        location = data_values[i][5]
        gender = data_values[i][10]
        crime_code_id = crime_code_values[crime_code]
        location_id = location_values[location]
        criminal_id = criminal_values[gender]
        crime_data.append([crime_date_time, inside_outside, weapon, crime_code_id, location_id, criminal_id])

    crime_data = [tuple(crime) for crime in crime_data]

    crime_data = sorted(crime_data, key = lambda c:c[0])

    with conn_normalized:
        cur = conn_normalized.cursor()
        cur.executemany(insert_crime_query, crime_data)


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