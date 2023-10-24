from connection import Connection
from crud_operations import CrudOperations


class CreateTable:

    def __init__(self, db_file, csv_file):
        self.db_file = db_file
        self.csv_file = csv_file
        self.conn = None

    def _connect(self):
        if self.conn is None:
            conn_normalized = Connection()
            self.conn = conn_normalized.create_connection(self.db_file)

    def create_crime_code_table(self):
        # Create a connection and initialize the necessary components
        self._connect()
        crud_op = CrudOperations(self.conn)

        # Read data from the CSV file
        data_values = crud_op.read_values(self.csv_file)

        # Define SQL statements and queries
        create_crime_code_table = """create table crimecode(CrimeCodeID integer not null primary key autoincrement,
                                    CrimeCode text not null,
                                    Description text not null)"""
        insert_crime_code_query = 'insert into crimecode(CrimeCode, Description) values(?, ?)'

        # Create the crime code table
        crud_op.create_table(create_crime_code_table, drop_table_name='crimecode')

        # Extract data from the CSV file and populate the table
        crime_code_dict = {}
        for i in range(len(data_values)):
            crime_code = data_values[i][4]
            description = data_values[i][6]
            crime_code_dict[crime_code] = description

        for key, value in crime_code_dict.items():
            self.conn.execute(insert_crime_code_query, (key, value,))

        # Commit the changes to the database
        self.conn.commit()

    def create_crime_code_dict(self):

        self._connect()
        crud_op = CrudOperations(self.conn)

        select_crime_code_query = 'select CrimeCode, CrimeCodeID from crimecode;'

        crime_code_values = crud_op.execute_sql_statement(select_crime_code_query)

        return dict(crime_code_values)

    def create_location_table(self):

        self._connect()
        crud_op = CrudOperations(self.conn)

        data_values = crud_op.read_values(self.csv_file)

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

        crud_op.create_table(create_location_table, drop_table_name='location')

        for key, value in loc_values.items():
            self.conn.execute(insert_location_query, (key, value[0], value[1],))

        self.conn.commit()

    def create_location_dict(self):

        self._connect()
        crud_op = CrudOperations(self.conn)

        select_location_query = 'select Location, LocationID from location;'

        location_values = crud_op.execute_sql_statement(select_location_query)

        return dict(location_values)

    def create_race_table(self):

        self._connect()
        crud_op = CrudOperations(self.conn)

        data_values = crud_op.read_values(self.csv_file)

        create_race_table = """create table race(RaceID integer not null primary key autoincrement,
                                                        Race text not null,
                                                        Ethnicity text);"""

        insert_race_query = 'insert into race(Race, Ethnicity) values(?, ?)'

        eth_race_dict = {}

        for i in range(len(data_values)):
            race = data_values[i][12]
            ethnicity = data_values[i][13]
            if race == '':
                eth_race_dict['UNKNOWN_RACE'] = ethnicity
            else:
                eth_race_dict[race] = ethnicity

        crud_op.create_table(create_race_table, drop_table_name='race')

        for key, value in eth_race_dict.items():
            self.conn.execute(insert_race_query, (key, value,))

        self.conn.commit()

    def create_race_dict(self):

        self._connect()
        crud_op = CrudOperations(self.conn)

        select_race_query = 'select Race, RaceID from race;'

        race_values = crud_op.execute_sql_statement(select_race_query)

        return dict(race_values)

    def create_criminal_table(self):

        self._connect()
        crud_op = CrudOperations(self.conn)

        data_values = crud_op.read_values(self.csv_file)

        race_values = self.create_race_dict()

        create_criminal_table = """create table criminal(CriminalID integer not null primary key autoincrement,
                                                    Gender text,
                                                    Age text,
                                                    RaceID text,
                                                    foreign key(RaceID) references race(RaceID));"""

        insert_criminal_query = 'insert into criminal(Gender, Age, RaceID) values(?, ?, ?)'

        crud_op.create_table(create_criminal_table, drop_table_name='criminal')

        criminal_data = []
        for i in range(len(data_values)):
            gender = data_values[i][10]
            age = data_values[i][11]
            race = data_values[i][12]
            if race == '':
                race = 'UNKNOWN_RACE'
            race_id = race_values[race]
            criminal_data.append([gender, age, race_id])

        criminal_data = [tuple(criminal) for criminal in criminal_data]

        with self.conn:
            cur = self.conn.cursor()
            cur.executemany(insert_criminal_query, criminal_data)

    def create_criminal_dict(self):

        self._connect()
        crud_op = CrudOperations(self.conn)

        select_criminal_query = 'select Gender, CriminalID from criminal;'

        criminal_values = crud_op.execute_sql_statement(select_criminal_query)

        return dict(criminal_values)

    def create_crime_table(self):

        self._connect()
        crud_op = CrudOperations(self.conn)

        data_values = crud_op.read_values(self.csv_file)

        crime_code_values = self.create_crime_code_dict()

        location_values = self.create_location_dict()

        criminal_values = self.create_criminal_dict()

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

        crud_op.create_table(create_crime_table, drop_table_name='crime')

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

        crime_data = sorted(crime_data, key=lambda c: c[0])

        with self.conn:
            cur = self.conn.cursor()
            cur.executemany(insert_crime_query, crime_data)
