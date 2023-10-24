import Connection
import CrudOperations

class CreateTable:

    def __init__(self):

    def create_crime_code_table():

        data_values = read_values()

        conn_normalized = create_connection('normal.db')

        create_crime_code_table = """create table crimecode(CrimeCodeID integer not null primary key autoincrement,
                                                            CrimeCode text not null,
                                                            Description text not null)"""

        insert_crime_code_query = 'insert into crimecode(CrimeCode, Description) values(?, ?)'

        create_table(conn_normalized, create_crime_code_table, drop_table_name= 'crimecode')

        crime_code_dict = {}
        for i in range(len(data_values)):
            crime_code = data_values[i][4]
            description = data_values[i][6]
            crime_code_dict[crime_code] = description

        for key,value in crime_code_dict.items():
            conn_normalized.execute(insert_crime_code_query, (key, value, ))

        conn_normalized.commit()