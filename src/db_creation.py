from create_table import CreateTable

createTable = CreateTable('crimeData.db', './data/Crime_Data.csv')

createTable.create_crime_code_table()

#createTable.create_crime_code_dict()

createTable.create_location_table()

#createTable.create_location_dict()

createTable.create_race_table()

#createTable.create_race_dict()

createTable.create_criminal_table()

#createTable.create_criminal_dict()

createTable.create_crime_table()