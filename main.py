from DbConnector import DbConnector
from tabulate import tabulate
# from datetime import datetime


class ExampleProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def drop_table(self, table_name):
        print("Dropping table %s..." % table_name)
        query = """DROP TABLE IF EXISTS %s"""
        self.cursor.execute(query % table_name)

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))
    
    def print_table(self, table_name):
        self.cursor.execute(f"SELECT * FROM {table_name}")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def create_user_table(self):
        """Create the user table"""
        query = """CREATE TABLE IF NOT EXISTS User (
                    userID varchar(25) NOT NULL PRIMARY KEY UNIQUE,
                    hasLabels BOOLEAN
                    )
                    """

        # Add table to database
        self.cursor.execute(query)
        self.db_connection.commit()

    def create_activity_table(self):
        """Create the activity table"""
        query = """CREATE TABLE IF NOT EXISTS Activity (
                    activityID INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                    userID varchar(25) NOT NULL,
                    transportationMode varchar(25),
                    startDatetime DATETIME,
                    endDatetime DATETIME,
                    CONSTRAINT fk_user FOREIGN KEY (userID)
                    REFERENCES User(userID))
                    """

        # Add table to database
        self.cursor.execute(query)
        self.db_connection.commit()

    def create_trackpoint_table(self):
        """Create the user table"""
        # TODO how big should decimal's be?
        total_numbers = 10 #total nr. of numbers in latitude
        decimal_len = 5 # total nr. of decimals in latitude
        
        query = f"""CREATE TABLE IF NOT EXISTS TrackPoint (
                    trackPointID INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                    activityID INT NOT NULL,
                    lat DOUBLE({total_numbers},{decimal_len}),
                    lon DOUBLE({total_numbers},{decimal_len}),
                    altitude INT,
                    dateDays DOUBLE({total_numbers},{decimal_len}),
                    date_time DATETIME,
                    CONSTRAINT fk_activity FOREIGN KEY (activityID)
                    REFERENCES Activity(activityID))
                    """

        # Add table to database
        self.cursor.execute(query)
        self.db_connection.commit()

    def insert_user(self, name, has_labels):
        """insert new user, into User table.
        name should be uniqe nameID
        has_lables should be "TRUE" or "FALSE"
        """

        query = f"""INSERT INTO User (userID, hasLabels) 
                    VALUES('{name}', {has_labels})"""

        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query)
        self.db_connection.commit()

    def insert_activity(self, name, transportationMode, startDatetime, endDatetime):
        """insert new activity, 
        name should be a userID,
        transportationMode should be string,
        startDatetime should be Datetime,
        endDatetime should be Datetime
        
        Datetime should have format:  "YYYY-MM-DD HH:MM:SS"
        """

        query = f"""INSERT INTO Activity(userID, transportationMode, startDatetime, endDatetime) 
                    VALUES('{name}', '{transportationMode}', '{startDatetime}', '{endDatetime}')"""

        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query)
        self.db_connection.commit()
    
    def insert_trackpoint(self, activityID, lat, lon, altitude, dateDays, date_time):
        """insert new activity, 
        activityID
        lat (double)
        lon (double)
        altitude (int)
        dateDays (double)
        date_time (datetime)
        """

        query = f"""INSERT INTO TrackPoint(activityID, lat, lon, altitude, dateDays, date_time) 
                    VALUES({activityID}, {lat}, {lon}, {altitude}, {dateDays}, '{date_time}')"""

        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query)
        self.db_connection.commit()


def main():
    program = None

    program = ExampleProgram()

    # remove data incase error previous iteration
    program.drop_table("Activity")
    program.drop_table("User")

    program.create_user_table(table_name="User")
    program.create_activity_table(table_name="Activity")

    program.insert_user('Hans', "TRUE")
    program.insert_activity("Hans", "Pizza")

    # program.show_tables()
    program.fetch_data("User")
    program.fetch_data("Activity")

    program.drop_table("Activity")
    program.drop_table("User")

    # program.create_table(table_name="TrackPoint")

    # exit
    if program:
        program.connection.close_connection()


if __name__ == '__main__':
    main()
