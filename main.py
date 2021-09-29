import os
from DbConnector import DbConnector
from tabulate import tabulate
from decouple import config
# from datetime import datetime

from pathlib import Path

dataFolder = Path(r'D:\Desktop dump\example.txt')


class Datahandler:
    def __init__(self):
        self.handler = DBhandler()
        self.datapath = Path(config("DATA_PATH"))
        self.userpath = Path(str(self.datapath) + r"\Data")
        self.all_users = os.listdir(self.userpath) # all users in dataset
        self.tables = ["TrackPoint","Activity","User"]
    
    def create_tables(self):
        """Creates User, Activity and TrackPoint tables"""
        self.handler.create_user_table()
        self.handler.create_activity_table()
        self.handler.create_trackpoint_table()
        
    def drop_tables(self):
        """Drop all tables"""
        for table in self.tables:
            self.handler.drop_table(table)
    
    def insert_users(self):
        """Insert all user into User table,
        assumes User table exists"""
        all_users = self.all_users # create tmp which can be modified
        
        f = open(Path(str(self.datapath) + r"\labeled_ids.txt"), 'r') # get users with labels
        labeled_users = f.read().splitlines()
        f.close()
        
        #insert all users with labels
        for user in labeled_users:
            all_users.remove(user)
            self.handler.insert_user(user, "TRUE")
        
        #insert users without labels
        for user in all_users:
            self.handler.insert_user(user, "FALSE")
    
    def insert_activities(self):
        # for each user:
        for user in self.all_users:
            user_dir = Path(str(self.userpath) + rf"\{user}")
            print(user_dir)

            try:
                f = open(Path(str(user_dir) + r"\labeled_ids.txt"), 'r')
                labels = f.read().splitlines()
                f.close()
            except:
                labels = None
                
            exit(0)
            # for each trajectory file:
                # if num_lines > 2506:
                    # drop file -> break
                #add activity and then trajectories
                
                # if label starttime == .plt file name(starttime) 
                    # if endtime in .plt file match givel label end time: 
                    # (second if test instead of AND operator to avoid unnecesary
                    # file read if first condition is false.)
                        # use label(add transport information to activity)
                        
                # had to read trajectory file, might as well insert all trajectories while it's in memory


class DBhandler:
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



if __name__ == '__main__':
    data = Datahandler()
    data.insert_activities()
