"""Datahandler and DBhandler classes
for connecting to MySQL server and
inserting Geolife dataset. (Part 1)"""
import os
from pathlib import Path
from tabulate import tabulate
from decouple import config
from DbConnector import DbConnector


class Datahandler:
    """Class for parsing Geolife data"""

    def __init__(self):
        self.handler = DBhandler()
        self.datapath = Path(config("DATA_PATH"))
        self.userpath = Path(str(self.datapath) + r"\Data")
        self.all_users = os.listdir(self.userpath)  # all users in dataset

        with open(Path(str(self.datapath) + r"\labeled_ids.txt"), 'r') as f:
            self.labeled_users = f.read().splitlines()  # all users with labels

        self.tables = ["TrackPoint", "Activity", "User"]

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

        all_users = self.all_users.copy()  # create tmp which can be modified

        # insert all users with labels
        for user in self.labeled_users:
            all_users.remove(user)
            self.handler.insert_user(user, "TRUE")

        # insert users without labels
        for user in all_users:
            self.handler.insert_user(user, "FALSE")

    def _format_labels(self, user, user_dir):
        """Read label file and format data"""
        label_formated = []  # all labels, format: ["starttime","endtime", "transportMode"]
        if user in self.labeled_users:
            # only if user is labeled user

            with open(Path(str(user_dir) + r"\labels.txt"), 'r') as f:
                raw_label = f.read().splitlines()[1:]

            for line in raw_label:
                # format all label data to match dataset
                list_line = list(line)  # turn str into list
                list_line[10] = "_"
                list_line[30] = "_"

                line = "".join(list_line)  # add changes
                line = line.split()

                tmp_label = []
                # replace characters with correct symbols
                for i in line:
                    val = i.replace("/", "-")
                    val = val.replace("_", " ")
                    tmp_label.append(val)

                label_formated.append(tmp_label)  # add formated label data

        return label_formated

    def insert_activities_and_trackpoints(self):
        """"Function for adding Activities and matching trackPoints"""

        activity_count = 0  # activity count to keep track of activity id

        for user in self.all_users:
            # data directory of given user
            user_dir = Path(str(self.userpath) + rf"\{user}")

            # all activity files of given user
            traj_path = Path(str(user_dir) + r"\Trajectory")
            files = os.listdir(traj_path)

            label_formated = self._format_labels(user, user_dir)

            for file in files:
                with open(Path(str(traj_path) + rf"\{file}")) as f:
                    track_raw = f.read().splitlines()[6:]

                # if activity is to large, ignore it
                if len(track_raw) > 2500:
                    continue

                track_points = []
                # format each trackPoint and add all values to list
                for line in track_raw:
                    formated = line.replace(",", " ").split()
                    # change date and time format to match datetime
                    formated[5] = f"{formated[5]} {formated[6]}"
                    formated.remove(formated[-1])  # remove time

                    track_points.append(formated)

                transportation = "NULL"

                # if file match label entry, add the transportation mode
                for label in label_formated:
                    if label[0] == track_points[0][5] and label[1] == track_points[-1][5]:
                        transportation = label[2]
                        break

                # add activity
                self.handler.insert_activity(
                    name=user,
                    transportationMode=transportation,
                    startDatetime=track_points[0][5],
                    endDatetime=track_points[-1][5])
                activity_count += 1

                # add all trackpoints to db
                self.handler.insert_trackpoints(activity_count, track_points)

    def db_close_connection(self):
        self.handler.db_close_connection()
        print("Database connection closed..")


class DBhandler:
    """Class for interacting
        with MySQL database"""

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
        """Print all values of given table"""
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
        total_numbers = 20  # total nr. of numbers in latitude
        decimal_len = 10  # total nr. of decimals in latitude

        query = f"""CREATE TABLE IF NOT EXISTS TrackPoint (
                    trackPointID INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                    activityID INT NOT NULL,
                    lat DOUBLE({total_numbers},{decimal_len}),
                    lon DOUBLE({total_numbers},{decimal_len}),
                    altitude INT,
                    date_time DATETIME,
                    CONSTRAINT fk_activity FOREIGN KEY (activityID)
                    REFERENCES Activity(activityID))
                    """

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

        self.cursor.execute(query)
        self.db_connection.commit()

    def insert_trackpoint(self, activityID, lat, lon, altitude, date_time):
        """insert new single trackpoint,
        activityID
        lat (double)
        lon (double)
        altitude (int)
        date_time (datetime)
        """

        query = f"""INSERT INTO TrackPoint(activityID, lat, lon, altitude, date_time)
                    VALUES({activityID}, {lat}, {lon}, {altitude}, '{date_time}')"""

        self.cursor.execute(query)
        self.db_connection.commit()

    def insert_trackpoints(self, activityID, track_list):
        """insert multiple trackpoints, with
        activityID (foreign activity id)
        tracklist (list on format: [lat, lon, 0, altitude, date_days, date_time)]
        """
        query = """INSERT INTO TrackPoint(activityID, lat, lon, altitude, date_time)
                    VALUES"""

        # insert all trackPoints into query
        for i, track in enumerate(track_list):
            value = f"""({activityID}, {track[0]},
                {track[1]},{track[3]},'{track[5]}')"""

            if i + 1 != len(track_list):
                value = value + ","

            query = query + value

        # execute query
        self.cursor.execute(query)

        self.db_connection.commit()

    def db_close_connection(self):
        """Close the database connection"""
        if self.connection:
            self.connection.close_connection()


if __name__ == '__main__':
    """Insert all data in database
    OBS: drops existing tables!"""
    pass
    # data = Datahandler()
    # data.drop_tables()  # make sure db is clean
    # data.create_tables()
    # data.insert_users()
    # data.insert_activities_and_trackpoints()
    # data.db_close_connection()
