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

        self.all_users = os.listdir(self.userpath)  # all users in dataset

        # get users with labels, TODO startup function
        f = open(Path(str(self.datapath) + r"\labeled_ids.txt"), 'r')
        self.labeled_users = f.read().splitlines()  # all users with labels
        f.close()

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

    def insert_activities_and_trackpoints(self):
        tmp_all_users = self.all_users.copy()
        activity_count = 0  # activity count to keep track of activity id

        # if label starttime == .plt file name(starttime)
        # if endtime in .plt file match givel label end time:
        # (second if test instead of AND operator to avoid unnecesary
        # file read if first condition is false.)
        # use label(add transport information to activity)

        for user in self.labeled_users:
            user_dir = Path(str(self.userpath) + rf"\{user}")
            print(user)
            print(f"{len(tmp_all_users)} len: {len(self.labeled_users)}")
            tmp_all_users.remove(user)

            # trajectory files
            traj_path = Path(str(user_dir) + r"\Trajectory")
            files = os.listdir(traj_path)

            # open labels
            f = open(Path(str(user_dir) + r"\labels.txt"), 'r')
            rawLabel = f.read().splitlines()[1:]

            labelFormated = []

            for line in rawLabel:
                # format all label data to match dataset
                list_line = list(line)  # turn str into list
                list_line[10] = "_"
                list_line[30] = "_"

                line = "".join(list_line)  # add changes
                line = line.split()

                tmp = []
                # replace characters with correct symbols
                for i in line:
                    val = i.replace("/", "-")
                    val = val.replace("_", " ")
                    tmp.append(val)

                labelFormated.append(tmp)  # add formated label data

            for label in labelFormated:
                # look for matching label file
                # (file name = YYYYMMDDHHMMSS.plt)
                filename = label[0].replace(
                    "-", "").replace(":", "").replace(" ", "") + ".plt"

                transport = "NULL"

                # check if filename exists
                if filename in files:
                    f = open(Path(str(traj_path) + rf"\{filename}"))
                    trackRaw = f.read().splitlines()[6:]
                    f.close()

                    # if activity is to large, ignore it
                    if len(trackRaw) > 2500:
                        continue

                    trackPoints = []

                    # format each trackpoint and add all values to list
                    for line in trackRaw:
                        formated = line.replace(",", " ").split()
                        # change date and time format to match datetime
                        formated[5] = f"{formated[5]} {formated[6]}"
                        formated.remove(formated[-1])  # remove time

                        trackPoints.append(formated)
                    # file exists, check if endtime is correct

                    # fetch datetime from last line
                    endDate = trackRaw[-1].replace(",", " ").split()
                    endDate = f"{endDate[5]} {endDate[6]}"

                    # check if label match
                    if endDate == label[1]:
                        transport = label[2]

                    # add activity
                    self.handler.insert_activity(
                        name=user, transportationMode=transport, startDatetime=trackPoints[0][5], endDatetime=trackPoints[-1][5])
                    activity_count += 1

                    # add all trajectories for given activity
                    self.handler.insert_trackpoints(
                        activity_count, trackPoints)

            for file in files:
                f = open(Path(str(traj_path) + rf"\{file}"))
                trackRaw = f.read().splitlines()[6:]
                f.close()

                # if activity is to large, ignore it
                if len(trackRaw) > 2500:
                    continue

                trackPoints = []

                # format each trackpoint and add all values to list
                for line in trackRaw:
                    formated = line.replace(",", " ").split()
                    # change date and time format to match datetime
                    formated[5] = f"{formated[5]} {formated[6]}"
                    formated.remove(formated[-1])  # remove time

                    trackPoints.append(formated)

                # add activity
                self.handler.insert_activity(
                    name=user, transportationMode=transport, startDatetime=trackPoints[0][5], endDatetime=trackPoints[-1][5])
                activity_count += 1

                # add all trajectories for given activity
                self.handler.insert_trackpoints(
                    activity_count, trackPoints)

        # for each user:
        for user in tmp_all_users:
            user_dir = Path(str(self.userpath) + rf"\{user}")

            # go through all of the user's activeties/trajectories
            traj_path = Path(str(user_dir) + r"\Trajectory")
            files = os.listdir(traj_path)

            for file in files:
                f = open(Path(str(traj_path) + rf"\{file}"))
                trackRaw = f.read().splitlines()[6:]
                f.close()

                # if activity is to large, ignore it
                if len(trackRaw) > 2500:
                    continue

                trackPoints = []
                # format each trajectory and add all values to list
                for line in trackRaw:
                    formated = line.replace(",", " ").split()
                    # change date and time format to match datetime
                    formated[5] = f"{formated[5]} {formated[6]}"
                    formated.remove(formated[-1])  # remove time

                    trackPoints.append(formated)

                # add activity
                self.handler.insert_activity(
                    name=user, transportationMode="NULL", startDatetime=trackPoints[0][5], endDatetime=trackPoints[-1][5])
                activity_count += 1

                # add all trajectories for given activity
                # for traj in trajectories:
                #     self.handler.insert_trackpoint(
                #         activityID=activity_count, lat=traj[0], lon=traj[1], altitude=traj[3], date_time=traj[5])
                self.handler.insert_trackpoints(activity_count, trackPoints)
                self.handler.print_table("TrackPoint")
                self.drop_tables()
                exit(0)


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

    def insert_trackpoints(self, activityID, trackList):
        """insert multiple trackpoints, with 
        activityID (foreign activity id)
        tracklist (list on format: [lat, lon, 0, altitude, date_days, date_time)]
        """
        query = f"""INSERT INTO TrackPoint(activityID, lat, lon, altitude, date_time) 
                    VALUES"""

        # insert all trackPoints into query
        for track in trackList:
            value = f"({activityID},{track[0]}, {track[1]}, {track[3]}, '{track[5]}')"
            if track != trackList[-1]:
                value = value + ","

            query = query + value

        # execute query
        self.cursor.execute(query)
        self.db_connection.commit()


if __name__ == '__main__':
    data = Datahandler()
    data.drop_tables()  # make sure db is clean
    data.create_tables()
    data.insert_users()
    data.insert_activities_and_trackpoints()
