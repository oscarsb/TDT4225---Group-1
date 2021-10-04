"""Datahandler and DBhandler classes
for connecting to MySQL server and
inserting Geolife dataset. (Part 1)"""
import os
from pathlib import Path
from tabulate import tabulate
from decouple import config
from DbConnector import DbConnector
from haversine import haversine, Unit

import constants

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

    def db_close_connection(self):
        """Close the database connection"""
        if self.connection:
            self.connection.close_connection()
    
    def get_num(self, table_name):
        self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        return self.cursor.fetchone()[0]

    def get_avg_activities_for_user(self):
        self.cursor.execute("SELECT AVG(count) FROM (SELECT COUNT(a.id) as count FROM User as u LEFT OUTER JOIN Activity as a ON u.id = a.user_id GROUP BY u.id) as i")
        return self.cursor.fetchone()[0]

    def get_max_activities_for_user(self):
        self.cursor.execute("SELECT MAX(count) as max FROM (SELECT u.id as user, COUNT(a.user_id) as count FROM User as u LEFT OUTER JOIN Activity as a ON u.id = a.user_id GROUP BY u.id) as i")
        return self.cursor.fetchone()[0]

    def get_min_activities_for_user(self):
        self.cursor.execute("SELECT MIN(count) as max FROM (SELECT u.id as user, COUNT(a.user_id) as count FROM User as u LEFT OUTER JOIN Activity as a ON u.id = a.user_id GROUP BY u.id) as i")
        return self.cursor.fetchone()[0]

    def get_top_10_users_with_most_activities(self):
        self.cursor.execute("SELECT u.id as user, COUNT(a.user_id) as count FROM User as u LEFT OUTER JOIN Activity as a ON u.id = a.user_id GROUP BY u.id ORDER BY count DESC LIMIT 10")
        return self.cursor.fetchall()

    def ended_activity_at_the_same_day(self):
        self.cursor.execute("SELECT COUNT(*) FROM (SELECT user_id FROM Activity WHERE SUBSTRING_INDEX(start_date_time, ' ', 1) = SUBSTRING_INDEX(end_date_time, ' ', 1) GROUP BY user_id) as i")
        return self.cursor.fetchone()[0]
    
    def get_same_activities(self):
        self.cursor.execute("SELECT user_id, transportation_mode, start_date_time, end_date_time, COUNT(*) FROM Activity GROUP BY user_id, transportation_mode, start_date_time, end_date_time HAVING COUNT(*) > 1")
        return self.cursor.fetchall()

    def get_number_of_close_users(self):
        self.cursor.execute("SELECT id FROM User as u JOIN (SELECT a.user_id, t.lat, t.lon, t.altitude FROM Activity as a JOIN TrackPoint as t ON a.id = t.activity_id GROUP BY a.user_id, t.lat, t.lon, t.altitude) as i ON u.id = i.user_id")
        return self.cursor.fetchall()

    def find_users_with_no_taxi(self):
        self.cursor.execute("SELECT COUNT(*) FROM User WHERE has_labels = 'FALSE'")
        num_no_label = self.cursor.fetchone()[0]
        self.cursor.execute("SELECT COUNT(*) FROM (SELECT DISTINCT i.id, i.transportation_mode FROM (SELECT u.id, a.transportation_mode FROM User as u JOIN Activity as a ON u.id = a.user_id) as i WHERE NOT i.transportation_mode = 'NULL') as ii WHERE ii.transportation_mode = 'taxi'")
        num_has_label_but_never_taken_taxi = self.cursor.fetchone()[0]
        return num_no_label+num_has_label_but_never_taken_taxi
        
    def count_users_per_transport_mode(self):
        self.cursor.execute("SELECT a.transportation_mode, COUNT(DISTINCT(u.id)) FROM Activity as a JOIN User as u ON a.user_id = u.id WHERE NOT transportation_mode = 'NULL' GROUP BY a.transportation_mode")
        return self.cursor.fetchall()

    def find_date_with_most_activities(self):
        self.cursor.execute("SELECT YEAR(start_date_time), MONTH(start_date_time) FROM Activity GROUP BY YEAR(start_date_time), MONTH(start_date_time) ORDER by COUNT(id) DESC LIMIT 1")
        return self.cursor.fetchone()

    def find_user_with_most_activities(self):
        year, month = self.find_date_with_most_activities()
        self.cursor.execute(f"SELECT u.id, COUNT(a.id), SUM(TIMESTAMPDIFF(HOUR, a.start_date_time, a.end_date_time)) FROM Activity as a JOIN User as u WHERE a.user_id = u.id AND YEAR(a.start_date_time) = {year} AND MONTH(a.start_date_time) = {month} GROUP BY u.id ORDER BY COUNT(a.id) DESC LIMIT 2")
        return self.cursor.fetchall()

    def find_distance_walked_in_year_by_user(self, year, user_id):
        self.cursor.execute(f"SELECT t.lat, t.lon FROM TrackPoint as t INNER JOIN (SELECT * FROM Activity as a INNER JOIN (SELECT id as id_user FROM User WHERE id = {user_id}) as u ON a.user_id = u.id_user WHERE YEAR(a.start_date_time) = {year} AND a.transportation_mode = 'walk') as a on t.activity_id = a.id")
        points = self.cursor.fetchall()
        dist = 0
        for i in range(0, len(points) - 1):
            dist += haversine(points[i], points[i+1])
        return dist


if __name__ == '__main__':
    data = DBhandler()

    # Task 1 - How many users, activities and trackpoints are there in the dataset
    #print("Number of users: ", data.get_num("User"))
    #print("Number of activities: ", data.get_num("Activity"))
    #print("Number of trackpoints: ", data.get_num("TrackPoint"))

    # Task 2 - Find the average, minimum and maximum number of activities per user
    #avg_activity_for_all_users = data.get_avg_activities_for_user()
    #print("Avrage activities for all users: ", avg_activity_for_all_users, "≈", round(avg_activity_for_all_users, 1))
    #print("Maximum number of activities: ", data.get_max_activities_for_user())
    #print("Minimum number of activities: ", data.get_min_activities_for_user())

    # Task 3 - Find the top 10 users with the highest number of activities
    #print("TOP 10 users with the highest number of activities:")
    #print(tabulate(data.get_top_10_users_with_most_activities()))

    # Task 4 - Find the number of users that have started the activity in one day and ended the activity the next day
    #print("number of users that have an activity that ended the same day as it started: ", data.ended_activity_at_the_same_day())

    # Task 5 - Find activities that are registered multiple times. You should find the query even if you get zero results
    #print(tabulate(data.get_same_activities()))
    # Prints the activities in this format (user_id, transportation_mode, start_date_time, end_date_time, count) count is how many duplicates there are of this activity
    # ---  -------------------  -------------------  -
    # 181  2008-03-14 02:57:55  2008-03-14 03:43:40  2 
    # ---  -------------------  -------------------  -
    # 
    # This query does not return anything because there are no duplicate activities in the DB, but it is tested for by inserting a duplicate. The result is shown above

    # Task 6 - Find the number of users which have been close to each other in time and space (Covid-19 tracking). Close is defined as the same minute (60 seconds) and space (100 meters).
    #print(tabulate(data.get_number_of_close_users()))
    #-------------------NOT DONE!!!-----------------

    # Task 7 - Find all users that have never taken a taxi
    #print(tabulate(data.find_users_with_no_taxi()))
    #print("Number of users that has never registered the use of label taxi: ", data.find_users_with_no_taxi())

    # Task 8 - Find all types of transportation modes and count how many distinct users that have used the different transportation modes. Do not count the rows where the transportation mode is null.
    # print("Number of distinct users that have used the different transportation modes:")
    # print(tabulate(data.count_users_per_transport_mode()))

    # Task 9
    # a) - Find the year and month with the most activities.
    #print("The year and month with the most activities:")
    #print(data.find_date_with_most_activities())
    # b) - Which user had the most activities this year and month, and how many recorded hours do they have? Do they have more hours recorded than the user with the second most activities?
    #print("The two users with the most activities that year and month:")
    #print("user id, number of activities, hours recorded")
    #print(tabulate(data.find_user_with_most_activities()))

    # Task 10 - Find the total distance (in km) walked in 2008, by user with id=112.
    print(data.find_distance_walked_in_year_by_user(2008, 112))

    # Task 11 - Find the top 20 users who have gained the most altitude meters.

    # Task 12 - Find all users who have invalid activities, and the number of invalid activities per user.

    # Close the db connection
    data.db_close_connection()

    # data = Datahandler()
    # data.drop_tables()  # make sure db is clean
    # data.create_tables()
    # data.insert_users()
    # data.insert_activities_and_trackpoints()
    # data.db_close_connection()
