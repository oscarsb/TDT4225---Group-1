import math
from tabulate import tabulate
from DbConnector import DbConnector
from haversine import haversine, Unit
from tqdm import tqdm

class DBhandler:
    """Class for interacting
        with MySQL database"""

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def db_close_connection(self):
        """Close the database connection"""
        if self.connection:
            self.connection.close_connection()
    
    def get_num(self, table_name):
        self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        return self.cursor.fetchone()[0]

    def get_avg_activities_for_user(self):
        self.cursor.execute("""SELECT AVG(count)
                               FROM (
                                   SELECT COUNT(a.id) as count 
                                   FROM User as u 
                                   LEFT OUTER JOIN Activity as a 
                                   ON u.id = a.user_id 
                                   GROUP BY u.id) as i""")
        return self.cursor.fetchone()[0]

    def get_max_activities_for_user(self):
        self.cursor.execute("""SELECT MAX(count) as max 
                               FROM (
                                   SELECT u.id as user, COUNT(a.user_id) as count 
                                   FROM User as u 
                                   LEFT OUTER JOIN Activity as a 
                                   ON u.id = a.user_id 
                                   GROUP BY u.id) as i""")
        return self.cursor.fetchone()[0]

    def get_min_activities_for_user(self):
        self.cursor.execute("""SELECT MIN(count) as max 
                               FROM (
                                   SELECT u.id as user, COUNT(a.user_id) as count 
                                   FROM User as u 
                                   LEFT OUTER JOIN Activity as a 
                                   ON u.id = a.user_id 
                                   GROUP BY u.id) as i""")
        return self.cursor.fetchone()[0]

    def get_top_10_users_with_most_activities(self):
        self.cursor.execute("""SELECT u.id as user, COUNT(a.user_id) as count 
                               FROM User as u 
                               JOIN Activity as a 
                               ON u.id = a.user_id 
                               GROUP BY u.id 
                               ORDER BY count 
                               DESC 
                               LIMIT 10""")
        return self.cursor.fetchall()

    def ended_activity_at_the_next_day(self):
        self.cursor.execute("""SELECT COUNT(*) 
                               FROM (
                                   SELECT user_id 
                                   FROM Activity 
                                   WHERE DATEDIFF(start_date_time, end_date_time) = -1
                                   GROUP BY user_id) as i""")

        return self.cursor.fetchone()[0]
    
    def get_same_activities(self):
        self.cursor.execute("""SELECT user_id, transportation_mode, start_date_time, end_date_time, COUNT(*) 
                               FROM Activity 
                               GROUP BY user_id, transportation_mode, start_date_time, end_date_time 
                               HAVING COUNT(*) > 1""")
        return self.cursor.fetchall()

    def get_number_of_close_users(self):

        def within60s(user1_pos, user2_pos):
            start_time1, end_time1 = user1_pos[1], user1_pos[2]
            start_time2, end_time2 = user2_pos[1], user2_pos[2]
            return -60 < (start_time1 - end_time2).seconds < 60 or -60 < (start_time2 - end_time1).seconds < 60

        def within100m(user1_pos, user2_pos):
            lat1, lon1, altitude1 = user1_pos[3], user1_pos[4], user1_pos[5]
            lat2, lon2, altitude2 = user2_pos[3], user2_pos[4], user2_pos[5]
            flat_distance = haversine((lat1, lon1), (lat2, lon2), unit=Unit.METERS)
            euclidian_distance = math.sqrt(flat_distance**2 + (altitude2 - altitude1)**2)
            return euclidian_distance < 100

        self.cursor.execute("""SELECT u.id, a.start_date_time, a.end_date_time, t.lat, t.lon, t.altitude 
                               FROM User as u 
                               JOIN Activity as a 
                               ON u.id = a.user_id 
                               JOIN TrackPoint as t 
                               ON a.id = t.activity_id ORDER BY u.id""")
        user_positions = self.cursor.fetchall()
        close_users = set([])
        change_user_indeces = []

        for i in range(1, len(user_positions)):
            if user_positions[i][0] != user_positions[i-1][0]:
                change_user_indeces.append(i)

        for i in tqdm(range(len(user_positions)), ncols=100, leave=False):
            user1_pos = user_positions[i]
            user1 = user1_pos[0]
            next_user_index = change_user_indeces[int(user1)]
            for user2_pos in user_positions[next_user_index:]:
                user2 = user2_pos[0]
                if within60s(user1_pos, user2_pos) and within100m(user1_pos, user2_pos):
                    close_users.update([user1, user2])
        
        return len(close_users)

    def find_users_with_no_taxi(self):
        self.cursor.execute("""SELECT u.id 
                               FROM User as u 
                               WHERE NOT EXISTS (
                                   SELECT a.id 
                                   FROM Activity as a 
                                   WHERE u.id = a.user_id 
                                   AND a.transportation_mode = 'taxi')""")
        return self.cursor.fetchall()
        
    def count_users_per_transport_mode(self):
        self.cursor.execute("""SELECT a.transportation_mode, COUNT(DISTINCT(u.id)) 
                               FROM Activity as a 
                               JOIN User as u 
                               ON a.user_id = u.id 
                               WHERE NOT transportation_mode = 'NULL' 
                               GROUP BY a.transportation_mode""")
        return self.cursor.fetchall()

    def find_date_with_most_activities(self):
        self.cursor.execute("""SELECT YEAR(start_date_time), MONTH(start_date_time) 
                               FROM Activity 
                               GROUP BY YEAR(start_date_time), MONTH(start_date_time) 
                               ORDER BY COUNT(id) 
                               DESC 
                               LIMIT 1""")
        return self.cursor.fetchone()

    def find_user_with_most_activities(self):
        year, month = self.find_date_with_most_activities()
        self.cursor.execute(f"""SELECT u.id, COUNT(a.id) as count, SUM(TIMESTAMPDIFF(HOUR, a.start_date_time, a.end_date_time)) 
                                FROM Activity as a 
                                JOIN User as u 
                                WHERE a.user_id = u.id 
                                AND YEAR(a.start_date_time) = {year} 
                                AND MONTH(a.start_date_time) = {month} 
                                GROUP BY u.id 
                                ORDER BY count 
                                DESC 
                                LIMIT 2""")
        return self.cursor.fetchall()

    def find_distance_walked_in_year_by_user(self, year, user_id):
        self.cursor.execute(f"""SELECT t.lat, t.lon 
                                FROM TrackPoint as t 
                                JOIN (
                                    SELECT * 
                                    FROM Activity as a 
                                    WHERE a.user_id = {user_id} 
                                    AND YEAR(a.start_date_time) = {year} 
                                    AND a.transportation_mode = 'walk') as a 
                                ON t.activity_id = a.id""")
        points = self.cursor.fetchall()
        dist = 0
        for i in range(1, len(points)):
            dist += haversine(points[i], points[i-1])
        return dist

    def find_20_users_with_most_altitude_gain(self):
        user_altitudes = {}
        self.cursor.execute("""SELECT User.id 
                               FROM User""")
        user_ids = self.cursor.fetchall()
        for uid in tqdm(user_ids, ncols=100, leave=False):
            gained = 0
            self.cursor.execute(f"""SELECT t.altitude 
                                    FROM Activity as a 
                                    JOIN TrackPoint as t 
                                    ON t.activity_id = a.id 
                                    WHERE a.user_id = {uid[0]}""")
            altitudes = self.cursor.fetchall()
            for i in range(1, len(altitudes)):
                if altitudes[i][0] > altitudes[i-1][0]:
                    gained += altitudes[i][0] - altitudes[i-1][0]
            user_altitudes[uid[0]] = round(gained*0.3048)

        sorted_altitudes = dict(sorted(user_altitudes.items(), key=lambda item: item[1], reverse=True))
        return [(list(sorted_altitudes.keys())[i], list(sorted_altitudes.values())[i]) for i in range(0, 20)]

    def find_all_users_with_invalid_activities(self): 
        self.cursor.execute("""SELECT a.user_id, start_date_time, end_date_time, activity_id 
                               FROM Activity as a 
                               JOIN TrackPoint as t 
                               ON a.id = t.activity_id""")
        user_dict = {}
        this_activity = -1
        user_trackpoints = self.cursor.fetchall()
        for i in range(0, len(user_trackpoints)):
            duration = user_trackpoints[i][1]-user_trackpoints[i-1][1]
            if duration.total_seconds() >= 300 and user_trackpoints[i][3] != this_activity:
                this_activity = user_trackpoints[i][3]
                if user_trackpoints[i][0] in user_dict:
                    user_dict[user_trackpoints[i][0]] += 1
                else:
                    user_dict[user_trackpoints[i][0]] = 1

        headers = ["user_id", "Number of invalid activities"]
        return (tabulate([k for k in user_dict.items()], headers = headers))


if __name__ == '__main__':
    data = DBhandler()

    # Task 1 - How many users, activities and trackpoints are there in the dataset
    print("Number of users: ", data.get_num("User"))
    print("Number of activities: ", data.get_num("Activity"))
    print("Number of trackpoints: ", data.get_num("TrackPoint"))

    # Task 2 - Find the average, minimum and maximum number of activities per user
    avg_activity_for_all_users = data.get_avg_activities_for_user()
    print("Avrage activities for all users: ", avg_activity_for_all_users, "≈", round(avg_activity_for_all_users, 1))
    print("Maximum number of activities: ", data.get_max_activities_for_user())
    print("Minimum number of activities: ", data.get_min_activities_for_user())

    # Task 3 - Find the top 10 users with the highest number of activities
    print("TOP 10 users with the highest number of activities:")
    headers = ["user_id", "Number of activities"]
    print(tabulate(data.get_top_10_users_with_most_activities(), headers=headers))

    # Task 4 - Find the number of users that have started the activity in one day and ended the activity the next day
    print("number of users that have an activity that ended the day after it started: ", data.ended_activity_at_the_next_day())

    # Task 5 - Find activities that are registered multiple times. You should find the query even if you get zero results
    print(tabulate(data.get_same_activities()))
    # Prints the activities in this format (user_id, transportation_mode, start_date_time, end_date_time, count) count is how many duplicates there are of this activity
    # ---  -------------------  -------------------  -
    # 181  2008-03-14 02:57:55  2008-03-14 03:43:40  2 
    # ---  -------------------  -------------------  -
    # 
    # This query does not return anything because there are no duplicate activities in the DB, but it is tested for by inserting a duplicate. The result is shown above

    # Task 6 - Find the number of users which have been close to each other in time and space (Covid-19 tracking). Close is defined as the same minute (60 seconds) and space (100 meters).
    print("The number of users which have been close to each other:")
    print(data.get_number_of_close_users())

    # Task 7 - Find all users that have never taken a taxi
    print("Users that have never taken a taxi:")
    print(data.find_users_with_no_taxi())

    # Task 8 - Find all types of transportation modes and count how many distinct users that have used the different transportation modes. Do not count the rows where the transportation mode is null.
    print("Number of distinct users that have used the different transportation modes:")
    print(tabulate(data.count_users_per_transport_mode()))

    # Task 9
    # a) - Find the year and month with the most activities.
    print("The year and month with the most activities:")
    print(data.find_date_with_most_activities())
    # b) - Which user had the most activities this year and month, and how many recorded hours do they have? Do they have more hours recorded than the user with the second most activities?
    print("The two users with the most activities that year and month:")
    headers = ["user id", "number of activities", "hours recorded"]
    print(tabulate(data.find_user_with_most_activities(), headers=headers))

    # Task 10 - Find the total distance (in km) walked in 2008, by user with id=112.
    print("Total distance walked by user 112 in 2008:")
    print(data.find_distance_walked_in_year_by_user(2008, 112), "km")

    # Task 11 - Find the top 20 users who have gained the most altitude meters.
    print("Top 20 users who have gained the most altitude meters:")
    print(tabulate(data.find_20_users_with_most_altitude_gain()))

    # Task 12 - Find all users who have invalid activities, and the number of invalid activities per user.
    print("Users with invalid activities: ")
    print(data.find_all_users_with_invalid_activities())

    # Close the db connection
    data.db_close_connection()