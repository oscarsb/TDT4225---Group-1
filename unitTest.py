from main import DBhandler, Datahandler


class DBhandler_Test:
    def __init__(self):
        self.program = DBhandler()

    def _test_setup(self):
        self.program.create_user_table()
        self.program.create_activity_table()
        self.program.create_trackpoint_table()

    def _test_insert_data(self):
        name = "Hans"
        self.program.insert_user(name, "TRUE")
        self.program.insert_activity(name=name,
                                     transportationMode="Climbing",
                                     startDatetime="1998-09-08 20:55:00",
                                     endDatetime="1998-09-08 23:00:00")

        self.program.insert_trackpoint(activityID=1,
                                       lat=24.11, lon=11.24,
                                       altitude=100, dateDays=12, date_time="1998-09-08 23:00:00")

    def _fetch_result(self):
        self.program.show_tables()

    def _remove_tables(self):
        self.program.drop_table("TrackPoint")
        self.program.drop_table("Activity")
        self.program.drop_table("User")

    def _print_tables(self):
        self.program.print_table("TrackPoint")
        self.program.print_table("Activity")
        self.program.print_table("User")

    def basic_test(self):
        self._remove_tables()
        self._test_setup()
        self._test_insert_data()
        self._print_tables()
        self._remove_tables()

class Datahandler_Test:
    def __init__(self):
        self.program = Datahandler()
    
    def test_insert_userdata(self):
        """Test inserting user"""
        self.program.drop_tables()
        self.program.create_tables()
        self.program.insert_users()
        self.program.handler.print_table("User")
        self.program.drop_tables()
    
if __name__ == "__main__":
    #dbhandler test
    DBtest = DBhandler_Test()
    DBtest.basic_test()
    
    #Datahandler test
    DATAtest = Datahandler_Test()
    DATAtest.test_insert_userdata()
    
