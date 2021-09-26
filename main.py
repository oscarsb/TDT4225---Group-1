from DbConnector import DbConnector
from tabulate import tabulate


class ExampleProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_table(self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                   id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                   name VARCHAR(30))
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def insert_data(self, table_name):
        names = ['Bobby', 'Mc', 'McSmack', 'Board']
        for name in names:
            # Take note that the name is wrapped in '' --> '%s' because it is a string,
            # while an int would be %s etc
            query = "INSERT INTO %s (name) VALUES ('%s')"
            self.cursor.execute(query % (table_name, name))
        self.db_connection.commit()

    def fetch_data(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        print("Data from table %s, raw format:" % table_name)
        print(rows)
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def drop_table(self, table_name):
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def create_user(self, table_name):
        """Create the user table"""
        query = """CREATE TABLE IF NOT EXISTS %s (
                    id varchar(25) NOT NULL PRIMARY KEY,
                    has_labels BOOLEAN
                    )
                    """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def create_activity(self, table_name):
        """Create the activity table"""
        query = """CREATE TABLE IF NOT EXISTS %s (
                    id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                    user_id varchar(25),
                    CONSTRAINT fk_user
                    FOREIGN KEY (user_id) 
                        REFERENCES User(id)
                    food varchar(25))
                    """
        # TODO add transportation_mode - string, start_date_time - datetime, end_date_time - datetime

        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def insert_user(self, name, has_labels):
        """insert new user, name should be uniqe name which will be used as a primary key, has_tables should be TRUE or FALSE"""

        query = f"""INSERT INTO User (id, has_labels) VALUES('{name}', {has_labels})"""
        print(query)
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query)
        self.db_connection.commit()

    def insert_activity(self, name, food):
        """insert new activity, name should be uniqe name which will be used as a primary key, has_tables should be TRUE or FALSE"""

        query = f"""INSERT INTO Activity (user_id, food) VALUES('{name}', '{food}')"""
        print(query)
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query)
        self.db_connection.commit()


def main():
    program = None

    program = ExampleProgram()

    # remove data incase error previous iteration
    try:
        program.drop_table("User")
        program.drop_table("Activity")
    except:
        pass

    program.create_user(table_name="User")
    program.create_table(table_name="Activity")

    program.insert_user('Hans', "TRUE")
    program.insert_activity("Hans", "Pizza")

    # program.show_tables()
    program.fetch_data("User")
    program.fetch_data("Activity")

    program.drop_table("User")
    program.drop_table("Activity")
    # program.create_table(table_name="TrackPoint")

    # exit
    if program:
        program.connection.close_connection()


if __name__ == '__main__':
    main()
