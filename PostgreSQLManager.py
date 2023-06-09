import psycopg2
import psycopg2.extensions


class PostgreSQLManager:
    def __init__(self, user, password, host, port, dbname=None):
        """
        Initializes a new instance of the PostgreSQLManager class.

        :param user: Username for the database
        :type user: str
        :param password: Password for the database
        :type password: str
        :param host: Hostname where the database is located
        :type host: str
        :param port: Port number on which the database is running
        :type port: str
        :param dbname: Name of the database to be connected to (optional)
        :type dbname: str, optional
        """
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.dbname = dbname
        self.conn = None
        self.cursor = None

    def connect(self, dbname=None):
        """
        Connect to the PostgreSQL database.

        :param dbname: Name of the database to be connected to (optional)
        :type dbname: str, optional
        """
        if dbname:
            self.dbname = dbname
        self.conn = psycopg2.connect(
            dbname=self.dbname, user=self.user, password=self.password,
            host=self.host, port=self.port
        )
        self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        self.cursor = self.conn.cursor()

    def disconnect(self):
        """Disconnect from the PostgreSQL database."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def execute(self, query, params=None):
        """
        Execute an SQL query.

        :param query: SQL query
        :type query: str
        :param params: Parameters for the SQL query
        :type params: tuple, optional
        """
        try:
            self.cursor.execute(query, params) if params else self.cursor.execute(query)
        except psycopg2.Error as e:
            print(f"An error occurred: {e}")

    def fetchone(self):
        """Fetch the next row of a query result set."""
        return self.cursor.fetchone()

    def commit(self):
        """Commit the current transaction."""
        self.conn.commit()


def create_database(db_manager, dbname):
    """
    Create a PostgreSQL database.

    :param db_manager: PostgreSQLManager instance
    :type db_manager: PostgreSQLManager
    :param dbname: Name of the database to be created
    :type dbname: str
    """
    db_manager.connect("postgres")
    query = "SELECT 1 FROM pg_database WHERE datname = %s;"
    db_manager.execute(query, (dbname,))
    if db_manager.fetchone():
        # Terminate connections and drop database if exists
        query = f"""SELECT pg_terminate_backend(pg_stat_activity.pid)
                    FROM pg_stat_activity
                    WHERE pg_stat_activity.datname = '{dbname}' AND pid <> pg_backend_pid();"""
        db_manager.execute(query)
        db_manager.execute(f"DROP DATABASE {dbname};")
    # Create a new database
    db_manager.execute(f"CREATE DATABASE {dbname};")
    db_manager.disconnect()


def create_tables(db_manager, dbname):
    """
    Create tables in the PostgreSQL database.

    :param db_manager: PostgreSQLManager instance
    :type db_manager: PostgreSQLManager
    :param dbname: Name of the database where tables will be created
    :type dbname: str
    """
    db_manager.connect(dbname)
    # SQL script for creating tables
    create_table_script = """CREATE TABLE Date_Dimension (
    Date_Key SERIAL PRIMARY KEY,
    Incident_Datetime TIMESTAMP NOT NULL,
    Incident_Date DATE NOT NULL,
    Incident_Time TIME NOT NULL,
    Incident_Year INT NOT NULL,
    Incident_Day_of_Week VARCHAR(255) NOT NULL,
    Report_Datetime TIMESTAMP NOT NULL
);

CREATE TABLE Category_Dimension (
    Category_Key SERIAL PRIMARY KEY,
    Incident_Category VARCHAR(255),
    Incident_Subcategory VARCHAR(255),
    Incident_Code INT NOT NULL
);

CREATE TABLE District_Dimension (
    District_Key SERIAL PRIMARY KEY,
    Police_District VARCHAR(255) NOT NULL,
    Analysis_Neighborhood VARCHAR(255)
);

CREATE TABLE Resolution_Dimension (
    Resolution_Key SERIAL PRIMARY KEY,
    Resolution VARCHAR(255) NOT NULL
);

CREATE TABLE Location_Dimension (
    Location_Key SERIAL PRIMARY KEY,
    Latitude FLOAT,
    Longitude FLOAT
);

CREATE TABLE Incident_Details_Dimension (
    Incident_Details_Key SERIAL PRIMARY KEY,
    Incident_Number INT NOT NULL,
    Incident_Description TEXT
);

CREATE TABLE Incidents (
    Incident_ID BIGSERIAL PRIMARY KEY,
    Date_Key INT,
    Category_Key INT,
    District_Key INT,
    Resolution_Key INT,
    Location_Key INT,
    Incident_Details_Key INT,
    FOREIGN KEY (Date_Key) REFERENCES Date_Dimension(Date_Key),
    FOREIGN KEY (Category_Key) REFERENCES Category_Dimension(Category_Key),
    FOREIGN KEY (District_Key) REFERENCES District_Dimension(District_Key),
    FOREIGN KEY (Resolution_Key) REFERENCES Resolution_Dimension(Resolution_Key),
    FOREIGN KEY (Location_Key) REFERENCES Location_Dimension(Location_Key),
    FOREIGN KEY (Incident_Details_Key) REFERENCES Incident_Details_Dimension(Incident_Details_Key)
);

"""
    db_manager.execute(create_table_script)
    db_manager.commit()
    db_manager.disconnect()






