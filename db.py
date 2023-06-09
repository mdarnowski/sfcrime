import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def check_database_exists(dbname, cursor):

    # Execute a query to check if the specified database exists
    cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (dbname,))

    exists = cursor.fetchone()

    return exists is not None

def create_database(dbname, user, password, host, port):
    conn = psycopg2.connect(dbname="postgres", user=user, password=password, host=host, port=port)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    if check_database_exists(dbname, cursor):
        cursor.execute(
            f"SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = '{dbname}' AND pid <> pg_backend_pid();")
        cursor.execute(f"DROP DATABASE {dbname};")

    cursor.execute(f"CREATE DATABASE {dbname};")
    cursor.close()
    conn.close()


def create_tables(dbname, user, password, host, port):
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    cursor = conn.cursor()

    cursor.execute("""CREATE TABLE Date_Dimension (
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

""")


    cursor.close()
    conn.commit()
    conn.close()


user = 'postgres'
password = 'sa'
host = '127.0.0.1'
port = '5432'

dbname = 'crime_data_sf'
create_database(dbname, user, password, host, port)

create_tables(dbname, user, password, host, port)
