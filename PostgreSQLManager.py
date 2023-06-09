import psycopg2
import psycopg2.extensions
from utilities.SQL_Loader import load_sql_queries as getQueries


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
    query = getQueries()['check_database_exists'].format(dbname=dbname)
    db_manager.execute(query, (dbname,))
    if db_manager.fetchone():
        # Terminate connections and drop database if exists
        query = getQueries()['terminate_connections'].format(dbname=dbname)
        db_manager.execute(query)
        db_manager.execute(getQueries()['drop_database'].format(dbname=dbname))
    # Create a new database
    db_manager.execute(getQueries()['create_database'].format(dbname=dbname))
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
    db_manager.execute(getQueries()['create_tables'])
    db_manager.commit()
    db_manager.disconnect()






