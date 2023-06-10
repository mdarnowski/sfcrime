from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from utilities.SQL_Loader import getQuery


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
        self.engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}')
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.dbname = dbname
        self.session = None

    def connect(self, dbname=None):
        """
        Connect to the PostgreSQL database.

        :param dbname: Name of the database to be connected to (optional)
        :type dbname: str, optional
        """
        if dbname:
            self.dbname = dbname
        conn_string = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname or 'postgres'}"
        self.engine = create_engine(conn_string)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def disconnect(self):
        """Disconnect from the PostgreSQL database."""
        if self.session:
            self.session.close()
        if self.engine:
            self.engine.dispose()

    def execute(self, query, params=None):
        """
        Execute an SQL query.

        :param query: SQL query
        :type query: str
        :param params: Parameters for the SQL query
        :type params: dict, optional
        """
        try:
            result = self.session.execute(text(query), params) if params else self.session.execute(text(query))
            return result
        except Exception as e:
            print(f"An error occurred: {e}")

    def fetchone(self):
        """Fetch the next row of a query result set."""
        return self.session.fetchone()

    def commit(self):
        """Commit the current transaction."""
        self.session.commit()


def create_database(db_manager, dbname):
    """
    Create a PostgreSQL database.

    :param db_manager: PostgreSQLManager instance
    :type db_manager: PostgreSQLManager
    :param dbname: Name of the database to be created
    :type dbname: str
    """
    db_manager.connect("postgres")
    query = getQuery('check_database_exists').format(dbname=dbname)
    result = db_manager.execute(query, {'dbname': dbname})

    if result.fetchone():
        # Terminate connections and drop database if exists
        terminate_connections_query = getQuery('terminate_connections').format(dbname=dbname)
        drop_database_query = getQuery('drop_database').format(dbname=dbname)

        # Using engine to execute the DROP DATABASE command
        with db_manager.engine.connect() as connection:
            connection.execution_options(isolation_level="AUTOCOMMIT").execute(terminate_connections_query)
            connection.execution_options(isolation_level="AUTOCOMMIT").execute(drop_database_query)

    # Create a new database
    db_manager.execute(getQuery('create_database').format(dbname=dbname))
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
    db_manager.execute(getQuery('create_tables'))
    db_manager.commit()
    db_manager.disconnect()

