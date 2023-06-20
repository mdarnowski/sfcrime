import pandas as pd
from sqlalchemy import create_engine, text, func
from sqlalchemy.orm import sessionmaker, scoped_session
from config.database import db_config
from model.SQLAlchemy import CategoryDimension, Incidents, ResolutionDimension, Base


class PostgreSQLManager:
    """
    A singleton class to manage PostgreSQL database connections and operations.
    """

    __instance = None

    @staticmethod
    def get_instance():
        """
        Retrieve the singleton instance of PostgreSQLManager.
        If an instance doesn't exist, it initializes a new one.

        :return: The singleton instance of PostgreSQLManager.
        """
        if PostgreSQLManager.__instance is None:
            PostgreSQLManager()
        return PostgreSQLManager.__instance

    def __init__(self):
        """
        Initialize the PostgreSQLManager singleton instance.
        Set up database configurations and establish a database connection.
        """
        if PostgreSQLManager.__instance is not None:
            raise Exception("This class is a singleton!")
        else:
            PostgreSQLManager.__instance = self
            self.user = db_config['user']
            self.password = db_config['password']
            self.host = db_config['host']
            self.port = db_config['port']
            self.dbname = db_config['dbname']
            self.engine = None
            self.Session = scoped_session(sessionmaker())
            self.connect()

    def connect(self, dbname=None, default_db=False):
        """
        Establish a connection to the PostgreSQL database.

        :param dbname: Name of the database to connect to. If not provided, it uses the default.
        :param default_db: If True, connects to the default 'postgres' database.
        """
        if dbname:
            self.dbname = dbname
        if self.engine is not None:
            self.engine.dispose()

        db_to_connect = 'postgres' if default_db else self.dbname
        engine_url = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{db_to_connect}"
        self.engine = create_engine(engine_url)

        if not self.Session.registry.has():
            self.Session.configure(bind=self.engine)

    def disconnect(self):
        """
        Disconnect from the PostgreSQL database by disposing the engine.
        """
        if self.engine:
            self.engine.dispose()

    def execute(self, query, params=None):
        """
        Execute an SQL query against the PostgreSQL database.

        :param query: SQL query string to execute.
        :param params: Dictionary of parameters to bind to the query.
        """
        try:
            self.Session.execute(query, params) if params else self.Session.execute(query)
        except Exception as e:
            print(f"An error occurred: {e}")

    def create_database(self):
        """
        Create a new database in PostgreSQL if it doesn't already exist.
        """
        self.connect(default_db=True)  # Connect to default 'postgres' database to check if desired database exists

        query = text("SELECT 1 FROM pg_database WHERE datname = :dbname")
        result_proxy = self.Session.execute(query, {"dbname": self.dbname})
        db_exists = result_proxy.fetchone()

        if not db_exists:
            self.Session.execute(text("COMMIT"))
            query = text(f"CREATE DATABASE {self.dbname}")
            self.Session.execute(query)
        # Reconnect to the newly created database or already existing one
        self.connect()

    def recreate_tables(self):
        """
        Create tables in the database based on the declarative base model.
        Existing tables will be dropped before creating new ones.

        :param Base: SQLAlchemy declarative base model containing table definitions.
        """
        Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)

    def fetch_category_counts(self):
        """
        Fetch data for plotting category counts.

        :return: DataFrame containing the result of the query.
        """
        query = self.Session.query(CategoryDimension.incident_category,
                                   func.count(Incidents.incident_id).label('num_of_incidents')) \
            .join(Incidents, Incidents.category_key == CategoryDimension.key) \
            .group_by(CategoryDimension.incident_category)

        return pd.read_sql(query.statement, self.Session.bind)

    def fetch_category_resolution_counts(self):
        """
        Fetch data for plotting category and resolution counts.

        :return: DataFrame containing the result of the query.
        """
        query = self.Session.query(CategoryDimension.incident_category,
                                   ResolutionDimension.resolution,
                                   func.count(Incidents.incident_id).label('num_of_incidents')) \
            .join(Incidents, Incidents.category_key == CategoryDimension.key) \
            .join(ResolutionDimension, ResolutionDimension.key == Incidents.resolution_key) \
            .group_by(CategoryDimension.incident_category, ResolutionDimension.resolution)

        return pd.read_sql(query.statement, self.Session.bind)
