from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class PostgreSQLManager:
    def __init__(self, user, password, host, port, dbname=None):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.dbname = dbname
        self.engine = None
        self.connection = None

    def connect(self, dbname=None):
        if dbname:
            self.dbname = dbname
        engine_url = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname if self.dbname else ''}"
        self.engine = create_engine(engine_url)
        self.connection = self.engine.connect()

    def connect_to_server(self):
        engine_url = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/"
        self.engine = create_engine(engine_url)
        self.connection = self.engine.connect()

    def disconnect(self):
        if self.connection:
            self.connection.close()
        if self.engine:
            self.engine.dispose()

    def execute(self, query, params=None):
        try:
            self.connection.execute(query, params) if params else self.connection.execute(query)
        except Exception as e:
            print(f"An error occurred: {e}")

    def create_database(self):
        self.connect_to_server()

        # Check if database exists
        result_proxy = self.connection.execute(f"SELECT 1 FROM pg_database WHERE datname = '{self.dbname}'")
        db_exists = result_proxy.fetchone()

        if not db_exists:
            # Create a new database
            self.connection.execute(f"COMMIT")  # Necessary to execute the next command outside of a transaction block
            self.connection.execute(f"CREATE DATABASE {self.dbname}")
        else:
            self.disconnect()
            self.connect(self.dbname)
            # Get all tables in the database
            result_proxy = self.connection.execute(f"SELECT tablename FROM pg_tables WHERE schemaname='public'")
            tables = result_proxy.fetchall()

            # Truncate all tables
            for table in tables:
                self.connection.execute(f"DROP TABLE IF EXISTS {table[0]} CASCADE")

        self.disconnect()

    def create_tables(self, Base):
        self.connect(self.dbname)
        Base.metadata.create_all(self.engine)
        session = sessionmaker(bind=self.engine)
        session = session()
        session.commit()
        self.disconnect()
