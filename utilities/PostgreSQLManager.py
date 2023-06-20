from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from config.database import db_config


class PostgreSQLManager:
    __instance = None

    @staticmethod
    def get_instance():
        if PostgreSQLManager.__instance is None:
            PostgreSQLManager()
        return PostgreSQLManager.__instance

    def __init__(self):
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

    def connect(self, dbname=None):
        if dbname:
            self.dbname = dbname
        if self.engine is not None:
            self.engine.dispose()
        engine_url = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname if self.dbname else ''}"
        self.engine = create_engine(engine_url)
        self.Session.configure(bind=self.engine)

    def disconnect(self):
        if self.engine:
            self.engine.dispose()

    def execute(self, query, params=None):
        try:
            self.Session.execute(query, params) if params else self.Session.execute(query)
        except Exception as e:
            print(f"An error occurred: {e}")

    def create_database(self):
        self.connect()

        result_proxy = self.Session.execute(f"SELECT 1 FROM pg_database WHERE datname = '{self.dbname}'")
        db_exists = result_proxy.fetchone()

        if not db_exists:
            self.Session.execute(f"COMMIT")
            self.Session.execute(f"CREATE DATABASE {self.dbname}")
        else:
            result_proxy = self.Session.execute(f"SELECT tablename FROM pg_tables WHERE schemaname='public'")
            tables = result_proxy.fetchall()

            for table in tables:
                self.Session.execute(f"DROP TABLE IF EXISTS {table[0]} CASCADE")

    def create_tables(self, Base):
        Base.metadata.create_all(self.engine)
