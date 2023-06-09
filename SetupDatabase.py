from PostgreSQLManager import PostgreSQLManager, create_database, create_tables
from config.database import db_config

def setup_database(user, password, host, port, dbname):
    """
    Set up a PostgreSQL database with the given configuration.

    :param user: Username for the database
    :type user: str
    :param password: Password for the database
    :type password: str
    :param host: Hostname where the database is located
    :type host: str
    :param port: Port number on which the database is running
    :type port: str
    :param dbname: Name of the database to be set up
    :type dbname: str
    """
    db_manager = PostgreSQLManager(user, password, host, port)

    create_database(db_manager, dbname)
    create_tables(db_manager, dbname)


if __name__ == "__main__":
    # Set up the database
    setup_database(**db_config)
