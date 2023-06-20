from model.SQLAlchemy import Base
from utilities.PostgreSQLManager import PostgreSQLManager


def setup_database():
    """
    Set up a PostgreSQL database with the given configuration.
    """
    db_manager = PostgreSQLManager.get_instance()
    db_manager.create_database()
    db_manager.create_tables(Base)


if __name__ == "__main__":
    # Set up the database
    setup_database()
