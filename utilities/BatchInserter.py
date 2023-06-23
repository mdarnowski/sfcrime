import threading
from concurrent.futures import ThreadPoolExecutor

from model.SQLAlchemy import (DateDimension, CategoryDimension, DistrictDimension,
                              IncidentDetailsDimension, LocationDimension,
                              ResolutionDimension, Incidents)
from utilities.DataLoader import DataLoader
from utilities.DimensionMapper import DimensionMapper
from utilities.PostgreSQLManager import PostgreSQLManager


class Singleton(type):
    """A Singleton Metaclass

    A Singleton is a design pattern that restricts the instantiation of a class to a single instance.
    This is useful when exactly one object is needed to coordinate actions across the system.
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        """
        Checks if an instance of the class already exists. If not, it creates one.

        :param args: Variable length argument list.
        :param kwargs: Arbitrary keyword arguments.
        :return: Singleton instance of the class.
        """
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class BatchInserter:
    """
    Handles the insertion of data in batches.

    :param df: A Pandas DataFrame containing the data to be inserted.
    :param session: SQLAlchemy session object.
    """

    def __init__(self, df, session):
        self.df = df
        self.session = session
        self.batches = self._create_batches()

        key_name_map = {
            DistrictDimension: 'district_key',
            ResolutionDimension: 'resolution_key',
            CategoryDimension: 'category_key',
            LocationDimension: 'location_key'
        }

        self.dimension_mapper = DimensionMapper(session, df, key_name_map)
        self.executor = ThreadPoolExecutor(max_workers=4)

    def _get_keys(self, dimension_class, num_of_values):
        """
        Returns keys for the given dimension class

        :param dimension_class: SQLAlchemy declarative base class.
        :param num_of_values: Number of values to retrieve.
        :return: List of keys.
        """

        last_key = self.session.query(dimension_class.key).order_by(dimension_class.key.desc()).first()[0]
        keys = range(last_key - num_of_values + 1, last_key + 1)
        return keys

    def _bulk_insert_and_get_keys(self, batch_df, dimension_class):
        """
        Inserts batch data and returns keys

        :param batch_df: A Pandas DataFrame containing the data to be inserted.
        :param dimension_class: SQLAlchemy declarative base class.
        :return: List of keys.
        """

        values = batch_df[dimension_class.get_columns()].to_dict('records')
        self.session.bulk_insert_mappings(dimension_class, values)
        keys = self._get_keys(dimension_class, len(values))
        return keys

    def _create_batches(self, batch_size=10000):
        """
        Yields data in batches

        :param batch_size: Size of each batch. Defaults to 10000.
        :return: A batch of data.
        """

        for start_idx in range(0, len(self.df), batch_size):
            yield self.df.iloc[start_idx:start_idx + batch_size]

    def insert_one_batch(self, commit=True):
        """
        Inserts one batch of data

        :param commit: Whether to commit the session after insertion. Defaults to True.
        :return: A tuple containing a boolean indicating whether insertion was successful, and the number of rows added.
        """

        try:
            batch_df = next(self.batches)
        except StopIteration:
            return False, 0

        self.session.autoflush = False

        date_keys = self.executor.submit(self._bulk_insert_and_get_keys, batch_df, DateDimension).result()
        incident_detail_keys = self.executor.submit(self._bulk_insert_and_get_keys, batch_df,
                                                    IncidentDetailsDimension).result()

        batch_values = [
            {
                **self.dimension_mapper.get_keys(row),
                'date_key': date_keys[idx],
                'incident_details_key': incident_detail_keys[idx]
            }
            for idx, row in enumerate(batch_df.itertuples(index=False))
        ]

        self.session.bulk_insert_mappings(Incidents, batch_values)

        if commit:
            self.session.commit()

        return True, len(batch_values)


class InsertTask(metaclass=Singleton):
    """
    Handles the running of the data insertion task.
    """

    def __init__(self):
        self.total_rows_added = 0
        self._inserter = None
        self.df = None
        self.total_batches = 0
        self.progress = 0
        self.running = False

    def run(self):
        """
        Loads data and runs the batch insertion task.
        """

        self.running = True
        self.df = DataLoader.get_instance().load_data()
        db_manager = PostgreSQLManager.get_instance()
        db_manager.connect()
        session = db_manager.Session()
        self._inserter = BatchInserter(self.df, session)

        self.total_batches = -(-len(self.df) // 10000)
        current_batch = 0
        self.total_rows_added = 0
        self.progress = 0
        success = True

        while success:
            success, batch_rows_added = self._inserter.insert_one_batch()
            current_batch += 1
            self.total_rows_added += batch_rows_added
            self.progress = (current_batch / self.total_batches) * 100

        session.commit()
        session.close()
        self._inserter = None
        self.running = False


class ActionLock(metaclass=Singleton):
    """
    Provides a thread-safe lock for actions.

    This class uses a threading.Lock to ensure that certain actions are performed in a thread-safe manner.
    """

    def __init__(self):
        self._lock = threading.Lock()

    def is_locked(self):
        """
        Checks if the lock is currently in use.

        :return: True if the lock is in use, False otherwise.
        """

        return self._lock.locked()

    def perform(self, func):
        """
        Performs a function within the locked context.

        :param func: The function to be performed.
        :return: The result of the function.
        """

        with self._lock:
            return func()
