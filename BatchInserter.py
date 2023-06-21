import threading
import time

from model.SQLAlchemy import DateDimension, CategoryDimension, DistrictDimension, IncidentDetailsDimension, \
    LocationDimension, ResolutionDimension, Incidents
from utilities.DataLoader import DataLoader
from utilities.DimensionMapper import DimensionMapper
from utilities.PostgreSQLManager import PostgreSQLManager

from concurrent.futures import ThreadPoolExecutor


class BatchInserter:
    def __init__(self, df, session):
        self.df = df
        self.session = session
        self.batches = self.create_batches()

        key_name_map = {
            DistrictDimension: 'district_key',
            ResolutionDimension: 'resolution_key',
            CategoryDimension: 'category_key',
            LocationDimension: 'location_key'
        }

        self.dimension_mapper = DimensionMapper(session, df, key_name_map)
        self.executor = ThreadPoolExecutor(max_workers=4)

    def get_keys(self, dimension_class, num_of_values):
        last_key = self.session.query(dimension_class.key).order_by(dimension_class.key.desc()).first()[0]
        keys = range(last_key - num_of_values + 1, last_key + 1)
        return keys

    def bulk_insert_and_get_keys(self, batch_df, dimension_class):
        values = batch_df[dimension_class.get_columns()].to_dict('records')
        self.session.bulk_insert_mappings(dimension_class, values)
        keys = self.get_keys(dimension_class, len(values))
        return keys

    def create_batches(self, batch_size=10000):
        for start_idx in range(0, len(self.df), batch_size):
            yield self.df.iloc[start_idx:start_idx + batch_size]

    def insert_one_batch(self, commit=True):
        try:
            batch_df = next(self.batches)
        except StopIteration:
            return False, 0

        self.session.autoflush = False  # Disable autoflush

        date_keys = self.executor.submit(self.bulk_insert_and_get_keys, batch_df, DateDimension).result()
        incident_detail_keys = self.executor.submit(self.bulk_insert_and_get_keys, batch_df, IncidentDetailsDimension).result()

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
            self.session.commit()  # Commit the session after each batch insertion

        return True, len(batch_values)


class InsertTask:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(InsertTask, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self):
        self.df = None
        self.total_rows_added = 0
        self.total_batches = 0
        self.progress = 0
        self.running = False

    def run(self):
        self.running = True
        self.df = DataLoader.get_instance().load_data()
        db_manager = PostgreSQLManager.get_instance()
        session = db_manager.Session()
        inserter = BatchInserter(self.df, session)

        self.total_batches = len(self.df) // 10000 + 1
        current_batch = 0

        while True:
            success, batch_rows_added = inserter.insert_one_batch()

            if not success:
                break

            current_batch += 1
            self.total_rows_added += batch_rows_added
            self.progress = (current_batch / self.total_batches) * 100
            time.sleep(1)

        session.commit()
        session.close()
        self.running = False


class ActionLock:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(ActionLock, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self):
        self._lock = threading.Lock()

    def is_locked(self):
        return self._lock.locked()

    def perform(self, func):
        with self._lock:
            return func()
