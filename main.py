import pandas as pd
from tqdm import tqdm
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, ForeignKey, insert
from sqlalchemy.orm import sessionmaker, mapper, relationship, registry, class_mapper
from utilities.SQL_Loader import getQuery
from config.database import db_config
from SQLAlchemy import Base, DateDimension, CategoryDimension, DistrictDimension, IncidentDetailsDimension, \
    LocationDimension, ResolutionDimension, Incidents


def load_data(filepath):
    """
    Load data from CSV file into DataFrame.

    :param filepath: Path to the CSV file.
    :return: DataFrame with loaded data.
    """
    df = pd.read_csv(filepath)
    df.columns = df.columns.str.replace(' ', '_')
    df.columns = map(str.lower, df.columns)
    return df.where(pd.notnull(df), None)


def get_mappings(session, df, table_class, key_column, *columns):
    """
    Get mappings for dimensions.

    :param session: SQLAlchemy session.
    :param df: DataFrame containing the data.
    :param table_class: ORM class for the table.
    :param key_column: The key column in the table.
    :param columns: Columns to be handled.
    :return: A dictionary containing mappings for dimensions.
    """
    mapping = {}
    unique_df = df[list(columns)].drop_duplicates()

    for record in unique_df.values:
        filter_conditions = dict(zip(columns, record))
        result = session.query(getattr(table_class, key_column)).filter_by(**filter_conditions).first()

        if result is None:
            new_record = dict(zip(columns, record))
            obj = table_class(**new_record)
            session.add(obj)
            mapping[tuple(record)] = getattr(obj, key_column)
        else:
            mapping[tuple(record)] = result[0]

    # Flush once after all records have been added
    session.flush()

    return mapping


def insert_data(df, engine):
    """
    Insert data from DataFrame into the database.

    :param df: DataFrame containing the data.
    :param engine: SQLAlchemy engine.
    """
    Session = sessionmaker(bind=engine)
    session = Session()

    # Define mappings
    mappings = {
        'district_dimension': get_mappings(
            session, df, DistrictDimension, 'district_key', 'police_district', 'analysis_neighborhood'
        ),
        'resolution_dimension': get_mappings(
            session, df, ResolutionDimension, 'resolution_key', 'resolution'
        ),
        'category_dimension': get_mappings(
            session, df, CategoryDimension, 'category_key', 'incident_category', 'incident_subcategory', 'incident_code'
        )
    }

    # Insert batches
    batch_size = 10000
    total_rows = df.shape[0]
    num_batches = -(-total_rows // batch_size)

    # Precompile the column names
    date_cols = ["incident_datetime", "incident_date", "incident_time", "incident_year", "incident_day_of_week", "report_datetime"]
    location_cols = ["latitude", "longitude"]
    incident_details_cols = ["incident_number", "incident_description"]

    for batch_num in tqdm(range(num_batches), desc="Inserting rows"):
        start_idx = batch_num * batch_size
        end_idx = min((batch_num + 1) * batch_size, total_rows)
        batch_df = df.iloc[start_idx:end_idx]

        # Prepare values for batch insertion
        date_values = [dict(zip(date_cols, record)) for record in batch_df[date_cols].values.tolist()]
        location_values = [dict(zip(location_cols, record)) for record in batch_df[location_cols].values.tolist()]
        incident_details_values = [dict(zip(incident_details_cols, record)) for record in batch_df[incident_details_cols].values.tolist()]

        # Bulk insert
        session.bulk_insert_mappings(DateDimension, date_values)
        session.bulk_insert_mappings(LocationDimension, location_values)
        session.bulk_insert_mappings(IncidentDetailsDimension, incident_details_values)

        # Query the recently inserted IDs
        date_keys = session.query(DateDimension.date_key).order_by(DateDimension.date_key.desc()).limit(len(date_values)).all()
        location_keys = session.query(LocationDimension.location_key).order_by(LocationDimension.location_key.desc()).limit(len(location_values)).all()
        incident_detail_keys = session.query(IncidentDetailsDimension.incident_details_key).order_by(IncidentDetailsDimension.incident_details_key.desc()).limit(len(incident_details_values)).all()

        # Reverse the keys because we queried in descending order
        date_keys = list(reversed(date_keys))
        location_keys = list(reversed(location_keys))
        incident_detail_keys = list(reversed(incident_detail_keys))

        # Prepare the final batch values for insertion
        batch_values = [
            {
                'row_id': row.row_id,
                'date_key': date_keys[idx][0],
                'category_key': mappings['category_dimension'][tuple([row.incident_category, row.incident_subcategory, row.incident_code])],
                'district_key': mappings['district_dimension'][tuple([row.police_district, row.analysis_neighborhood])],
                'resolution_key': mappings['resolution_dimension'][tuple([row.resolution])],
                'location_key': location_keys[idx][0],
                'incident_details_key': incident_detail_keys[idx][0]
            }
            for idx, row in enumerate(batch_df.itertuples(index=False))
        ]

        # Use bulk_insert_mappings for final batch insert
        session.bulk_insert_mappings(Incidents, batch_values)

        # Commit the changes
        session.commit()


def main(orm_exc=None):
    """
    Main function to execute the script.
    """
    # Define engine
    engine = create_engine(
        f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}")

    # Reflect the tables and map them to the ORM classes
    meta = MetaData()
    meta.reflect(bind=engine)

    # Use registry for mapping
    mapper_registry = registry()

    for class_name, table_name in [('DateDimension', 'date_dimension'),
                                   ('LocationDimension', 'location_dimension'),
                                   ('IncidentDetailsDimension', 'incident_details_dimension'),
                                   ('DistrictDimension', 'district_dimension'),
                                   ('ResolutionDimension', 'resolution_dimension'),
                                   ('CategoryDimension', 'category_dimension')]:
        orm_class = globals()[class_name]
        table = meta.tables[table_name]
        try:
            # Check if the class is already mapped
            class_mapper(orm_class)
        except orm_exc.UnmappedClassError:
            # If not, map it
            mapper_registry.map_imperatively(orm_class, table)

    # Load data and insert it into the database
    df = load_data('data/crime_sf.csv')
    insert_data(df, engine)


if __name__ == "__main__":
    main()
