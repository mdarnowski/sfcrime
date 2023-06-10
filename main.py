import pandas as pd
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker, registry, class_mapper
from tqdm import tqdm
from SQLAlchemy import DateDimension, CategoryDimension, DistrictDimension, IncidentDetailsDimension, \
    LocationDimension, ResolutionDimension, Incidents
from config.database import db_config


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
    existing_records = session.query(table_class).all()
    existing_mapping = {tuple(getattr(record, col) for col in columns): getattr(record, key_column) for record in
                        existing_records}

    unique_df = df[list(columns)].drop_duplicates()
    new_records = [dict(zip(columns, row)) for row in unique_df.values if tuple(row) not in existing_mapping]

    new_objects = [table_class(**record) for record in new_records]
    session.add_all(new_objects)
    session.commit()

    new_mapping = {tuple(getattr(obj, col) for col in columns): getattr(obj, key_column) for obj in new_objects}
    mapping = {**existing_mapping, **new_mapping}

    return mapping


def insert_data(df, engine):
    """
    Insert data into the database.  This function will insert data into the dimension tables first, then insert data
    :param df: Dataframe containing the data.
    :param engine: Engine to connect to the database.
    :return:    None
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

    date_cols = ["incident_datetime", "incident_date", "incident_time", "incident_year", "incident_day_of_week",
                 "report_datetime"]
    location_cols = ["latitude", "longitude"]
    incident_details_cols = ["incident_number", "incident_description"]

    for start_idx in tqdm(range(0, total_rows, batch_size), desc="Inserting rows"):
        end_idx = start_idx + batch_size
        batch_df = df.iloc[start_idx:end_idx]

        # Prepare values for batch insertion more efficiently
        date_values = batch_df[date_cols].to_dict('records')
        location_values = batch_df[location_cols].to_dict('records')
        incident_details_values = batch_df[incident_details_cols].to_dict('records')

        # Bulk insert
        session.bulk_insert_mappings(class_mapper(DateDimension), date_values)
        session.bulk_insert_mappings(class_mapper(LocationDimension), location_values)
        session.bulk_insert_mappings(class_mapper(IncidentDetailsDimension), incident_details_values)

        # Optimizing the way we get the IDs (one possible method)
        last_date_key = session.query(DateDimension.date_key).order_by(DateDimension.date_key.desc()).first()[0]
        date_keys = range(last_date_key - len(date_values) + 1, last_date_key + 1)
        last_location_key = \
        session.query(LocationDimension.location_key).order_by(LocationDimension.location_key.desc()).first()[0]
        location_keys = range(last_location_key - len(location_values) + 1, last_location_key + 1)
        last_incident_details_key = session.query(IncidentDetailsDimension.incident_details_key).order_by(
            IncidentDetailsDimension.incident_details_key.desc()).first()[0]
        incident_detail_keys = range(last_incident_details_key - len(incident_details_values) + 1,
                                     last_incident_details_key + 1)

        # Prepare the final batch values for insertion
        batch_values = [
            {
                'row_id': row.row_id,
                'date_key': date_keys[idx],
                'category_key': mappings['category_dimension'][
                    tuple([row.incident_category, row.incident_subcategory, row.incident_code])],
                'district_key': mappings['district_dimension'][tuple([row.police_district, row.analysis_neighborhood])],
                'resolution_key': mappings['resolution_dimension'][tuple([row.resolution])],
                'location_key': location_keys[idx],
                'incident_details_key': incident_detail_keys[idx]
            }

            for idx, row in enumerate(batch_df.itertuples(index=False))
        ]

        session.bulk_insert_mappings(class_mapper(Incidents), batch_values)
        # Close the session
    session.commit()
    session.close()


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
                                   ('CategoryDimension', 'category_dimension'),
                                   ('Incidents', 'incidents')
                                   ]:
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
