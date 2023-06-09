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
        filter_conditions = {col: value for col, value in zip(columns, record)}
        result = session.query(getattr(table_class, key_column)).filter_by(**filter_conditions).first()

        if result is None:
            new_record = {col: value for col, value in zip(columns, record)}
            obj = table_class(**new_record)
            session.add(obj)
            session.flush()
            mapping[tuple(record)] = getattr(obj, key_column)
        else:
            mapping[tuple(record)] = result[0]

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

    for batch_num in tqdm(range(num_batches), desc="Inserting rows"):
        start_idx = batch_num * batch_size
        end_idx = min((batch_num + 1) * batch_size, total_rows)
        batch_df = df.iloc[start_idx:end_idx]

        # Prepare values for batch insertion
        date_values = batch_df[
            ['incident_datetime', 'incident_date', 'incident_time', 'incident_year', 'incident_day_of_week',
             'report_datetime']].values.tolist()
        location_values = batch_df[['latitude', 'longitude']].values.tolist()
        incident_details_values = batch_df[['incident_number', 'incident_description']].values.tolist()

        # Insert batch and get the keys
        date_keys = session.execute(
            insert(DateDimension).values([dict(zip(
                ["incident_datetime", "incident_date", "incident_time", "incident_year", "incident_day_of_week",
                 "report_datetime"], record)) for record in date_values]).returning(DateDimension.date_key)
        ).fetchall()
        location_keys = session.execute(
            insert(LocationDimension).values([dict(zip(["latitude", "longitude"], record)) for record in location_values]).returning(LocationDimension.location_key)
        ).fetchall()
        incident_detail_keys = session.execute(
            insert(IncidentDetailsDimension).values([dict(zip(["incident_number", "incident_description"], record)) for record in incident_details_values]).returning(IncidentDetailsDimension.incident_details_key)
        ).fetchall()

        # Prepare the final batch values for insertion
        batch_values = [
            {
                'row_id': row.row_id,
                'date_key': date_keys[idx][0],
                'category_key': mappings['category_dimension'][(row.incident_category, row.incident_subcategory, row.incident_code)],
                'district_key': mappings['district_dimension'][(row.police_district, row.analysis_neighborhood)],
                'resolution_key': mappings['resolution_dimension'][(row.resolution,)],
                'location_key': location_keys[idx][0],
                'incident_details_key': incident_detail_keys[idx][0]
            }
            for idx, row in enumerate(batch_df.itertuples(index=False))
        ]

        # Insert the final batch
        session.execute(insert(Incidents), batch_values)

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
