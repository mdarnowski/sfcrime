from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
from tqdm import tqdm

from config.database import db_config
from model.SQLAlchemy import DateDimension, CategoryDimension, DistrictDimension, IncidentDetailsDimension, \
    LocationDimension, ResolutionDimension, Incidents


def load_data(filepath):
    """
    Load data from CSV file into DataFrame.

    :param filepath: Path to the CSV file.
    :return: DataFrame with loaded data.
    """
    df = pd.read_csv(filepath)
    df.columns = df.columns.str.replace(' ', '_').str.lower()
    return df.where(pd.notnull(df), None)


def get_mappings(session, df, table_class):
    """
    Generates a dictionary with mappings for the dimensions based on the data from a DataFrame and a table class.

    :param session: An active SQLAlchemy session.
    :param df: The DataFrame containing the data.
    :param table_class: The class of the table.
    :return: A dictionary with mappings for the dimensions.
    """
    existing_records = session.query(table_class).all()
    columns = table_class.get_columns()
    existing_mapping = {tuple(getattr(record, col) for col in columns): record.key for record in existing_records}
    unique_df = df[list(columns)].drop_duplicates()
    new_records = [
        dict(zip(columns, row)) for row in unique_df.values
        if tuple(row) not in existing_mapping and not all(pd.isnull(cell) for cell in row)
    ]

    new_objects = [table_class(**record) for record in new_records]
    session.add_all(new_objects)
    session.commit()

    new_mapping = {tuple(getattr(obj, col) for col in columns): obj.key for obj in new_objects}
    mapping = {**existing_mapping, **new_mapping}

    return mapping


def get_key(row, mapping, *columns):
    """
    Retrieve the key from a mapping using row values.

    :param row: DataFrame row
    :param mapping: dictionary with keys as tuples
    :param columns: tuple with column names
    :return: The key from the mapping if found, otherwise None
    """
    key_elements = tuple(getattr(row, col) for col in columns)
    return mapping.get(key_elements)


def get_keys(session, dimension_class, num_of_values):
    """
    Retrieves keys for a given dimension class and number of values.

    :param session: An active SQLAlchemy session.
    :param dimension_class: The class of the dimension table.
    :param num_of_values: The number of values inserted into the dimension table.
    :return: A range object representing the keys.
    """
    last_key = session.query(dimension_class.key).order_by(dimension_class.key.desc()).first()[0]
    keys = range(last_key - num_of_values + 1, last_key + 1)
    return keys


def bulk_insert_and_get_keys(session, batch_df, dimension_class):
    """
    Performs bulk insertion and retrieves keys for a given batch DataFrame and dimension class.

    :param session: An active SQLAlchemy session.
    :param batch_df: A DataFrame representing a batch of data.
    :param dimension_class: The class of the dimension table.
    :return: A range object representing the keys.
    """
    values = batch_df[dimension_class.get_columns()].to_dict('records')
    session.bulk_insert_mappings(dimension_class, values)
    keys = get_keys(session, dimension_class, len(values))

    return keys


def insert_data(df, engine):
    """
    Insert data into the database. This function will insert data into the dimension tables first, then insert data
    into the fact table.

    :param df: Dataframe containing the data.
    :param engine: Engine to connect to the database.
    :return: None
    """
    Session = sessionmaker(bind=engine)
    session = Session()

    mappings = {
        DimensionClass: get_mappings(session, df, DimensionClass)
        for DimensionClass in [DistrictDimension, ResolutionDimension, CategoryDimension, LocationDimension]
    }

    batch_size = 10000
    for start_idx in tqdm(range(0, len(df), batch_size), desc="Inserting rows"):
        batch_df = df.iloc[start_idx:start_idx + batch_size]

        date_keys = bulk_insert_and_get_keys(session, batch_df, DateDimension)
        incident_detail_keys = bulk_insert_and_get_keys(session, batch_df, IncidentDetailsDimension)

        batch_values = [
            {
                'date_key': date_keys[idx],
                'category_key': get_key(row, mappings[CategoryDimension], *CategoryDimension.get_columns()),
                'district_key': get_key(row, mappings[DistrictDimension], *DistrictDimension.get_columns()),
                'resolution_key': get_key(row, mappings[ResolutionDimension], *ResolutionDimension.get_columns()),
                'location_key': get_key(row, mappings[LocationDimension], *LocationDimension.get_columns()),
                'incident_details_key': incident_detail_keys[idx]
            }
            for idx, row in enumerate(batch_df.itertuples(index=False))
        ]

        session.bulk_insert_mappings(Incidents, batch_values)

    session.commit()
    session.close()


def main():
    """
    Main function to execute the script. It sets up the database connection,
    loads the data, and inserts it into the database.
    :return: None
    """
    engine = create_engine(
        f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}")

    df = load_data('data/crime_sf.csv')
    insert_data(df, engine)


if __name__ == "__main__":
    main()
