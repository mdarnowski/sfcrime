import pandas as pd
from tqdm import tqdm
from model.SQLAlchemy import DateDimension, CategoryDimension, DistrictDimension, IncidentDetailsDimension, \
    LocationDimension, ResolutionDimension, Incidents
from utilities.DimensionMapper import DimensionMapper
from utilities.PostgreSQLManager import PostgreSQLManager


def load_data(filepath):
    """
    Load data from CSV file into DataFrame.

    :param filepath: Path to the CSV file.
    :return: DataFrame with loaded data.
    """
    df = pd.read_csv(filepath)
    df.columns = df.columns.str.replace(' ', '_').str.lower()
    return df.where(pd.notnull(df), None)


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


def insert_data(df):
    """
    Insert data into the database. This function will insert data into the dimension tables first, then insert data
    into the fact table.

    :param df: Dataframe containing the data.
    :return: None
    """
    db_manager = PostgreSQLManager.get_instance()
    session = db_manager.Session()

    key_name_map = {
            DistrictDimension: 'district_key',
            ResolutionDimension: 'resolution_key',
            CategoryDimension: 'category_key',
            LocationDimension: 'location_key'
        }

    dimension_mapper = DimensionMapper(session, df, key_name_map)
    session.commit()

    batch_size = 10000
    for start_idx in tqdm(range(0, len(df), batch_size), desc="Inserting rows"):
        batch_df = df.iloc[start_idx:start_idx + batch_size]

        date_keys = bulk_insert_and_get_keys(session, batch_df, DateDimension)
        incident_detail_keys = bulk_insert_and_get_keys(session, batch_df, IncidentDetailsDimension)

        batch_values = [
            {
                **dimension_mapper.get_keys(row),
                'date_key': date_keys[idx],
                'incident_details_key': incident_detail_keys[idx]
            }
            for idx, row in enumerate(batch_df.itertuples(index=False))
        ]

        session.bulk_insert_mappings(Incidents, batch_values)

    session.commit()
    session.close()
    db_manager.disconnect()


def main():
    """
    Main function to execute the script. It sets up the database connection,
    loads the data, and inserts it into the database.
    :return: None
    """

    df = load_data('data/crime_sf.csv')
    insert_data(df)


if __name__ == "__main__":
    main()
