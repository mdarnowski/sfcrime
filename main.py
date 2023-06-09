import pandas as pd
from psycopg2.extras import execute_values
from tqdm import tqdm

from utilities.PostgreSQLManager import PostgreSQLManager
from config.database import db_config
from utilities.SQL_Loader import getQuery


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


def handle_operations(df, cursor, table_name, key_column, *columns):
    """
    Handles database operations to avoid duplication.

    :param df: DataFrame containing the data.
    :param cursor: Database cursor.
    :param table_name: Name of the table in the database.
    :param key_column: The key column in the table.
    :param columns: Columns to be handled.
    :return: A mapping of records to their keys.
    """
    mapping = {}
    unique_df = df[list(columns)].drop_duplicates()
    placeholders = ', '.join(['%s'] * len(columns))
    conditions = ' AND '.join([f'{col} = %s' for col in columns])

    select_query = getQuery('handle_operations_select').format(key_column=key_column, table_name=table_name,
                                                               conditions=conditions)
    insert_query = getQuery('handle_operations_insert').format(table_name=table_name, columns=', '.join(columns),
                                                               placeholders=placeholders, key_column=key_column)

    for record in unique_df.values:
        print(select_query)
        cursor.execute(select_query, record)
        result = cursor.fetchone()

        if result is None:
            cursor.execute(insert_query, record)
            mapping[tuple(record)] = cursor.fetchone()[0]
        else:
            mapping[tuple(record)] = result[0]

    return mapping


def insert_batch_returning_key(cursor, table_name, key, columns, values, page_size):
    """
    Insert a batch of records and return their keys.

    :param cursor: Database cursor.
    :param table_name: Name of the table in the database.
    :param key: The key column in the table.
    :param columns: Columns to be inserted.
    :param values: Values to be inserted.
    :param page_size: Number of records in each batch.
    :return: List of keys for the inserted records.
    """
    query = getQuery('insert_batch').format(table_name=table_name, columns=', '.join(columns), key=key)
    execute_values(cursor, query, values, template=None, page_size=page_size)
    return [item[0] for item in cursor.fetchall()]


def get_mappings(df, cursor):
    """
    Get mappings for dimensions.

    :param df: DataFrame containing the data.
    :param cursor: Database cursor.
    :return: A dictionary containing mappings for dimensions.
    """
    return {
        'district_dimension': handle_operations(
            df, cursor, 'district_dimension', 'district_Key', 'police_district', 'analysis_neighborhood'
        ),
        'resolution_dimension': handle_operations(
            df, cursor, 'resolution_dimension', 'resolution_key', 'resolution'
        ),
        'category_dimension': handle_operations(
            df, cursor, 'category_dimension', 'category_key', 'incident_category', 'incident_subcategory',
            'incident_code'
        )
    }


def prepare_batch_values(batch_df, mappings, date_keys, location_keys, incident_detail_keys):
    """
    Prepare the final batch values for insertion.

    :param batch_df: DataFrame containing the batch data.
    :param mappings: A dictionary containing mappings for dimensions.
    :param date_keys: A list containing date keys.
    :param location_keys: A list containing location keys.
    :param incident_detail_keys: A list containing incident detail keys.
    :return: A list of tuples containing batch values.
    """
    return [
        (
            row.row_id, date_keys[idx], mappings['category_dimension'][
                (row.incident_category, row.incident_subcategory, row.incident_code)
            ], mappings['district_dimension'][(row.police_district, row.analysis_neighborhood)],
            mappings['resolution_dimension'][(row.resolution,)], location_keys[idx], incident_detail_keys[idx]
        )
        for idx, row in enumerate(batch_df.itertuples(index=False))
    ]


def insert_data(df, connection_manager):
    """
    Insert data from DataFrame into the database.

    :param df: DataFrame containing the data.
    :param connection_manager: Connection manager for the database.
    """
    with connection_manager.cursor as cursor:
        mappings = get_mappings(df, cursor)

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

            # Insert batches and get the keys
            date_keys = insert_batch_returning_key(
                cursor, "date_dimension", "date_key",
                ["incident_datetime", "incident_date", "incident_time", "incident_year", "incident_day_of_week",
                 "report_datetime"],
                date_values, batch_size)
            location_keys = insert_batch_returning_key(
                cursor, "location_dimension", "location_key", ["latitude", "longitude"], location_values, batch_size)
            incident_detail_keys = insert_batch_returning_key(
                cursor, "incident_details_dimension", "incident_details_key",
                ["incident_number", "incident_description"],
                incident_details_values, batch_size)

            # Prepare the final batch values for insertion
            batch_values = prepare_batch_values(batch_df, mappings, date_keys, location_keys, incident_detail_keys)

            # Insert the final batch
            insert_query = getQuery('insert_incidents')
            cursor.executemany(insert_query, batch_values)

    connection_manager.commit()


def main():
    """
    Main function to execute the script.
    """
    connection_manager = PostgreSQLManager(**db_config)
    connection_manager.connect(auto_commit=False)
    df = load_data('data/crime_sf.csv')
    insert_data(df, connection_manager)


if __name__ == "__main__":
    main()
