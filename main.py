import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from tqdm import tqdm


db_config = {
    "database": "crime_data_sf",
    "user": "postgres",
    "password": "sa",
    "host": "127.0.0.1",
    "port": "5432"
}


def connect():
    return psycopg2.connect(**db_config)


def load_data():
    df = pd.read_csv('crime_sf.csv')
    df.columns = df.columns.str.replace(' ', '_')
    return df.where(pd.notnull(df), None)


def handle_operations(df, cur, table_name, key_column, *columns):
    mapping = {}
    unique_df = df[list(columns)].drop_duplicates()

    placeholders = ', '.join(['%s' for _ in columns])
    select_query = f"SELECT {key_column} FROM {table_name} WHERE ({' AND '.join([f'{col} = %s' for col in columns])})"
    insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders}) RETURNING {key_column};"

    for record in unique_df.values:
        cur.execute(select_query, record)
        result = cur.fetchone()

        if result is None:
            cur.execute(insert_query, record)
            mapping[tuple(record)] = cur.fetchone()[0]
        else:
            mapping[tuple(record)] = result[0]

    return mapping


def insert_data(df, conn):
    mappings = {
        'District_Dimension':
            handle_operations(df, conn.cursor(),
                              'District_Dimension',
                              'District_Key',
                              'Police_District',
                              'Analysis_Neighborhood'),
        'Resolution_Dimension':
            handle_operations(df, conn.cursor(),
                              'Resolution_Dimension',
                              'Resolution_Key',
                              'Resolution'),
        'Category_Dimension':
            handle_operations(df, conn.cursor(),
                              'Category_Dimension',
                              'Category_Key',
                              'Incident_Category',
                              'Incident_Subcategory',
                              'Incident_Code')
    }

    batch_size = 10000
    total_rows = df.shape[0]
    num_batches = (total_rows - 1) // batch_size + 1

    with conn.cursor() as cur:
        for batch_num in tqdm(range(num_batches), desc="Inserting rows"):
            start_idx = batch_num * batch_size
            end_idx = min((batch_num + 1) * batch_size, total_rows)
            batch_df = df.iloc[start_idx:end_idx]

            date_values = batch_df[
                ['Incident_Datetime', 'Incident_Date', 'Incident_Time', 'Incident_Year', 'Incident_Day_of_Week',
                 'Report_Datetime']].values.tolist()
            location_values = batch_df[['Latitude', 'Longitude']].values.tolist()
            incident_details_values = batch_df[['Incident_Number', 'Incident_Description']].values.tolist()

            date_keys = insert_batch_returning_key(cur, "date_dimension", "Date_Key",
                                                   ["Incident_Datetime", "Incident_Date", "Incident_Time",
                                                    "Incident_Year", "Incident_Day_of_Week", "Report_Datetime"],
                                                   date_values, batch_size)
            location_keys = insert_batch_returning_key(cur, "Location_Dimension", "Location_Key",
                                                       ["Latitude", "Longitude"], location_values, batch_size)
            incident_detail_keys = insert_batch_returning_key(cur, "Incident_Details_Dimension", "Incident_Details_Key",
                                                              ["Incident_Number", "Incident_Description"],
                                                              incident_details_values, batch_size)

            batch_values = []

            for idx, row in enumerate(batch_df.itertuples(index=False)):
                district_key = mappings['District_Dimension'][(row.Police_District, row.Analysis_Neighborhood)]
                resolution_key = mappings['Resolution_Dimension'][(row.Resolution,)]
                category_key = mappings['Category_Dimension'][
                    (row.Incident_Category, row.Incident_Subcategory, row.Incident_Code)]
                batch_values.append((row.Row_ID, date_keys[idx], category_key, district_key, resolution_key,
                                     location_keys[idx], incident_detail_keys[idx]))

            cur.executemany("""
                INSERT INTO Incidents (Incident_ID, Date_Key, Category_Key, District_Key, Resolution_Key, Location_Key, Incident_Details_Key)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            """, batch_values)

    conn.commit()


def insert_batch_returning_key(cur, table_name, key, columns, values, page_size):
    query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s RETURNING {key}"
    psycopg2.extras.execute_values(cur, query, values, template=None, page_size=page_size)
    return [item[0] for item in cur.fetchall()]


def main():
    conn = connect()
    df = load_data()

    with conn:
        insert_data(df, conn)


if __name__ == "__main__":
    main()
