import pandas as pd
from sqlalchemy import insert

from model.SQLAlchemy import Dimension


class DimensionMapper:
    """
    A mapper class to map data from a DataFrame to a Dimension model.

    This class helps to create mappings from a DataFrame to a SQLAlchemy Dimension model. It collects
    all dimension classes from the provided key-name mapping, creates a mapping for each dimension class
    using the data from the DataFrame, and provides a method to get the keys for each row of the DataFrame.

    :param session: SQLAlchemy Session object, used to query and insert data to the database.
    :param df: pandas DataFrame, the source of the data.
    :param key_name_mapping: a dictionary that maps dimension classes to key names.
    """
    def __init__(self, session, df, key_name_mapping):
        self.session = session
        self.df = df
        self.key_name_mapping = key_name_mapping
        self.dimension_classes = self._collect_dimension_classes()
        self.mappings = self._create_mappings()

    def _collect_dimension_classes(self):
        """
        Collect all Dimension classes from the key-name mapping.

        This method iterates over the key-name mapping and collects all items that are subclasses
        of the Dimension class.

        :return: a list of Dimension classes.
        """
        return [dimension_class for dimension_class, _ in self.key_name_mapping.items()
                if issubclass(dimension_class, Dimension)]

    def _get_mappings(self, DimensionClass):
        """
        Get a mapping for a Dimension class.

        This method queries all existing records for the provided Dimension class, creates a mapping
        from the existing records, gets all new records from the DataFrame, and creates a mapping from
        the new records.

        :param DimensionClass: a Dimension class to create a mapping for.
        :return: a dictionary that maps tuples of record values to keys.
        """
        existing_records = self.session.query(DimensionClass).all()
        existing_mapping = {
            tuple(getattr(record, col) for col in DimensionClass.get_columns()): record.key
            for record in existing_records
        }
        new_records = self._get_new_records(DimensionClass, existing_mapping)
        new_mapping = self._get_new_mapping(DimensionClass, new_records)
        return {**existing_mapping, **new_mapping}

    def _get_new_records(self, DimensionClass, existing_mapping):
        """
        Get new records for a Dimension class.

        This method filters unique rows from the DataFrame based on the columns of the provided
        Dimension class and returns all rows that are not already in the existing mapping and not all null.

        :param DimensionClass: a Dimension class to get new records for.
        :param existing_mapping: a dictionary that maps tuples of existing record values to keys.
        :return: a list of dictionaries, each representing a new record.
        """
        unique_df_rows = self.df[DimensionClass.get_columns()].drop_duplicates().values
        return [
            dict(zip(DimensionClass.get_columns(), row))
            for row in unique_df_rows
            if tuple(row) not in existing_mapping and not pd.isnull(row).all()
        ]

    def _get_new_mapping(self, DimensionClass, new_records):
        """
        Get a mapping from new records for a Dimension class.

        This method inserts the new records to the database and creates a mapping from the inserted
        records. If there are no new records, it returns an empty dictionary.

        :param DimensionClass: a Dimension class to create a mapping for.
        :param new_records: a list of dictionaries, each representing a new record.
        :return: a dictionary that maps tuples of new record values to generated keys.
        """
        if not new_records:
            return {}

        stmt = insert(DimensionClass).values(new_records).returning(DimensionClass.key)
        result = self.session.execute(stmt)
        return {tuple(record.values()): generated_key[0] for record, generated_key in zip(new_records, result)}

    def _create_mappings(self):
        """
        Create mappings for all Dimension classes.

        This method iterates over all collected Dimension classes and creates a mapping for each
        Dimension class.

        :return: a dictionary that maps Dimension classes to mappings.
        """
        return {DimensionClass: self._get_mappings(DimensionClass) for DimensionClass in self.dimension_classes}

    def get_keys(self, row):
        """
        Get the keys for a row of the DataFrame.

        This method iterates over all collected Dimension classes and gets the key for each Dimension
        class based on the values of the row.

        :param row: a Series representing a row of the DataFrame.
        :return: a dictionary that maps key names to keys.
        """
        return {
            self.key_name_mapping[dimension_class]: self.mappings[dimension_class].get(
                tuple(getattr(row, col) for col in dimension_class.get_columns())
            )
            for dimension_class in self.dimension_classes
        }
