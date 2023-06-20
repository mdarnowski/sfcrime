import pandas as pd
from sqlalchemy import insert

from model.SQLAlchemy import Dimension


class DimensionMapper:
    def __init__(self, session, df, key_name_mapping):
        self.session = session
        self.df = df
        self.key_name_mapping = key_name_mapping
        self.dimension_classes = self._collect_dimension_classes()
        self.mappings = self._create_mappings()

    def _collect_dimension_classes(self):
        return [dimension_class for dimension_class, _ in self.key_name_mapping.items()
                if issubclass(dimension_class, Dimension)]

    def _get_mappings(self, DimensionClass):
        existing_records = self.session.query(DimensionClass).all()
        existing_mapping = {
            tuple(getattr(record, col) for col in DimensionClass.get_columns()): record.key
            for record in existing_records
        }
        new_records = self._get_new_records(DimensionClass, existing_mapping)
        new_mapping = self._get_new_mapping(DimensionClass, new_records)
        return {**existing_mapping, **new_mapping}

    def _get_new_records(self, DimensionClass, existing_mapping):
        unique_df_rows = self.df[DimensionClass.get_columns()].drop_duplicates().values
        return [
            dict(zip(DimensionClass.get_columns(), row))
            for row in unique_df_rows
            if tuple(row) not in existing_mapping and not pd.isnull(row).all()
        ]

    def _get_new_mapping(self, DimensionClass, new_records):
        if not new_records:
            return {}

        stmt = insert(DimensionClass).values(new_records).returning(DimensionClass.key)
        result = self.session.execute(stmt)
        return {tuple(record.values()): generated_key[0] for record, generated_key in zip(new_records, result)}

    def _create_mappings(self):
        return {DimensionClass: self._get_mappings(DimensionClass) for DimensionClass in self.dimension_classes}

    def get_keys(self, row):
        return {
            self.key_name_mapping[dimension_class]: self.mappings[dimension_class].get(
                tuple(getattr(row, col) for col in dimension_class.get_columns())
            )
            for dimension_class in self.dimension_classes
        }
