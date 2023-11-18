from pyspark.sql import DataFrame


class Cleanser:

    def deduplicate(self, input_df: DataFrame, primary_columns: list[str]) -> DataFrame:
        return input_df.dropDuplicates(primary_columns)

