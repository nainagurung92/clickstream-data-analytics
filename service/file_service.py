import application_configs as config
from main import spark_session
from pyspark.sql import DataFrame

import pymongo


class FileService:

    def read_csv_file(self, file_path: str) -> DataFrame:

        return spark_session.read.format(config.RAW_DATA_FORMAT).option("header", "true").load(file_path)

    def write_mongodb(self, input_df, collection_name):

        input_df.write\
            .format('com.mongodb.spark.sql.DefaultSource')\
            .mode('overwrite')\
            .option('spark.mongodb.output.uri', f'mongodb://127.0.0.1:27017/snehasr.{collection_name}?authSource=admin')\
            .save()

    def mongo_db_test(SELF, test_df: DataFrame):
        my_client = pymongo.MongoClient('mongodb://127.0.0.1:27017/')
        my_db = my_client['snehasr']
        my_col = my_db['transactions']
        test_df_pd = test_df.limit(10).toPandas()
        my_col.insert_many(test_df_pd.to_dict('records'))
        print(my_client.list_database_names())
        print(my_db.list_collection_names())

