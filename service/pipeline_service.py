from service import file_service
from service import transform_service
from service import cleanser_service
import application_configs as config
from pyspark.sql import DataFrame


class PipelineService:

    def execute_pipeline(self):

        print("\nPipeline Execution Started...")
        print("\nProcessing clickstream data...")
        clickstream_df: DataFrame = file_service.FileService().read_csv_file(config.CLICKSTREAM_FILE_PATH)
        print(f"\nClickstream Data is read.")
        clickstream_dedup_df: DataFrame = cleanser_service.Cleanser().deduplicate(clickstream_df, config.CLICKSTREAM_PRIMARY_COLUMNS)
        print(f"\nClickstream Data is de-duplicated.")
        transformed_clickstream_df: DataFrame = transform_service.Transform().flatten_clickstream(clickstream_dedup_df)
        print("\nClickstream data is transformed...")
        clickstream_type_cast_df: DataFrame = transform_service.Transform().type_cast_clickstream(transformed_clickstream_df).limit(500000)
        print("\nClickstream data is type-casted...")
        file_service.FileService().write_mongodb(clickstream_type_cast_df, config.MONGODB_CLICKSTREAM_TABLE_NAME)
        print("\nClickstream data is written to Mongo DB...")