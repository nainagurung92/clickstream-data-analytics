from service import file_service
from service import transform_service
from service import cleanser_service
import application_configs as config
from pyspark.sql import DataFrame


class PipelineService:

    def execute_pipeline(self):

        clickstream_df: DataFrame = file_service.FileService().read_csv_file(config.CLICKSTREAM_FILE_PATH)
        clickstream_dedup_df: DataFrame = cleanser_service.Cleanser().deduplicate(clickstream_df, config.CLICKSTREAM_PRIMARY_COLUMNS)
        transformed_clickstream_df: DataFrame = transform_service.Transform().flatten_clickstream(clickstream_dedup_df)
        clickstream_type_cast_df: DataFrame = transform_service.Transform().type_cast_clickstream(transformed_clickstream_df)
        file_service.FileService().write_mongodb(clickstream_type_cast_df, config.MONGODB_CLICKSTREAM_TABLE_NAME)

        customer_df: DataFrame = file_service.FileService().read_csv_file(config.CUSTOMERS_FILE_PATH)
        customer_dedup_df: DataFrame = cleanser_service.Cleanser().deduplicate(customer_df, config.CUSTOMER_PRIMARY_COLUMNS)
        customer_type_cast_df: DataFrame = transform_service.Transform().type_cast_customer(customer_dedup_df)
        file_service.FileService().write_mongodb(customer_type_cast_df, config.MONGODB_CUSTOMERS_TABLE_NAME)

        product_df: DataFrame = file_service.FileService().read_csv_file(config.PRODUCTS_FILE_PATH)
        product_dedup_df: DataFrame = cleanser_service.Cleanser().deduplicate(product_df, config.PRODUCT_PRIMARY_COLUMNS)
        product_type_cast_df: DataFrame = transform_service.Transform().type_cast_product(product_dedup_df)
        file_service.FileService().write_mongodb(product_type_cast_df, config.MONGODB_PRODUCTS_TABLE_NAME)

        transaction_df: DataFrame = file_service.FileService().read_csv_file(config.TRANSACTIONS_FILE_PATH)
        transaction_dedup_df: DataFrame = cleanser_service.Cleanser().deduplicate(transaction_df, config.TRANSACTION_PRIMARY_COLUMNS)
        transformed_transaction_df: DataFrame = transform_service.Transform().flatten_transaction(transaction_dedup_df)
        file_service.FileService().write_mongodb(transformed_transaction_df, config.MONGODB_TRANSACTION_TABLE_NAME)

        enriched_transaction_df: DataFrame = transform_service.Transform().enrich_transaction(
            transformed_transaction_df,
            product_type_cast_df,
            customer_type_cast_df
        )
        file_service.FileService().write_mongodb(enriched_transaction_df, config.MONGODB_TRANSACTION_AGGREGATE_TABLE_NAME)