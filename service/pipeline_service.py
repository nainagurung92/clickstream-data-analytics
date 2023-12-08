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

        print("\nProcessing customer data...")
        customer_df: DataFrame = file_service.FileService().read_csv_file(config.CUSTOMERS_FILE_PATH)
        print(f"\nCustomer Data is read.")
        customer_dedup_df: DataFrame = cleanser_service.Cleanser().deduplicate(customer_df, config.CUSTOMER_PRIMARY_COLUMNS)
        print(f"\nCustomer Data is de-duplicated.")
        customer_type_cast_df: DataFrame = transform_service.Transform().type_cast_customer(customer_dedup_df)
        print("\nCustomer data is type-casted...")
        file_service.FileService().write_mongodb(customer_type_cast_df, config.MONGODB_CUSTOMERS_TABLE_NAME)
        print("\nCustomer data is written to Mongo DB...")

        product_df: DataFrame = file_service.FileService().read_csv_file(config.PRODUCTS_FILE_PATH)
        print(f"\nProduct Data is read.")
        product_dedup_df: DataFrame = cleanser_service.Cleanser().deduplicate(product_df, config.PRODUCT_PRIMARY_COLUMNS)
        print(f"\nProduct Data is de-duplicated.")
        product_type_cast_df: DataFrame = transform_service.Transform().type_cast_product(product_dedup_df)
        print("\nProduct data is type-casted...")
        file_service.FileService().write_mongodb(product_type_cast_df, config.MONGODB_PRODUCTS_TABLE_NAME)
        print("\nProduct data is written to Mongo DB...")

        transaction_df: DataFrame = file_service.FileService().read_csv_file(config.TRANSACTIONS_FILE_PATH)
        print(f"\nTransaction Data is read.")
        transaction_dedup_df: DataFrame = cleanser_service.Cleanser().deduplicate(transaction_df, config.TRANSACTION_PRIMARY_COLUMNS)
        print(f"\nTransaction Data is de-duplicated.")
        transformed_transaction_df: DataFrame = transform_service.Transform().flatten_transaction(transaction_dedup_df).limit(500000)
        print("\nTransaction data is transformed...")
        file_service.FileService().write_mongodb(transformed_transaction_df, config.MONGODB_TRANSACTION_TABLE_NAME)
        print("\nTransaction data is written to Mongo DB...")

        enriched_transaction_df: DataFrame = transform_service.Transform().enrich_transaction(
            transformed_transaction_df,
            product_type_cast_df,
            customer_type_cast_df
        ).limit(300000)
        print("\nTransaction aggregate data is created...")
        file_service.FileService().write_mongodb(enriched_transaction_df, config.MONGODB_TRANSACTION_AGGREGATE_TABLE_NAME)
        print("\nTransaction aggregate data is written to Mongo DB...")