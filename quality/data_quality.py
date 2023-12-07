from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from service import file_service
import application_configs as config


class DataQuality:

    def duplicateCheck(self, input_df: DataFrame, primary_columns: list[str]) -> bool:
        dedup_df: DataFrame = input_df.dropDuplicates(primary_columns)
        duplicate_count: int = input_df.count() - dedup_df.count()
        if duplicate_count > 0:
            return False
        else:
            return True

    def nullCheck(self, input_df: DataFrame, non_null_column: str) -> bool:
        invalid_df: DataFrame = input_df.filter(f"{non_null_column} is null")
        if invalid_df.count() > 0:
            return False
        else:
            return True

    def categoricalCheck(self, input_df: DataFrame, column: str, categorical_value_list: list[str]) -> bool:
        validate_df: DataFrame = input_df.withColumn(
            "value_check",
            F.when(F.col(column).isin(categorical_value_list), 1).otherwise(0)
        )
        invalid_count: int = validate_df.filter(F.col("value_check") == 0).count()
        if invalid_count > 0:
            return False
        else:
            return True

    def executeDataQualityChecks(self) -> bool:

        device_type_list: list[str] = ["iOS", "Android"]
        clickstream: DataFrame = file_service.FileService().read_mongodb("clickstream").drop("_id")
        product: DataFrame = file_service.FileService().read_mongodb("products").drop("_id")
        customer: DataFrame = file_service.FileService().read_mongodb("customers").drop("_id")
        transaction: DataFrame = file_service.FileService().read_mongodb("transactions").drop("_id")

        clickstream_duplicate_check: bool = self.duplicateCheck(clickstream, config.CLICKSTREAM_PRIMARY_COLUMNS)
        print(f"\nclickstream_duplicate_check: {clickstream_duplicate_check}")
        clickstream_id_null_check: bool = self.nullCheck(clickstream, config.CLICKSTREAM_EVENT_ID)
        print(f"\nclickstream_id_null_check: {clickstream_id_null_check}")

        customer_duplicate_count: bool = self.duplicateCheck(customer, config.CUSTOMER_PRIMARY_COLUMNS)
        print(f"\ncustomer_duplicate_count: {customer_duplicate_count}")
        customer_id_null_check: bool = self.nullCheck(customer, config.CUSTOMER_ID)
        print(f"\ncustomer_id_null_check: {customer_id_null_check}")
        customer_email_null_check: bool = self.nullCheck(customer, config.CUSTOMER_EMAIL)
        print(f"\ncustomer_email_null_check: {customer_email_null_check}")
        customer_device_type_check: bool = self.categoricalCheck(customer, config.CUSTOMER_DEVICE_TYPE, device_type_list)
        print(f"\ncustomer_device_type_check: {customer_device_type_check}")

        product_duplicate_count: bool = self.duplicateCheck(product, config.PRODUCT_PRIMARY_COLUMNS)
        print(f"\nproduct_duplicate_count: {product_duplicate_count}")
        product_id_null_check: bool = self.nullCheck(product, config.PRODUCT_ID)
        print(f"\nproduct_id_null_check: {product_id_null_check}")

        transaction_null_check: bool = self.nullCheck(transaction, config.TRANSACTION_BOOKING_ID)
        print(f"\ntransaction_null_check: {transaction_null_check}")

        return clickstream_duplicate_check & clickstream_id_null_check & customer_duplicate_count & customer_id_null_check & customer_email_null_check & customer_device_type_check & product_duplicate_count & product_id_null_check & transaction_null_check




