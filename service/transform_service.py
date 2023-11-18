from pyspark.sql import functions as F
import application_configs as config
from pyspark.sql import DataFrame
from pyspark.sql.types import TimestampType, IntegerType, DateType, DoubleType, FloatType


class Transform:

    def flatten_clickstream(self, clickstream_df: DataFrame) -> DataFrame:
        return clickstream_df.select(
            F.col(config.CLICKSTREAM_SESSION_ID)
            , F.col(config.CLICKSTREAM_EVENT_NAME)
            , F.col(config.CLICKSTREAM_EVENT_TIME)
            , F.col(config.CLICKSTREAM_EVENT_ID)
            , F.col(config.CLICKSTREAM_TRAFFIC_SOURCE)
            , F.col(config.CLICKSTREAM_EVENT_METADATA)
            , F.json_tuple(
                F.col(config.CLICKSTREAM_EVENT_METADATA)
                , config.EVENT_METADATA_PRODUCT_ID
                , config.EVENT_METADATA_QUANTITY
                , config.EVENT_METADATA_ITEM_PRICE
                , config.EVENT_METADATA_PROMO_CODE
                , config.EVENT_METADATA_PROMO_AMOUNT
                , config.EVENT_METADATA_PAYMENT_STATUS
                , config.EVENT_METADATA_SEARCH_KEYWORDS
            )
            .alias(
                config.EVENT_METADATA_PRODUCT_ID
                , config.EVENT_METADATA_QUANTITY
                , config.EVENT_METADATA_ITEM_PRICE
                , config.EVENT_METADATA_PROMO_CODE
                , config.EVENT_METADATA_PROMO_AMOUNT
                , config.EVENT_METADATA_PAYMENT_STATUS
                , config.EVENT_METADATA_SEARCH_KEYWORDS
            )
        )

    def flatten_transaction(self, transaction_df: DataFrame) -> DataFrame:

        return transaction_df.selectExpr(
            f"cast({config.TRANSACTION_CREATED_AT} as timestamp) as {config.TRANSACTION_CREATED_AT}",
            f"cast({config.TRANSACTION_CUSTOMER_ID} as int) as {config.TRANSACTION_CUSTOMER_ID}",
            config.TRANSACTION_BOOKING_ID,
            config.TRANSACTION_SESSION_ID,
            config.TRANSACTION_PAYMENT_METHOD,
            config.TRANSACTION_PAYMENT_STATUS,
            f"cast({config.TRANSACTION_PROMO_AMOUNT} as float) as {config.TRANSACTION_PROMO_AMOUNT}",
            config.TRANSACTION_PROMO_CODE,
            f"cast({config.TRANSACTION_SHIPMENT_FEE} as float) as {config.TRANSACTION_SHIPMENT_FEE}",
            f"cast({config.TRANSACTION_SHIPMENT_DATE_LIMIT} as timestamp) as {config.TRANSACTION_SHIPMENT_DATE_LIMIT}",
            f"cast({config.TRANSACTION_SHIPMENT_LOCATION_LAT} as double) as {config.TRANSACTION_SHIPMENT_LOCATION_LAT}",
            f"cast({config.TRANSACTION_SHIPMENT_LOCATION_LONG} as double) as {config.TRANSACTION_SHIPMENT_LOCATION_LONG}",
            f"cast({config.TRANSACTION_TOTAL_AMOUNT} as float) as {config.TRANSACTION_TOTAL_AMOUNT}",
            f"inline(from_json({config.TRANSACTION_PRODUCT_METADATA}, '{config.TRANSACTION_PRODUCT_METADATA_SCHEMA}'))"
        )

    def type_cast_customer(self, customer_df: DataFrame) -> DataFrame:

        return customer_df.withColumn(
            config.CUSTOMER_ID,
            F.col(config.CUSTOMER_ID).cast(IntegerType())
        ).withColumn(
            config.CUSTOMER_BIRTH_DATE,
            F.col(config.CUSTOMER_BIRTH_DATE).cast(DateType())
        ).withColumn(
            config.CUSTOMER_HOME_LOCATION_LAT,
            F.col(config.CUSTOMER_HOME_LOCATION_LAT).cast(DoubleType())
        ).withColumn(
            config.CUSTOMER_HOME_LOCATION_LONG,
            F.col(config.CUSTOMER_HOME_LOCATION_LONG).cast(DoubleType())
        ).withColumn(
            config.CUSTOMER_FIRST_JOIN_DATE,
            F.col(config.CUSTOMER_FIRST_JOIN_DATE).cast(DateType())
        )

    def type_cast_product(self, product_df: DataFrame) -> DataFrame:

        return product_df.withColumn(
            config.PRODUCT_ID,
            F.col(config.PRODUCT_ID).cast(IntegerType())
        ).withColumn(
            config.PRODUCT_YEAR,
            F.col(config.PRODUCT_YEAR).cast(IntegerType())
        )

    def type_cast_clickstream(self, clickstream_df: DataFrame) -> DataFrame:
        return clickstream_df.withColumn(
            config.CLICKSTREAM_EVENT_TIME,
            F.col(config.CLICKSTREAM_EVENT_TIME).cast(TimestampType())
        ).withColumn(
            config.EVENT_METADATA_PRODUCT_ID,
            F.col(config.EVENT_METADATA_PRODUCT_ID).cast(IntegerType())
        ).withColumn(
            config.EVENT_METADATA_QUANTITY,
            F.col(config.EVENT_METADATA_QUANTITY).cast(FloatType())
        ).withColumn(
            config.EVENT_METADATA_ITEM_PRICE,
            F.col(config.EVENT_METADATA_ITEM_PRICE).cast(FloatType())
        ).withColumn(
            config.EVENT_METADATA_PROMO_AMOUNT,
            F.col(config.EVENT_METADATA_PROMO_AMOUNT).cast(FloatType())
        )

    def enrich_transaction(self, transaction_df: DataFrame, product_df: DataFrame, customer_df: DataFrame) -> DataFrame:

        new_transaction_df: DataFrame = transaction_df.withColumnRenamed(
            config.TRANSACTION_CUSTOMER_ID,
            config.TRANSACTION_CUSTOMER_ID_RENAME
        )

        return new_transaction_df.join(
            customer_df,
            F.col(config.TRANSACTION_CUSTOMER_ID_RENAME) == F.col(config.CUSTOMER_ID),
            "left"
        ).drop(
            config.TRANSACTION_CUSTOMER_ID_RENAME
        ).join(
            product_df,
            F.col(config.TRANSACTION_PRODUCT_ID) == F.col(config.PRODUCT_ID),
            "left"
        ).drop(
            config.TRANSACTION_PRODUCT_ID
        ).withColumnRenamed(
            config.PRODUCT_ID,
            config.TRANSACTION_PRODUCT_ID
        )

