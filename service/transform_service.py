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
        ).drop(config.CLICKSTREAM_EVENT_METADATA)


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



