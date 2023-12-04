from pyspark.sql import DataFrame
from service import file_service
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType, IntegerType, DateType, DoubleType, FloatType


class Analysis:

    def perform_analysis(self):

        clickstream: DataFrame = file_service.FileService().read_mongodb("clickstream") \
            .drop("_id") \
            .withColumn(
                'event_date', F.to_date('event_time')
            ).withColumn(
                'event_year_month', F.date_format('event_time', 'yyyy-MM')
            ).withColumn(
                'event_year', F.year('event_time')
            )

        product: DataFrame = file_service.FileService().read_mongodb("products").drop("_id")
        transaction:  DataFrame = file_service.FileService().read_mongodb("transactions").drop("_id")
        transaction_agg: DataFrame = file_service.FileService().read_mongodb("transactions_aggregate").drop("_id")

        # clickstream.show(10)

        total_clickstream: int = clickstream.count()

        # Percentage of events that use the different traffic source
        device_df: DataFrame = clickstream.groupby('traffic_source').count().withColumn(
            'device_percentage',
            (F.col('count')/total_clickstream)*100
        ).drop('count')

        print("\nPercentage of events that use the different traffic source: ")
        device_df.show(10, False)

        print("\nDifferent event names: ")
        clickstream.select('event_name').distinct().show(10)

        # The percentage portion device on booking
        booking_device_df: DataFrame = clickstream.filter(F.col('event_name') == 'BOOKING').groupby('traffic_source').count().withColumn(
            'booking_device_percentage',
            (F.col('count')/clickstream.filter(F.col('event_name') == 'BOOKING').count())
        ).drop('count')

        print("\nPercentage of devices on booking by traffic source:")
        booking_device_df.show()

        # Most bookings by month
        print("\nTop 10 months ordered by booking count: ")
        clickstream.filter(F.col('event_name') == 'BOOKING').groupby('event_year_month').count().sort(F.col('count').desc()).show(10)

        # Total booking year to year
        print("\nTotal booking year to year: ")
        clickstream.filter(F.col('event_name') == 'BOOKING').groupby('event_year').count().sort(F.col('event_year').desc()).show(10)

        # Total device user use on year to year for booking event
        print("\nTotal device user use on year to year for booking event: ")
        clickstream.filter(F.col('event_name') == 'BOOKING').groupby('event_year', 'traffic_source').count().sort(F.col('event_year').desc()).show(10)

        # Average time spent on each page
        window = Window.partitionBy("event_name").orderBy("event_time")
        diff = F.col("event_time").cast('long') - F.lag(F.col("event_time"), 1).over(window).cast("long")
        print("\nAverage time spent on each page: ")
        clickstream\
            .withColumn("diff", diff)\
            .groupBy("event_name")\
            .agg(F.avg('diff'))\
            .withColumnRenamed('avg(diff)', 'average_time_spent')\
            .sort(F.col('average_time_spent').desc()).show(10)

        # Conversion rate from all interactions to cart
        other_interactions: list = ['SCROLL', 'ITEM_DETAIL', 'ADD_PROMO', 'CLICK', 'PROMO_PAGE', 'HOMEPAGE', 'SEARCH']
        cart_conversion_rate = clickstream.filter(F.col('event_name') == 'ADD_TO_CART').count() / clickstream.filter(F.col('event_name').isin(other_interactions)).count()
        print(f"\nItem to cart conversion rate: {cart_conversion_rate}")

        # Conversion rate from all interactions to check out
        other_interactions_new: list = ['SCROLL', 'ITEM_DETAIL', 'ADD_PROMO', 'CLICK', 'PROMO_PAGE', 'HOMEPAGE', 'SEARCH', 'ADD_TO_CART']
        checkout_conversion_rate = clickstream.filter(F.col('event_name') == 'BOOKING').count() / clickstream.filter(F.col('event_name').isin(other_interactions_new)).count()
        print(f"\nItem to check out conversion rate: {checkout_conversion_rate}")

        max_product_id: int = clickstream.filter('product_id is not null').groupby('product_id').count().sort(F.col('count').desc()).collect()[0][0]
        print(f"\nProduct details of the most interacted product: {max_product_id}")
        product.filter(F.col('id') == max_product_id).show(10, False)

        max_search_word: str = clickstream.filter('search_keywords is not null').groupby('search_keywords').count().sort(F.col('count').desc()).collect()[0][0]
        print(f"\nMost searched key word: {max_search_word}")

        # Gender proportion split in the transaction data:
        print("\nGender proportion split in the transaction data: ")
        transaction_agg_new: DataFrame = transaction_agg.withColumn(
            "gender_derived",
            F.when(F.col("gender") == "Girls", "Women").when(F.col("gender") == "Boys", "Men").otherwise(F.col("gender"))
        )
        transaction_agg_new.groupby('gender_derived').count().withColumn('gender_percentage',(F.col('count')/transaction_agg.count())*100).drop('count').show(10)

        # transaction_agg.show(10)
        print("\nTotal sales amount for each product category: ")
        total_sales_per_category = transaction_agg.groupBy("masterCategory").sum("item_price")\
            .sort(F.col('sum(item_price)').desc()).withColumnRenamed("sum(item_price)", "total_sales")
        total_sales_per_category.show(10)

        print("\nEstimate the customer lifetime value for each customer based on their historical transactions: ")
        windowSpec = Window.partitionBy("customer_id").orderBy(F.col("created_at").desc())

        customer_clv = transaction_agg.withColumn("rank", F.rank().over(windowSpec)) \
            .withColumn("cumulative_amount", F.sum("item_price").over(windowSpec).cast(IntegerType())) \
            .filter(F.col("rank") == 1) \
            .select("customer_id", "cumulative_amount").distinct().orderBy(F.col("cumulative_amount").desc())
        customer_clv.show(10)

        print("\nAnalyze monthly sales trends to identify seasonality: ")
        monthly_sales_trends = transaction_agg\
            .withColumn("year_month", F.year("created_at") * 100 + F.month("created_at")) \
            .groupBy("year_month").agg(F.sum("item_price").alias("monthly_sales"))\
            .orderBy(F.col("year_month").desc())
        monthly_sales_trends.show()

        print("\nTop N Products by Revenue: ")
        top_n_products = transaction_agg.groupBy("product_id").agg(F.sum("item_price").alias("total_revenue")) \
            .orderBy("total_revenue", ascending=False).limit(10)
        top_n_products.show()

        print("\nAverage Transaction Value Over Time: ")
        windowSpec = Window.orderBy("created_at").rowsBetween(-7, 0)  # 7-day rolling window

        avg_transaction_value = transaction_agg\
            .withColumn("avg_transaction_value", F.avg("item_price").over(windowSpec))\
            .orderBy(F.col("created_at"))
        avg_transaction_value.show()

        print("\nIdentify customers who have not made a purchase in the last N months: ")

        churn_threshold: str = "2023-01-01"  # Adjust the threshold as needed

        customer_churn = transaction_agg\
            .groupBy("customer_id")\
            .agg(F.max("created_at").alias("last_purchase_date"))\
            .filter(f"last_purchase_date < cast('{churn_threshold}' as date)")
        customer_churn.show()









        






