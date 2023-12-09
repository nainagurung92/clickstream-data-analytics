from pyspark.sql import DataFrame
from service import file_service
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType


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









        






