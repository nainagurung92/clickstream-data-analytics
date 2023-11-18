CLICKSTREAM_FILE_PATH = "/Users/Z0045NQ/Documents/MTech/Data Engg/Project/Data/archive/click_stream.csv"

CUSTOMERS_FILE_PATH = "/Users/Z0045NQ/Documents/MTech/Data Engg/Project/Data/archive/customer.csv"

PRODUCTS_FILE_PATH = "/Users/Z0045NQ/Documents/MTech/Data Engg/Project/Data/archive/product.csv"

TRANSACTIONS_FILE_PATH = "/Users/Z0045NQ/Documents/MTech/Data Engg/Project/Data/archive/transactions.csv"

MASTER_NAME = "local[*]"
APPLICATION_NAME = "Click Stream Data Pipeline"
RAW_DATA_FORMAT = "csv"

# Atomic Clicksream Columns
CLICKSTREAM_SESSION_ID = "session_id"
CLICKSTREAM_EVENT_NAME = "event_name"
CLICKSTREAM_EVENT_TIME = "event_time"
CLICKSTREAM_EVENT_ID = "event_id"
CLICKSTREAM_TRAFFIC_SOURCE = "traffic_source"
CLICKSTREAM_EVENT_METADATA = "event_metadata"

CLICKSTREAM_PRIMARY_COLUMNS = ["event_id"]

# Atomic Clickstream Exploded Events
EVENT_METADATA_PRODUCT_ID = "product_id"
EVENT_METADATA_QUANTITY = "quantity"
EVENT_METADATA_ITEM_PRICE = "item_price"
EVENT_METADATA_PAYMENT_STATUS = "payment_status"
EVENT_METADATA_SEARCH_KEYWORDS = "search_keywords"
EVENT_METADATA_PROMO_CODE = "promo_code"
EVENT_METADATA_PROMO_AMOUNT = "promo_amount"

# Atomic Customer Columns
CUSTOMER_ID = "customer_id"
CUSTOMER_FIRST_NAME = "first_name"
CUSTOMER_LAST_NAME = "last_name"
CUSTOMER_USERNAME = "username"
CUSTOMER_EMAIL = "email"
CUSTOMER_GENDER = "gender"
CUSTOMER_BIRTH_DATE = "birthdate"
CUSTOMER_DEVICE_TYPE = "device_type"
CUSTOMER_DEVICE_ID = "device_id"
CUSTOMER_DEVICE_VERSION = "device_version"
CUSTOMER_HOME_LOCATION_LAT = "home_location_lat"
CUSTOMER_HOME_LOCATION_LONG = "home_location_long"
CUSTOMER_HOME_LOCATION = "home_location"
CUSTOMER_HOME_COUNTRY = "home_country"
CUSTOMER_FIRST_JOIN_DATE = "first_join_date"

CUSTOMER_PRIMARY_COLUMNS = ["customer_id"]

# Atomic Product Data
PRODUCT_ID = "id"
PRODUCT_GENDER = "gender"
PRODUCT_MASTER_CATEGORY = "masterCategory"
PRODUCT_SUB_CATEGORY = "subCategory"
PRODUCT_ARTICLE_TYPE = "articleType"
PRODUCT_BASE_COLOUR = "baseColour"
PRODUCT_SEASON = "season"
PRODUCT_YEAR = "year"
PRODUCT_USAGE = "usage"
PRODUCT_DISPLAY_NAME = "productDisplayName"

PRODUCT_PRIMARY_COLUMNS = ["id"]

# Atomic Transaction Data
TRANSACTION_CREATED_AT = "created_at"
TRANSACTION_CUSTOMER_ID = "customer_id"
TRANSACTION_CUSTOMER_ID_RENAME = "transaction_customer_id"
TRANSACTION_BOOKING_ID = "booking_id"
TRANSACTION_SESSION_ID = "session_id"
TRANSACTION_PRODUCT_METADATA = "product_metadata"
TRANSACTION_PRODUCT_METADATA_SCHEMA = "array<struct<product_id:integer,quantity:float,item_price:float>>"
TRANSACTION_PRODUCT_ID = "product_id"
TRANSACTION_PAYMENT_METHOD = "payment_method"
TRANSACTION_PAYMENT_STATUS = "payment_status"
TRANSACTION_PROMO_AMOUNT = "promo_amount"
TRANSACTION_PROMO_CODE = "promo_code"
TRANSACTION_SHIPMENT_FEE = "shipment_fee"
TRANSACTION_SHIPMENT_DATE_LIMIT = "shipment_date_limit"
TRANSACTION_SHIPMENT_LOCATION_LAT = "shipment_location_lat"
TRANSACTION_SHIPMENT_LOCATION_LONG = "shipment_location_long"
TRANSACTION_TOTAL_AMOUNT = "total_amount"

TRANSACTION_PRIMARY_COLUMNS = ["booking_id"]

# Mongo Db Table Names

MONGODB_CUSTOMERS_TABLE_NAME = "customers"
MONGODB_PRODUCTS_TABLE_NAME = "products"
MONGODB_CLICKSTREAM_TABLE_NAME = "clickstream"
MONGODB_TRANSACTION_TABLE_NAME = "transactions"
MONGODB_TRANSACTION_AGGREGATE_TABLE_NAME = "transactions_aggregate"


