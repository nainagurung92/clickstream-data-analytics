CLICKSTREAM_FILE_PATH = "/Users/Mac/Documents/IISc/Aug-23-Term/DataEnggAtScale/mywork/Data/click_stream.csv"


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

# Mongo Db Table Names
MONGODB_CLICKSTREAM_TABLE_NAME = "clickstream"



