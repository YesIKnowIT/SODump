import os

DEBUG=os.getenv('DEBUG', '')

REQUESTS_USER_AGENT='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)'

REQUESTS_CONNECT_TIMEOUT=6.5
REQUESTS_READ_TIMEOUT=10
REQUESTS_TIMEOUT=(REQUESTS_CONNECT_TIMEOUT,REQUESTS_READ_TIMEOUT)
REQUESTS_COOLDOWN=15
MAX_SLEEP_TIME=120
MAX_RETRY=5

CACHE_MAX_SIZE=100 if DEBUG else 10000

QUEUE_LENGTH=1000

URL_PREFIX = 'http://stackoverflow.com/questions/'
CDX_API_ENDPOINT="http://web.archive.org/cdx/search/cdx"
CDX_LIMIT=10000

DB_URI="file:{}?mode=rwc".format("test.db" if DEBUG else "questions.db")

PARSER_OK = 'OK'
PARSER_ERROR = 'ERROR'
PARSER_SYS_ERROR = 'SYSERR'
PARSER_CORE_DATA_ERROR = 'DATAERR_CD'
PARSER_VIEW_COUNT_ERROR = 'DATAERR_VC'


CDX_PROCESS_COUNT=2
LOADER_PROCESS_COUNT=16
PARSER_PROCESS_COUNT=5