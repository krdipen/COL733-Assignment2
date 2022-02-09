import redis
rds = redis.Redis(host='10.17.50.173', port=6579, decode_responses=True)

TWEET = "tweet"
WORD_PREFIX = "w_"
WORD_BUCKETS = 2

WORDSET = "words"
