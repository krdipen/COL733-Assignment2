from config import WORDSET, rds, TWEET, WORD_PREFIX, WORD_BUCKETS
import tasks

def setup_stream(stream_name: str):
  garbage = {"a": 1}
  id = rds.xadd(stream_name, garbage)
  rds.xdel(stream_name, id)

rds.flushall()
setup_stream(TWEET)
for i in range(WORD_BUCKETS):
  stream_name = f"{WORD_PREFIX}{i}"
  setup_stream(stream_name)

rds.xgroup_create(TWEET, "my_group1", 0)
rds.xgroup_create(f"{WORD_PREFIX}0", "my_group2", 0)
rds.xgroup_create(f"{WORD_PREFIX}1", "my_group3", 0)

print("server is running...")

jobs = []
for i in range(8):
  job = tasks.workTweet.delay()
  jobs.append(job)

tweet = 8
w0, w1 = 0, 0
while True:
  for i in range(8):
    if jobs[i].ready():
      id = jobs[i].get()
      if id == f"{WORD_PREFIX}0":
        w0 = w0 - 1
        tweet = tweet + 1
        jobs[i] = tasks.workTweet.delay()
      elif id == f"{WORD_PREFIX}1":
        w1 = w1 - 1
        tweet = tweet + 1
        jobs[i] = tasks.workTweet.delay()
      else:
        if (w0 == 0 and rds.xlen(f"{WORD_PREFIX}0") > 0) or (w0 != 0 and rds.xlen(f"{WORD_PREFIX}0")/w0 > 10):
          tweet = tweet - 1
          w0 = w0 + 1
          jobs[i] = tasks.workWord0.delay()
        elif (w1 == 0 and rds.xlen(f"{WORD_PREFIX}1") > 0) or (w1 != 0 and rds.xlen(f"{WORD_PREFIX}1")/w1 > 10):
          tweet = tweet - 1
          w1 = w1 + 1
          jobs[i] = tasks.workWord1.delay()
        else:
          jobs[i] = tasks.workTweet.delay()
