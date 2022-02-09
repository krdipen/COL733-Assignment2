import sys
import os
import time
import sys
from config import rds, TWEET, WORDSET


if (len(sys.argv) < 2):
    print("Use the command: python3 client.py <data_dir>")

DIR=sys.argv[1]

# Clear the output
rds.delete(WORDSET)
rds.zadd(WORDSET, {"foo": 0})

abs_files=[os.path.join(pth, f) for pth, dirs, files in os.walk(DIR) for f in files]

for filename in abs_files:
  with open(filename, mode='r') as f:
    for line in f:
      # push all the lines into input stream
      rds.xadd(TWEET, {TWEET: line})

ctr = 0
while True:
  # print top 10 words
  print(rds.zrevrangebyscore(WORDSET, '+inf', '-inf', 0, 10, withscores=True))
  time.sleep(1)
  ctr = ctr + 1
  if ctr >= 2000:
    break
