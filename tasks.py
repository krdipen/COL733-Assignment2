from config import rds, TWEET, WORD_PREFIX, WORD_BUCKETS, WORDSET
from celery import Celery
app = Celery("tasks", backend="redis://localhost:6579", broken="pyamqp://guest@localhost:5672")

@app.task(acks_late=True)
def workTweet():
	try:
		tweets = rds.xreadgroup("my_group1", "dipen1", {TWEET: ">"}, 10000)[0][1]
	except IndexError:
		tweets = rds.xautoclaim(TWEET, "my_group1", "dipen1", 10000, "0-0", 10000)
	wc = {}
	p = rds.pipeline()
	for tweet in tweets:
		p.xack(TWEET, "my_group1", tweet[0])
		p.xdel(TWEET, tweet[0])
		line = tweet[1][TWEET]
		if line == "\n":
			continue
		sp = line.split(",")[4:-2]
		text = " ".join(sp)
		for word in text.split(" "):
			word = word.lower()
			if word not in wc:
				wc[word] = 0
			wc[word] = wc[word] + 1
	w = [{}, {}]
	for word in wc.keys():
		w[hash(word)%WORD_BUCKETS][word] = wc[word]
	if len(w[0]) > 0:
		p.xadd(f"{WORD_PREFIX}0", w[0])
	if len(w[1]) > 0:
		p.xadd(f"{WORD_PREFIX}1", w[1])
	p.execute()
	return TWEET

@app.task(acks_late=True)
def workWord0():
	while(True):
		try:
			tweet = rds.xreadgroup("my_group2", "dipen2", {f"{WORD_PREFIX}0": ">"}, 1)[0][1][0]
		except IndexError:
			try:
				tweet = rds.xautoclaim(f"{WORD_PREFIX}0", "my_group2", "dipen2", 10000, "0-0", 1)[0]
			except IndexError:
				return f"{WORD_PREFIX}0"
		p = rds.pipeline()
		p.xack(f"{WORD_PREFIX}0", "my_group2", tweet[0])
		p.xdel(f"{WORD_PREFIX}0", tweet[0])
		for word in tweet[1].keys():
			p.zincrby(WORDSET, tweet[1][word], word)
		p.execute()

@app.task(acks_late=True)
def workWord1():
	while(True):
		try:
			tweet = rds.xreadgroup("my_group3", "dipen3", {f"{WORD_PREFIX}1": ">"}, 1)[0][1][0]
		except IndexError:
			try:
				tweet = rds.xautoclaim(f"{WORD_PREFIX}1", "my_group3", "dipen3", 10000, "0-0", 1)[0]
			except:
				return f"{WORD_PREFIX}1"
		p = rds.pipeline()
		p.xack(f"{WORD_PREFIX}1", "my_group3", tweet[0])
		p.xdel(f"{WORD_PREFIX}1", tweet[0])
		for word in tweet[1].keys():
			p.zincrby(WORDSET, tweet[1][word], word)
		p.execute()
