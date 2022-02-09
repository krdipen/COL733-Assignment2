from config import rds, TWEET, WORD_PREFIX, WORD_BUCKETS, WORDSET
from celery import Celery
app = Celery("tasks", backend="redis://localhost:6579", broken="pyamqp://guest@localhost:5672")

@app.task(acks_late=True)
def workTweet():
	for i in range(1000):
		try:
			tweets = rds.xreadgroup("my_group1", "dipen1", {TWEET: ">"}, 1)[0][1]
		except IndexError:
			return TWEET
		wc = {}
		for tweet in tweets:
			rds.xdel(TWEET, tweet[0])
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
			rds.xadd(f"{WORD_PREFIX}0", w[0])
		if len(w[1]) > 0:
			rds.xadd(f"{WORD_PREFIX}1", w[1])
	return TWEET

@app.task(acks_late=True)
def workWord0():
	while(True):
		try:
			tweets = rds.xreadgroup("my_group2", "dipen2", {f"{WORD_PREFIX}0": ">"}, 1)[0][1]
		except IndexError:
			return f"{WORD_PREFIX}0"
		wc = {}
		for tweet in tweets:
			rds.xdel(f"{WORD_PREFIX}0", tweet[0])
			for word in tweet[1]:
				if word not in wc:
					wc[word] = 0
				wc[word] = wc[word] + int(tweet[1][word])
		for word in wc.keys():
			rds.zincrby(WORDSET, wc[word], word)

@app.task(acks_late=True)
def workWord1():
	while(True):
		try:
			tweets = rds.xreadgroup("my_group3", "dipen3", {f"{WORD_PREFIX}1": ">"}, 1)[0][1]
		except IndexError:
			return f"{WORD_PREFIX}1"
		wc = {}
		for tweet in tweets:
			rds.xdel(f"{WORD_PREFIX}1", tweet[0])
			for word in tweet[1]:
				if word not in wc:
					wc[word] = 0
				wc[word] = wc[word] + int(tweet[1][word])
		for word in wc.keys():
			rds.zincrby(WORDSET, wc[word], word)
