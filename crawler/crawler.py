#!/usr/bin/env python

"""YouTube API Data Crawler"""

# Copyright (C) 2009 Christopher Foo <chris.foo@gmail.com>
#
# This file is part of ytdata.
#
# ytdata is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# ytdata is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with ytdata.  If not, see <http://www.gnu.org/licenses/>.

__docformat__ = "restructuredtext en"

import os
import os.path
import sys
import pickle
import logging
import logging.handlers
import traceback
import time
import random
import signal
import gdata.youtube.service
import gdata.service
import threading
import tempfile
import shelve
import shutil
import httplib
import gzip
import cStringIO as StringIO
import threading
import Queue as queue

import database
import ytextract
import http_server
import connections

class Crawler:
	CRAWL_QUEUE_FILE = "./data/queue.pickle"
	TABLE_NAME = "vidtable1"
	USER_TABLE_NAME = "usertable1"
#	QUEUE_SLEEP_TIME = .1 # seconds
	ITERATION_SLEEP_TIME = 0.1 # seconds
	WRITE_INTERVAL = 60 # seconds
	MAX_QUEUE_SIZE = 50
	MAX_DOWNLOAD_THREADS = 4
#	DOWNLOAD_STALL_TIME = 1 # seconds
	THROTTLE_SLEEP_TIME = 20 # seconds; for iteration sleep time
	TRAVERSE_RATE = 0.06 # Crawl related videos
	USER_TRAVERSE_RATE = 0.1 # crawl user favs, uploads, playlists
	THROTTLE_STALL_TIME = 60 * 5 # seconds
	RECENT_VIDS_URI = "http://gdata.youtube.com/feeds/api/standardfeeds/most_recent"
	RECENT_VIDS_INTERVAL = 60 * 45 # seconds
	MIGHT_AS_WELL_RATE = 0.1 # decrease this for disk performance
	PROCESS_BLOCK_SIZE = 20 # number of stuff to process in one stage
	
	def __init__(self):
		# Crawl queue:
		# A list of [feed uri string, video id, referring video id]
		if os.path.exists(self.CRAWL_QUEUE_FILE):
			f = open(self.CRAWL_QUEUE_FILE, "r")
			self.crawl_queue = pickle.load(f) # list of video ids and urls
			f.close()
		else:
			logging.warning("Crawl queue file not found. Queue is empty")
			self.crawl_queue = []
		
		self.db = database.Database()
		self.httpclient = connections.HTTPClient()
		self.yt_service = gdata.youtube.service.YouTubeService(
			http_client=self.httpclient)
		self.running = False
#		self.write_counter = 0 # Counter for write interval
		self.entry_queue = queue.Queue(0) # YouTube video Entrys to be processed
		self.user_entry_queue = queue.Queue(0) # user entries to be be processed
		self.username_queue = queue.Queue(0) # usernames to be inserted
		self.crawl_sync_queue = queue.Queue(0) # syncronized crawl_queue
		self.throttle_required = False
		self.throttle_next_time = None
		self.tasks = []
	
	def run(self):
		logging.info("Running")
		self.running = True
		
		self.vids_crawled_session = 0
		self.start_time = time.time()
		self.last_write_time = time.time()
		
		# time in the future when we fetch new recent videos
		recent_vids_next_time = time.time() + self.RECENT_VIDS_INTERVAL 
		
		while self.running and (\
			len(self.crawl_queue) > 0 \
			or len(self.tasks) > 0 \
			or not self.entry_queue.empty() \
			or not self.username_queue.empty() ):
			
			logging.debug("Run iteration")
			
			if not self.throttle_required:
				if time.time() >= recent_vids_next_time:
					logging.info("Inject recent videos to crawl queue")
					self.add_uri_to_crawl(self.RECENT_VIDS_URI)
					recent_vids_next_time = time.time() + self.RECENT_VIDS_INTERVAL
			
				if len(self.crawl_queue) > 0 \
				and len(self.tasks) < self.MAX_DOWNLOAD_THREADS:
					self.process_crawl_queue_item()
			
			self.process_entries()
			self.process_user_entry_queue()
			self.process_username_queue()
			self.process_tasks()
			
			if random.random() < self.TRAVERSE_RATE \
			and len(self.crawl_queue) < self.MAX_QUEUE_SIZE / 2:
				username = self.get_username()
				if username is not None:
					self.traverse_user(username)
			
#			if not self.throttle_required:
#				self.write_counter += 1
				
			if time.time() - self.last_write_time >= self.WRITE_INTERVAL:
				logging.info(self.get_stats_string())
				self.write_state()
#				self.write_counter = 0
				self.last_write_time = time.time()
			
			logging.debug("Sleeping for %s seconds" % self.ITERATION_SLEEP_TIME)
			time.sleep(self.ITERATION_SLEEP_TIME)
			
			if self.throttle_required:
				logging.info("Throttle required")
				time.sleep(self.THROTTLE_SLEEP_TIME)
				self.throttle_required = time.time() < self.throttle_next_time
			
		self.write_state()
		logging.info("Run finished. This shouldn't happen; try adding more video ids to crawl or adjusting the traversal rate.")
		
	def in_database(self, video_id):
		"""Get whether video is already in database
		
		:Return:
			`boolean`
		"""
		
		r = self.db.conn.execute("SELECT 1 FROM %s WHERE id = ? LIMIT 1" % self.db.TABLE_NAME, [video_id]).fetchone()
		return r is not None and r[0] == 1
	
	def was_traversed(self, video_id):
		"""Get whether video was already traversed
		
		:Return:
			`boolean`
		"""
		
		r = self.db.conn.execute("SELECT traversed FROM %s WHERE id = ? LIMIT 1" % self.db.TABLE_NAME, [video_id]).fetchone()
		return r is not None and r[0] == 1
	
	def user_in_database(self, username):
		"""Get whether user is in database"""
		
		r = self.db.conn.execute("SELECT 1 FROM %s WHERE username = ? LIMIT 1" % self.db.USER_TABLE_NAME, [username]).fetchone()
		return r is not None and r[0] == 1
		
	def user_was_traversed(self, username):
		"""Get whether use was traversed"""
		
		r = self.db.conn.execute("SELECT traversed FROM %s WHERE username = ? LIMIT 1" % self.db.USER_TABLE_NAME, [username]).fetchone()
		return r is not None and r[0] == 1
	
	def get_username(self):
		"""Return a username which has not been traversed"""
		
		r = self.db.conn.execute("SELECT username FROM %s WHERE traversed IS NULL LIMIT 1" % self.db.USER_TABLE_NAME, []).fetchone()[0]
		return r
	
	def process_crawl_queue_item(self):
		"""Process a Feed uri to crawl"""
		
		while not self.crawl_sync_queue.empty():
			self.crawl_queue.append(self.crawl_sync_queue.get(block=True))
		
		if isinstance(self.crawl_queue[0], str):
			uri = self.crawl_queue[0]
			video_id = None
			referred_by = None
		else:
			uri, video_id, referred_by = self.crawl_queue[0]
		logging.info("Crawling %s %s" % (uri, video_id))
		
		# Should be a single video
		if video_id is not None:
			has_seen = self.in_database(video_id)
			was_traversed = self.was_traversed(video_id)
			logging.debug("\tHas seen: %s; Was traversed: %s" % (has_seen, was_traversed))
			
			if not has_seen:
				task = Task("fetch-entry", video_id=video_id,
					yt_service=self.yt_service,
					queue=self.entry_queue)
				task.start()
				self.tasks.append(task)
		
			if not was_traversed:
				logging.debug("\tTraversing")
			
				self.traverse_video(video_id)
			
		
		# Should be a playlist 
		else:
			
			task = Task("fetch-feed", uri=uri, yt_service=self.yt_service,
			 	referred_by=referred_by, queue=self.entry_queue)
			task.start()
			self.tasks.append(task)
		
		return self.crawl_queue.pop(0)
	
	
	def process_entries(self):
		for i in range(self.PROCESS_BLOCK_SIZE):
			if self.entry_queue.empty():
				break
			
			self.process_entry(self.entry_queue.get(block=True))
			
	
	def process_entry(self, entry, referred_by=None):
		d = ytextract.extract_from_entry(entry)
		
		if referred_by is None and entry.referred_by is not None:
			referred_by = entry.referred_by
			
		d["referred_by"] = referred_by
		logging.info("Processing entry %s" % d["id"])
		logging.debug("\tData: %s" % d)
		
		if self.in_database(d["id"]):
			logging.debug("\tAlready database. Not updating")
		else:
			logging.info("\tNew! Adding to database.")
			self.db_video_insert(d)
			self.vids_crawled_session += 1
		
		if self.was_traversed(d["id"]):
			logging.debug("\tAlready traversed.")
		elif random.random() > self.TRAVERSE_RATE:
			logging.debug("\tDecided to not traverse.")
		elif len(self.crawl_queue) >= self.MAX_QUEUE_SIZE:
			logging.debug("\tCrawl queue too large. Not traversing.")
		else:
#			logging.info("\tAdding to crawl queue.")
			self.add_uri_to_crawl(None, video_id=d["id"], referred_by=referred_by)
		
		username = entry.author[0].name.text.decode("utf-8")
		
		if random.random() < self.MIGHT_AS_WELL_RATE:
			if not self.user_in_database(username):
				logging.debug("\tInsert username into database")
				self.db.conn.execute("""INSERT INTO %s (username)
					VALUES (?)""" % self.db.USER_TABLE_NAME, [username])
		
		if self.user_was_traversed(username):
			logging.debug("\tUser was already traversed.")
		elif random.random() > self.USER_TRAVERSE_RATE:
			logging.debug("\tDecided to not traverse user")
		elif len(self.crawl_queue) >= self.MAX_QUEUE_SIZE:
			logging.debug("\tCrawl queue too large. Not traversing user.")
		else:
			try:
				self.traverse_user(username)
			except:
				logging.exception("Failed to get info for user %s" % username)
	
	def process_username_queue(self):
		for i in range(self.PROCESS_BLOCK_SIZE):
			if self.username_queue.empty():
				break
			
			username = self.username_queue.get(block=True)
			if not self.user_in_database(username):
				logging.debug("Insert username %s into database" % username)
				self.db.conn.execute("""INSERT INTO %s (username)
					VALUES (?)""" % self.db.USER_TABLE_NAME, [username])
	
	def traverse_video(self, video_id, entry=None):
		"""Queue related and video responses feed, mark video as traversed"""
		
		logging.info("Traversing video %s" % video_id)
		
		related_uri = "http://gdata.youtube.com/feeds/api/videos/%s/related?start-index=%s&max-results=%s" % (video_id, 1, 50)

		response_uri = "http://gdata.youtube.com/feeds/api/videos/%s/responses?start-index=%s&max-results=%s" % (video_id, 1, 50)

		self.add_uri_to_crawl(related_uri, referred_by=video_id)
		self.add_uri_to_crawl(response_uri, referred_by=video_id)
		
		logging.debug("\tMarking %s as traversed" % video_id)
		self.db.conn.execute("""UPDATE %s SET traversed=? WHERE
			id=?""" % self.TABLE_NAME, (1, video_id))
		
		logging.debug("\tDone traversing %s" % video_id)
		
	
	def process_user_entry_queue(self):
		for i in range(self.PROCESS_BLOCK_SIZE):
			if self.user_entry_queue.empty():
				break
			
			entry = self.user_entry_queue.get(block=True)
			
			d = ytextract.extract_from_user_entry(entry)
			logging.debug("\tGot user data %s" % d)
			
			username = d["username"]
			
			if self.user_in_database(username):
				logging.debug("\tUpdate user data into database")
				self.db.conn.execute("""UPDATE %s SET videos_watched = ?
					WHERE username = ?""" % self.db.USER_TABLE_NAME, 
					(d["videos_watched"], username ))
			else:
				logging.debug("\tInsert user data into database")
				self.db.conn.execute("""INSERT INTO %s (username, videos_watched)
					VALUES (?,?)""" % self.db.USER_TABLE_NAME, 
					(username, d["videos_watched"]))
	
	def traverse_user(self, username):
		"""Get user stats, favorites, playlists, subscribers"""
		
		logging.info("Traversing user %s" % username)
		task = Task("fetch-user", username=username.encode("utf-8"),
			yt_service=self.yt_service, queue=self.user_entry_queue)
		task.start()
		self.tasks.append(task)
		
		task = Task("fetch-user-playlists", username=username.encode("utf-8"),
			yt_service=self.yt_service, queue=self.crawl_sync_queue)
		task.start()
		self.tasks.append(task)
		
#		if random.random() < self.MIGHT_AS_WELL_RATE:
		task = Task("fetch-user-subscribers", username=username.encode("utf-8"),
			http_client=self.httpclient, queue=self.username_queue)
		task.start()
		self.tasks.append(task)
		
		fav_uri = "http://gdata.youtube.com/feeds/api/users/%s/favorites?start-index=1&max-results=50" % username
		uploads_uri = "http://gdata.youtube.com/feeds/api/users/%s/uploads?start-index=1&max-results=50" % username
		self.add_uri_to_crawl(fav_uri)
		self.add_uri_to_crawl(uploads_uri)
		
		logging.debug("\tMarking user %s as traversed" % username)
		self.db.conn.execute("""UPDATE %s SET traversed=? WHERE
			username=?""" % self.db.USER_TABLE_NAME, (1, username))

	def add_uri_to_crawl(self, uri, video_id=None, referred_by=None):
		logging.debug("Adding uri to queue %s, video_id %s, referred_by %s" % (uri, video_id, referred_by))
		self.crawl_queue.append([uri, video_id, referred_by])
	
	def add_to_crawl(self, s):
		"""Add a video id or a API URL to crawl"""
		
		if s.startswith("http://"):
			logging.info("Adding feed %s" % i)
			crawler.add_uri_to_crawl(s)
		else:
			logging.info("Adding video %s" % i)
			crawler.add_uri_to_crawl(None, video_id=s)
	
	def db_video_insert(self, d):
		logging.debug("Database video insert")
		self.db.conn.execute("""INSERT INTO %s
			(id, views, rating, rates, 
			date_published, length, title,
			referred_by, favorite_count) VALUES
			(:id, :views, :rating, :rates,
			:date_published, :length, :title,
			:referred_by, :favorite_count)""" % self.TABLE_NAME, 
			d)
		
	def stop(self):
		logging.info("Stopping")
		self.running = False
	
	def write_state(self):
		"""Push data to disk"""
		
		logging.info("Writing state..")
		logging.debug("\tSaving crawl queue..")
		try:
			f = open(self.CRAWL_QUEUE_FILE + ".new", "w")
			pickle.dump(self.crawl_queue, f)
			f.close()
			
			if os.path.exists(self.CRAWL_QUEUE_FILE):
				os.rename(self.CRAWL_QUEUE_FILE, self.CRAWL_QUEUE_FILE + "~")
			os.rename(self.CRAWL_QUEUE_FILE + ".new", self.CRAWL_QUEUE_FILE)
			
			logging.debug("\tOK")
			logging.debug("\tDatabase commit..")
			self.db.conn.commit()
			logging.info("\tOK")
		
		except:
			logging.exception("Error during writing state")
		
	
	def check_error(self, d):
		if d is not None and str(d).find("too_many_recent_calls") != -1:
			self.throttle_required = True
			self.throttle_next_time = time.time() + self.THROTTLE_STALL_TIME
			logging.warning("too_many_recent_calls encountered. Waiting %s seconds before new requests." % self.THROTTLE_STALL_TIME)
	
	def throttle_back(self):
		"""Call this function when ``too_many_recent_calls`` occurs"""
		
		logging.warning("too_many_recent_calls encountered. Stalling for %s seconds." % self.THROTTLE_STALL_TIME)
		
		time.sleep(self.THROTTLE_STALL_TIME)
		
		logging.info("OK, let's go.")
	
	def process_tasks(self):
		for task in self.tasks:
			if not task.is_alive() and task.error_dict is not None:
				self.check_error(task.error_dict)
			
			if not task.is_alive():
				self.tasks.remove(task)
	
	def get_stats_string(self):
		r = self.vids_crawled_session / (time.time() - self.start_time)
		return "Crawl rate: new %f videos per second (%d total this session)" % (r, self.vids_crawled_session)
	
	def quit(self):
		logging.info("Quiting..")
		self.running = False
		self.write_state()
		self.db.close()

class Task(threading.Thread):
	"""Task for downloading"""
	
	def __init__(self, task, queue=None, uri=None, yt_service=None, referred_by=None, username=None, video_id=None, http_client=None):
		threading.Thread.__init__(self)
		self.setDaemon(True)
		self.task = task
		self.queue = queue
		self.yt_service = yt_service
		self.referred_by = referred_by
		self.uri = uri
		self.error_dict = None
		self.username = username
		self.video_id = video_id
		self.http_client = http_client
	
	def run(self):
		try:
			if self.task == "fetch-entry":
				logging.debug("Fetch single entry")
				entry = self.yt_service.GetYouTubeVideoEntry(video_id=self.video_id)
				entry.referred_by = self.referred_by
				self.queue.put(entry, block=True)
		
			elif self.task == "fetch-feed":
				fetcher = ytextract.FeedFetcher(self.uri, self.yt_service, 
					self.referred_by)
				
				for e in fetcher.fetch():
					e.referred_by = self.referred_by
					self.queue.put(e, block=True)
			
				self.error_dict = fetcher.error_dict
		
			elif self.task == "fetch-user-playlists":
				logging.debug("Get %s's playlists" % self.username)
			
				playlist_uri = "http://gdata.youtube.com/feeds/api/users/%s/playlists?start-index=1&max-results=50" % self.username
				playlists = self.yt_service.GetYouTubePlaylistFeed(uri=playlist_uri)
		
				for entry in playlists.entry:
					for feed_link in entry.feed_link:
						if feed_link.rel == "http://gdata.youtube.com/schemas/2007#playlist":
							uri = feed_link.href
							self.queue.put([uri, None, self.referred_by], block=True)
			
			elif self.task == "fetch-user":
				entry = self.yt_service.GetYouTubeUserEntry(username=self.username.encode("utf-8"))	
				
				entry.referred_by = self.referred_by
				self.queue.put(entry)
		
			elif self.task == "fetch-user-subscribers":
				for u in ytextract.fetch_subscribers(self.username, self.http_client):
					self.queue.put(u, block=True)
			
			else:
				logging.warning("Unknown task %s" % self.task)
		
		except gdata.service.RequestError, d:
			self.error_dict = d
			logging.exception("YouTube error (task: %s" % self.task)
	
def run():
	LOG_FILE = "data/log"
	
	# Catch KeyboardInterrupt for threads
	def sigint_handler(signum, frame):
		sys.exit(0)
		raise KeyboardInterrupt
	
	signal.signal(signal.SIGINT, sigint_handler)
	
	logger = logging.getLogger()
	logger.setLevel(logging.DEBUG)
	
	formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(module)s:%(funcName)s:%(lineno)d: %(message)s")
	rfh = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=4194304, backupCount=9)
	rfh.setLevel(logging.WARNING)
	rfh.setFormatter(formatter)
	logger.addHandler(rfh)
	
	console_formatter = logging.Formatter("%(name)s %(levelname)s: %(message)s")
	sh = logging.StreamHandler()
	sh.setLevel(logging.INFO)
	sh.setFormatter(console_formatter)
	logger.addHandler(sh)
	
	try:
		crawler = Crawler()
		
		server = http_server.Server()
		server.crawler = crawler
		server.start()
		
		if len(crawler.crawl_queue) == 0:
			s = raw_input("No videos to crawl in queue.\nEnter space deliminated video ids or Favorites, Playlist (not feed of playlists), Responses, Upload urls (prefix with http://) and press enter (leave blank for default video):")
			l = s.split()
			if len(l) == 0:
				print "Using default jNQXAC9IVRw"
				crawler.add_to_crawl("jNQXAC9IVRw")
			else:
				for i in l:
					crawler.add_to_crawl(i)
		crawler.run()
	except:
		logging.exception("Run-time error")
		sys.exit(1)
	
if __name__ == "__main__":
	run()
	
