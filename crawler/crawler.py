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

import database
import ytextract

class Crawler:
	CRAWL_QUEUE_FILE = "./data/queue.pickle"
	TABLE_NAME = "vidtable1"
	USER_TABLE_NAME = "usertable1"
	QUEUE_SLEEP_TIME = .1 # seconds
	WRITE_INTERVAL = 5
	MAX_QUEUE_SIZE = 50
	MAX_DOWNLOAD_THREADS = 4
	DOWNLOAD_STALL_TIME = 2 # seconds
	TRAVERSE_RATE = 0.05 # Crawl related videos
	USER_TRAVERSE_RATE = 0.5 # crawl user favs, uploads, playlists
	THROTTLE_STALL_TIME = 60 * 5 # seconds
	RECENT_VIDS_URI = "http://gdata.youtube.com/feeds/api/standardfeeds/most_recent"
	RECENT_VIDS_INTERVAL = 3600 # seconds
	
	def __init__(self):
		# Crawl queue:
		# A list of [feed uri string, video id, referring video id]
		if os.path.exists(self.CRAWL_QUEUE_FILE):
			f = open(self.CRAWL_QUEUE_FILE, "r")
			self.crawl_queue = pickle.load(f) 
			f.close()
		else:
			logging.warning("Crawl queue file not found. Queue is empty")
			self.crawl_queue = []
		
		self.db = database.Database()
		self.yt_service = gdata.youtube.service.YouTubeService()
		self.running = False
		self.write_counter = 0 # Counter for write interval
#		self.db_insert_queue = [] # List of arguements to be inserted into db
#		self.db_update_queue = []
#		self.video_traversed_table = None # anydbm
#		self.user_traversed_table = None # anydmb
#		self.playlist_traversed_table = None # anydbm
		self.download_threads = []
		self.entry_queue = [] # A list of YouTube Entry to be inserted into db
		self.lock = threading.Lock()
	
#	def _setup_cache(self):
#		"""Setup on-disk tables"""
#		
#		logging.debug("Setting up table caches")
#		
#		# http://jjinux.blogspot.com/2008/08/python-memory-conservation-tip_05.html
##		tdir = tempfile.mkdtemp()
##		self.video_traversed_table = shelve.open(os.path.join(tdir, "vid"))
##		self.user_traversed_table = shelve.open(os.path.join(tdir, "user"))		
##		shutil.rmtree(tdir)
#		
#		# Get list of video ids
#		rows = self.db.conn.execute("SELECT id, traversed FROM %s" % self.TABLE_NAME)
#		for row in rows:
#			self.video_traversed_table[row[0].encode("utf-8")] = (row[1] == 1)
#		
#		
#		# Get user and traversed status
#		rows = self.db.conn.execute("SELECT username, traversed FROM %s" % self.USER_TABLE_NAME)
#		
#		for row in rows:
#			self.user_traversed_table[row[0].encode("utf-8")] = (row[1] == 1)
#		
#		
##		# Get playlist and traversed status
##		rows = self.db.conn.execute("SELECT id, traversed FROM %s" % self.db.PLAYLIST_TABLE_NAME)
##		for row in rows:
##			self.playlist_traversed_table[row[0]] = (row[1] == 1)
#		
##		print self.in_database_table
##		print self.was_not_traversed_table
##		sys.exit()
	
	def run(self):
		logging.info("Running")
		self.running = True
		
		self.vids_crawled_session = 0
		self.start_time = time.time()
#		self._setup_cache()
		recent_vids_next_time = time.time() + self.RECENT_VIDS_INTERVAL
		
		while self.running and (len(self.crawl_queue) > 0 or len(self.download_threads) > 0 or len(self.entry_queue) > 0):
			logging.debug("Run iteration")
			
			if time.time() >= recent_vids_next_time:
				logging.debug("Inject recent videos to crawl queue")
				self.add_uri_to_crawl(self.RECENT_VIDS_URI)
				recent_vids_next_time = time.time() + self.RECENT_VIDS_INTERVAL
			
			if len(self.crawl_queue) > 0:
				self.process_crawl_queue_item()
			
			self.process_entries()
			self.process_downloaders()
			
			self.write_counter += 1
			if self.write_counter >= self.WRITE_INTERVAL:
				self.write_state()
				self.write_counter = 0
			
			r = self.vids_crawled_session / (time.time() - self.start_time)
			logging.info("Crawl rate: new %f videos per second (%d total this session)" % (r, self.vids_crawled_session))
			if len(self.crawl_queue) == 0:
				logging.debug("Sleeping for %s seconds" % self.DOWNLOAD_STALL_TIME)
				time.sleep(self.DOWNLOAD_STALL_TIME)
			else:
				logging.debug("Sleeping for %s seconds" % self.QUEUE_SLEEP_TIME)
				time.sleep(self.QUEUE_SLEEP_TIME)
		
		self.write_state()
		logging.info("Run finished. This shouldn't happen; try adding more video ids to crawl or adjusting the traversal rate.")
		
	def in_database(self, video_id):
		"""Get whether video is already in database
		
		:Return:
			`boolean`
		"""
		#video_id = video_id.encode("utf-8")
		#return video_id in self.video_traversed_table
		
		r = self.db.conn.execute("SELECT 1 FROM %s WHERE id = ? LIMIT 1" % self.db.TABLE_NAME, [video_id]).fetchone()
		return r is not None and r[0] == 1
	
	def was_traversed(self, video_id):
		"""Get whether video was already traversed
		
		:Return:
			`boolean`
		"""
#		video_id = video_id.encode("utf-8")
#		r = (video_id in self.video_traversed_table and self.video_traversed_table[video_id])
		r = self.db.conn.execute("SELECT traversed FROM %s WHERE id = ? LIMIT 1" % self.db.TABLE_NAME, [video_id]).fetchone()
		return r is not None and r[0] == 1
	
	def user_in_database(self, username):
		r = self.db.conn.execute("SELECT 1 FROM %s WHERE username = ? LIMIT 1" % self.db.USER_TABLE_NAME, [username]).fetchone()
		return r is not None and r[0] == 1
		
	def user_was_traversed(self, username):
		r = self.db.conn.execute("SELECT traversed FROM %s WHERE username = ? LIMIT 1" % self.db.USER_TABLE_NAME, [username]).fetchone()
		return r is not None and r[0] == 1
		
	
	
#	def get_item_crawl(self):
#		"""Return a untraversed video id to traversed"""
#		
#		row = self.db.conn.execute("SELECT id FROM %s WHERE traversed!=1" % self.TABLE_NAME).fetchone()
#		
#		if row is not None:
#			return row[0]
#		else:
#			return None
	
	def process_crawl_queue_item(self):
		"""Process a Feed uri to crawl"""
		
		uri, video_id, referred_by = self.crawl_queue[0]
		logging.info("Crawling %s %s" % (uri, video_id))
		
		# Should be a single video
		if video_id is not None:
			has_seen = self.in_database(video_id)
			was_traversed = self.was_traversed(video_id)
			logging.debug("\tHas seen: %s; Was traversed: %s" % (has_seen, was_traversed))
			
			if not has_seen:
				try:
					entry = self.yt_service.GetYouTubeVideoEntry(video_id=video_id)
					logging.debug("\tAdding to entry queue")
					self.process_entry(entry, referred_by)
			
				except gdata.service.RequestError, d:
					logging.exception("\tError getting YouTube video entry")
					logging.warning("\tSkipping %s due to YouTube service error" % video_id)
					self.check_error(d)
		
			if not was_traversed:
				logging.debug("\tTraversing")
			
				self.traverse_video(video_id)
			
		
		# Should be a playlist 
		else:
			
			if len(self.download_threads) >= self.MAX_DOWNLOAD_THREADS:
				logging.warning("\tToo many download threads (Now: %d; Max: %d). Stalling for %s seconds." % (len(self.download_threads), self.MAX_DOWNLOAD_THREADS, self.DOWNLOAD_STALL_TIME))
			while len(self.download_threads) >= self.MAX_DOWNLOAD_THREADS:
				self.process_downloaders()
				time.sleep(self.DOWNLOAD_STALL_TIME)
			
			d1 = ytextract.FeedDownloader(uri, self.yt_service, referred_by)
			d1.start()
		
			self.download_threads.append(d1)
		
		
		return self.crawl_queue.pop(0)
	
	def process_downloaders(self):
		for downloader in self.download_threads:
			if not downloader.isAlive():
				logging.debug("A download thread has completed")
				
				self.check_error(downloader.error_dict)
				
				self.download_threads.remove(downloader)
			
			# http://jessenoller.com/2009/02/01/python-threads-and-the-global-interpreter-lock/
			self.lock.acquire()
			entries = []
			try:
				entries = downloader.entries
				downloader.entries = []
			finally:
				self.lock.release()
				
			for entry in entries:
				self.process_entry(entry, downloader.referred_by)
			
			time.sleep(0.5)
	
	def process_entries(self):
		for entry in self.entry_queue:
			self.entry_queue.remove(entry)
			self.process_entry(entry)
		
#		self.process_db_queue()
	
	def process_entry(self, entry, referred_by=None):
		d = ytextract.extract_from_entry(entry)
		d["referred_by"] = referred_by
		logging.info("Processing entry %s" % d["id"])
		logging.debug("\tData: %s" % d)
		
		if self.in_database(d["id"]):
			logging.debug("\tAlready database. Not updating")
		else:
			#logging.info("\tNew. Queueing for database insert")
			#self.db_insert_queue.append(d)
			logging.info("\tNew! Adding to database.")
			self.db_video_insert(d)
#			self.video_traversed_table[d["id"]] = False
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
		
		username = entry.author[0].name.text
		if self.user_was_traversed(username):
			logging.debug("\tUser was already traversed.")
		elif random.random() > self.USER_TRAVERSE_RATE:
			logging.debug("\tDecided to not traverse user")
		elif len(self.crawl_queue) >= self.MAX_QUEUE_SIZE:
			logging.debug("\tCrawl queue too large. Not traversing user.")
		else:
			self.traverse_user(username)
		
	
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
#		self.video_traversed_table[video_id] = True
		
		logging.debug("\tDone traversing %s" % video_id)
		
	
	def traverse_user(self, username):
		logging.info("Traversing user %s" % username)
		try:
			entry = self.yt_service.GetYouTubeUserEntry(username=username)
		
			d = ytextract.extract_from_user_entry(entry)
			logging.debug("\tGot user data %s" % d)
			
			logging.debug("\tInsert user data into database")
			self.db.conn.execute("""INSERT INTO %s (username, videos_watched)
				VALUES (?,?)""" % self.db.USER_TABLE_NAME, 
				(username, d["videos_watched"]))
		except gdata.service.RequestError, d:
			logging.exception("YouTube request error for user %s" % username)
			logging.warning("Skipping user %s statistics " % username)
			
			self.check_error(d)
			
			logging.debug("\tInsert username into database")
			
			self.db.conn.execute("""INSERT INTO %s (username)
				VALUES (?)""" % self.db.USER_TABLE_NAME, [username])
			
		
		playlist_uri = "http://gdata.youtube.com/feeds/api/users/%s/playlists?start-index=1&max-results=50" % username
		try:
			playlists = self.yt_service.GetYouTubePlaylistFeed(uri=playlist_uri)
		
			for entry in playlists.entry:
				for feed_link in entry.feed_link:
					if feed_link.rel == "http://gdata.youtube.com/schemas/2007#playlist":
						uri = feed_link.href
						self.add_uri_to_crawl(uri)
		
		except gdata.service.RequestError, d:
			logging.exception("Unable to get playlists for %s " % username)
			self.check_error(d)
			logging.warning("Skipping playlists for %s" % username)
		
		fav_uri = "http://gdata.youtube.com/feeds/api/users/%s/favorites?start-index=1&max-results=50" % username
		uploads_uri = "http://gdata.youtube.com/feeds/api/users/%s/uploads?start-index=1&max-results=50" % username
		self.add_uri_to_crawl(fav_uri)
		self.add_uri_to_crawl(uploads_uri)
		
		logging.debug("\tMarking user %s as traversed" % username)
#		self.user_traversed_table[username.encode("utf-8")] = True
		self.db.conn.execute("""UPDATE %s SET traversed=? WHERE
			username=?""" % self.db.USER_TABLE_NAME, (1, username))

	def add_uri_to_crawl(self, uri, video_id=None, referred_by=None):
		logging.debug("Adding uri to queue %s, video_id %s, referred_by %s" % (uri, video_id, referred_by))
		self.crawl_queue.append([uri, video_id, referred_by])
	
#	def update_entry(self, video_id, referral_id=None):
#		entry = self.yt_service.GetYouTubeVideoEntry(video_id=video_id)
#		self.add_entry(video_id, entry)
	
#	def add_entry(self, video_id, entry, referral_id=None):
#		"""Add video data to database
#		
#		:Parameters:
#			video_id : `str`
#				A YouTube video id
#			entry : `Entry`
#				A YouTube Feed Entry
#			referral_id : `str` or `None`
#				A YouTube video id to be used as the parent
#		"""
#		
#		logging.info("Adding entry %s", video_id)
#		d = ytextract.extract_from_entry(entry)
#		logging.debug("\tGot metadata %s", d)
#			
#		if self.in_database(video_id):
#			# Update an entry	
#			logging.info("\tDatabase update row")
#			self.db.conn.execute("""UPDATE %s SET 
#				views=?, rating=?, rates=?, date_published=?,
#				length=?, title=?, favorite_count=? WHERE id=?;""" %
#					 self.TABLE_NAME, 
#				(d["views"], d["rating"], d["rates"], d["date_published"],
#				d["length"], d["title"], d["favorite_count"], video_id))
#		else:
#			# Add an entry
#			logging.info("\tInsert row")
#			self.db.conn.execute("""INSERT INTO %s
#				(id, views, rating, rates, 
#				date_published, length, title, favorite_count) VALUES
#				(?,?,?,?,?,?,?,?)""" % self.TABLE_NAME, 
#				(video_id, 	d["views"], d["rating"], d["rates"],
#				d["date_published"], d["length"], d["title"], d["favorite_count"]))
#		
#		
#		if referral_id:
#			logging.info("\tAdding referral id %s", referral_id)
#			self.db.conn.execute("""UPDATE %s SET referred_by=? WHERE
#				id=?""" % self.TABLE_NAME, (referral_id, video_id))
#		
#		if video_id not in self.video_traversed_table:
#			self.video_traversed_table[video_id] = False
#		
#		self.vids_crawled_session += 1
#		logging.info("\tDone")
	
	
#	def process_db_queue(self):
#		"""Batch insert into database"""
#		
#		logging.debug("Batch processing database queue..")
#		self.db.conn.execute("""UPDATE %s SET 
#			views=:views, 
#			rating=:rating, 
#			rates=:rates, 
#			date_published=:date_published,
#			length=:length,
#			title=:title 
#			WHERE id=:id""" % self.TABLE_NAME, self.db_update_queue)
#		
#		self.db_update_queue = []
		
#		self.db.conn.executemany("""INSERT INTO %s
#			(id, views, rating, rates, 
#			date_published, length, title,
#			referred_by, favorite_count) VALUES
#			(:id, :views, :rating, :rates,
#			:date_published, :length, :title,
#			:referred_by, :favorite_count)""" % self.TABLE_NAME, self.db_insert_queue)
#		
#		self.db_insert_queue = []
#		
#		logging.debug("OK")
	
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
		
	
#	def add_crawl_queue(self, video_id):
#		"""Add a video to the crawl queue
#		
#		:Parameters:
#			video_id : `str`
#				A YouTube video id
#		"""
#		
#		logging.info("Adding %s to crawl queue", video_id)
#		self.crawl_queue.append(video_id)
	
	def check_error(self, d):
		if d is not None and str(d).find("too_many_recent_calls") != -1:
			self.throttle_back()
	
	def throttle_back(self):
		"""Call this function when ``too_many_recent_calls`` occurs"""
		
		logging.warning("too_many_recent_calls encountered. Stalling for %s seconds." % self.THROTTLE_STALL_TIME)
		time.sleep(self.THROTTLE_STALL_TIME)
		logging.info("OK, let's go.")
	
	def quit(self):
		logging.info("Quiting..")
		self.running = False
		self.write_state()
		self.db.close()

def run():
	LOG_FILE = "data/log"
	
	# Catch KeyboardInterrupt for threads
	def sigint_handler(signum, frame):
		raise KeyboardInterrupt
	
	signal.signal(signal.SIGINT, sigint_handler)
	
	logger = logging.getLogger()
	logger.setLevel(logging.INFO)
	
	formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(module)s:%(funcName)s:%(lineno)d: %(message)s")
	rfh = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=4194304, backupCount=9)
	rfh.setLevel(logging.DEBUG)
	rfh.setFormatter(formatter)
	logger.addHandler(rfh)
	
	console_formatter = logging.Formatter("%(name)s %(levelname)s: %(message)s")
	sh = logging.StreamHandler()
	sh.setLevel(logging.INFO)
	sh.setFormatter(console_formatter)
	logger.addHandler(sh)
	
	try:
		crawler = Crawler()
		if len(crawler.crawl_queue) == 0:
			s = raw_input("No videos to crawl in queue.\nEnter space deliminated video ids or Favorites, Playlist (not feed of playlists), Responses, Upload urls (prefix with http://) and press enter (leave blank for default video):")
			l = s.split()
			if len(l) == 0:
				print "Using default jNQXAC9IVRw"
				crawler.add_uri_to_crawl(None, video_id="jNQXAC9IVRw")
			else:
				for i in l:
					if i.startswith("http://"):
						print "Adding feed", i
						crawler.add_uri_to_crawl(i)
					else:
						print "Adding video", i
						crawler.add_uri_to_crawl(None, video_id=i)
		crawler.run()
	except:
		logging.exception("Run-time error")
	
if __name__ == "__main__":
	run()
	
