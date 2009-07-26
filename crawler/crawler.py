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
import gdata.youtube.service
import gdata.service

import database
import ytextract

class Crawler:
	CRAWL_QUEUE_FILE = "./data/queue.pickle"
	TABLE_NAME = "vidtable1"
	QUEUE_SLEEP_TIME = .1
	WRITE_INTERVAL = 5
	MAX_QUEUE_SIZE = 50
	
	def __init__(self):
		# Get video ids to crawl
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
		self.write_counter = 0
		self.db_insert_queue = []
#		self.db_update_queue = []
		self.in_database_table = {}
		self.was_traversed_table = {}
	
	def _setup_cache(self):
		logging.info("Setting up caches")
		
		# Get list of video ids
		rows = self.db.conn.execute("SELECT id FROM %s" % self.TABLE_NAME).fetchall()
		for row in rows:
			id = row[0]
			self.in_database_table[id] = True
		
		# Get list of not traversed ids
		rows = self.db.conn.execute("SELECT id FROM %s WHERE traversed=1" % self.TABLE_NAME).fetchall()
		
		for row in rows:
			id = row[0]
			self.was_traversed_table[id] = True
		
#		print self.in_database_table
#		print self.was_not_traversed_table
#		sys.exit()
	
	def run(self):
		logging.info("Running")
		self.running = True
		
		self.vids_crawled_session = 0
		self.start_time = time.time()
		self._setup_cache()
		
		while self.running:
			if len(self.crawl_queue) > 0:
				logging.info("Processing queue")
				
				video_id = self.process_queue_item()
				
				#self._setup_cache()
				
				self.write_counter += 1
				if self.write_counter >= self.WRITE_INTERVAL:
					self.write_state()
					self.write_counter = 0
			else:
				logging.info("Crawl queue finished")
				break
			
			r = self.vids_crawled_session / (time.time() - self.start_time)
			logging.info("Crawl rate: new %f videos per second" % r)
			logging.info("Sleeping for %s seconds" % self.QUEUE_SLEEP_TIME)
			time.sleep(self.QUEUE_SLEEP_TIME)
		
		self.write_state()
		logging.info("Run finished")
		
	def in_database(self, video_id):
		"""Get whether video is already in database
		
		:Return:
			`boolean`
		"""
		
#		rows = self.db.conn.execute("SELECT 1 FROM %s WHERE id = ?" % self.TABLE_NAME, (video_id,)).fetchone()
#		return rows is not None #len(rows) > 0
		return video_id in self.in_database_table
	
	def was_traversed(self, video_id):
		"""Get whether video was already traversed
		
		:Return:
			`boolean`
		"""
		
#		row = self.db.conn.execute("SELECT 1 FROM %s WHERE (id=? AND traversed=1)" % self.TABLE_NAME, (video_id,)).fetchone()
#		return row is not None #len(rows) > 0
		return video_id in self.was_traversed_table
	
#	def get_item_crawl(self):
#		"""Return a untraversed video id to traversed"""
#		
#		row = self.db.conn.execute("SELECT id FROM %s WHERE traversed!=1" % self.TABLE_NAME).fetchone()
#		
#		if row is not None:
#			return row[0]
#		else:
#			return None
	
	def process_queue_item(self):
		"""Add video to database and related videos to queue"""
		
		video_id = self.crawl_queue[0]
		logging.info("Process 1 queue item %s", video_id)
		
		has_seen = self.in_database(video_id)
		was_traversed = self.was_traversed(video_id)
		logging.info("\tHas seen: %s; Was traversed: %s" % (has_seen, was_traversed))
	
		#if has_seen:
		#	self.update_entry(video_id)
		
		try:
			entry = None
			if not has_seen:
				entry = self.yt_service.GetYouTubeVideoEntry(video_id=video_id)
				self.add_entry(video_id, entry)
		
			if not was_traversed:
#				if not entry:
#					entry = self.yt_service.GetYouTubeVideoEntry(video_id=video_id)
				self.traverse_video(video_id, entry)
			
		except gdata.service.RequestError:
			logging.error(traceback.format_exc())
			logging.warning("Skipping %s due to YouTube service error" % video_id)
		
		
		return self.crawl_queue.pop(0)
	
	def traverse_video(self, video_id, entry):
		"""Add related videos to queue"""
		
		logging.info("Traversing video %s" % video_id)
		
#		user = entry.author[0].name.text
#		user_uri = "http://gdata.youtube.com/feeds/api/users/%s/uploads" % user
		
		related_feed = self.yt_service.GetYouTubeRelatedVideoFeed(video_id=video_id)
		response_feed = self.yt_service.GetYouTubeVideoResponseFeed(video_id=video_id)
		
#		fav_feed = self.yt_service.GetUserFavoritesFeed(username=user)
#		user_feed = self.yt_service.GetYouTubeVideoFeed(user_uri)
#		entries = related_feed.entry + response_feed.entry + fav_feed.entry + user_feed.entry
		entries = related_feed.entry + response_feed.entry
		queue_toggle = False
		
		for entry in entries:
			id = entry.id.text.rsplit("/", 1)[-1]
			#self.add_entry(id, entry, video_id)
			if not self.in_database(id):
				d = ytextract.extract_from_entry(entry)
				d["id"] = id
				d["referred_by"] = video_id
				logging.info("\tAdding %s to database insert queue" % id)
				logging.debug("\tUsing data %s" % d)
				self.db_insert_queue.append(d)
				self.in_database_table[id] = True
				self.vids_crawled_session += 1
			else:
				logging.info("\t%s already in database, not going to update it" % id)
			
			if self.was_traversed(id):
				logging.info("\t%s already traversed, not going to traverse it" % id)
			elif random.random() < 0.8 or queue_toggle:
				logging.info("\tChosen to not traverse %s" % id)
			elif len(self.crawl_queue) < self.MAX_QUEUE_SIZE:
				self.add_crawl_queue(id)
				if random.randint(0, 1):
					queue_toggle = True
			else:
				logging.info("\tCrawl queue too big, %s not added" % id)
		
		self.process_db_queue()
		
		logging.info("\tMarking %s as traversed" % video_id)
		self.db.conn.execute("""UPDATE %s SET traversed=? WHERE
			id=?""" % self.TABLE_NAME, (1, video_id))
		self.was_traversed_table[video_id] = True
		
		logging.info("\tDone traversing %s" % video_id)
	
	def update_entry(self, video_id, referral_id=None):
		entry = self.yt_service.GetYouTubeVideoEntry(video_id=video_id)
		self.add_entry(video_id, entry)
	
	def add_entry(self, video_id, entry, referral_id=None):
		"""Add video data to database
		
		:Parameters:
			video_id : `str`
				A YouTube video id
			entry : `Entry`
				A YouTube Feed Entry
			referral_id : `str` or `None`
				A YouTube video id to be used as the parent
		"""
		
		logging.info("Adding entry %s", video_id)
		d = ytextract.extract_from_entry(entry)
		logging.debug("\tGot metadata %s", d)
			
		if self.in_database(video_id):
			# Update an entry	
			logging.info("\tDatabase update row")
			self.db.conn.execute("""UPDATE %s SET 
				views=?, rating=?, rates=?, date_published=?,
				length=?, title=?, favorite_count=? WHERE id=?;""" %
					 self.TABLE_NAME, 
				(d["views"], d["rating"], d["rates"], d["date_published"],
				d["length"], d["title"], d["favorite_count"], video_id))
		else:
			# Add an entry
			logging.info("\tInsert row")
			self.db.conn.execute("""INSERT INTO %s
				(id, views, rating, rates, 
				date_published, length, title, favorite_count) VALUES
				(?,?,?,?,?,?,?,?)""" % self.TABLE_NAME, 
				(video_id, 	d["views"], d["rating"], d["rates"],
				d["date_published"], d["length"], d["title"], d["favorite_count"]))
		
		
		if referral_id:
			logging.info("\tAdding referral id %s", referral_id)
			self.db.conn.execute("""UPDATE %s SET referred_by=? WHERE
				id=?""" % self.TABLE_NAME, (referral_id, video_id))
			
		self.in_database_table[video_id] = True
		self.vids_crawled_session += 1
		logging.info("\tDone")
	
	def process_db_queue(self):
		"""Batch insert into database"""
		
		logging.info("Batch processing database queue..")
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
		
		self.db.conn.executemany("""INSERT INTO %s
			(id, views, rating, rates, 
			date_published, length, title,
			referred_by, favorite_count) VALUES
			(:id, :views, :rating, :rates,
			:date_published, :length, :title,
			:referred_by, :favorite_count)""" % self.TABLE_NAME, self.db_insert_queue)
		
		self.db_insert_queue = []
		
		logging.info("OK")
		
	def stop(self):
		logging.info("Stopping")
		self.running = False
	
	def write_state(self):
		"""Push data to disk"""
		
		logging.info("Writing state..")
		logging.info("\tSaving crawl queue..")
		f = open(self.CRAWL_QUEUE_FILE + ".new", "w")
		pickle.dump(self.crawl_queue, f)
		f.close()
		os.rename(self.CRAWL_QUEUE_FILE, self.CRAWL_QUEUE_FILE + "~")
		os.rename(self.CRAWL_QUEUE_FILE + ".new", self.CRAWL_QUEUE_FILE)
		logging.info("\tOK")
		logging.info("\tDatabase commit..")
		self.db.conn.commit()
		logging.info("\tOK")
		
	
	def add_crawl_queue(self, video_id):
		"""Add a video to the crawl queue
		
		:Parameters:
			video_id : `str`
				A YouTube video id
		"""
		
		logging.info("Adding %s to crawl queue", video_id)
		self.crawl_queue.append(video_id)
	
	def quit(self):
		logging.info("Quiting..")
		self.running = False
		self.write_state()
		self.db.close()

def run():
	LOG_FILE = "data/log"
	
	formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(module)s:%(funcName)s:%(lineno)d: %(message)s")
	logging.basicConfig(level=logging.INFO)
	rfh = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=4194304, backupCount=9)
	rfh.setFormatter(formatter)
	logging.getLogger().addHandler(rfh)
	
	try:
		crawler = Crawler()
		if len(crawler.crawl_queue) == 0:
			s = raw_input("No videos to crawl in queue.\nEnter space deliminated video ids and press enter (leave blank for default video):")
			l = s.split()
			if len(l) == 0:
				crawler.add_crawl_queue("jNQXAC9IVRw")
			else:
				for i in l:
					crawler.add_crawl_queue(i)
		crawler.run()
	except:
		logging.error(traceback.format_exc())
	
if __name__ == "__main__":
	run()
	
