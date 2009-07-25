
import os
import os.path
import sys
import pickle
import logging
import logging.handlers
import traceback
import time
import gdata.youtube.service

import database
import ytextract

class Crawler:
	CRAWL_QUEUE_FILE = "./data/queue.pickle"
	TABLE_NAME = "vidtable1"
	QUEUE_SLEEP_TIME = 2
	def __init__(self):
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
	
	def run(self):
		logging.info("Running")
		self.running = True
		
		while self.running:
			if len(self.crawl_queue) > 0:
				logging.info("Processing queue")
				
				video_id = self.process_queue_item()
				
				self.write_state()
			else:
				logging.info("Crawl queue finished")
				break
			
			logging.info("Sleeping for %s seconds" % self.QUEUE_SLEEP_TIME)
			time.sleep(self.QUEUE_SLEEP_TIME)
		
		logging.info("Run finished")
		
	def in_database(self, video_id):
		rows = self.db.conn.execute("SELECT 1 FROM %s WHERE id = ?" % self.TABLE_NAME, (video_id,)).fetchone()
		return rows is not None #len(rows) > 0
	
	def was_traversed(self, video_id):
		rows = self.db.conn.execute("SELECT 1 FROM %s WHERE (id=? AND traversed=1)" % self.TABLE_NAME, (video_id,)).fetchone()
		return rows is not None #len(rows) > 0
	
	
	def process_queue_item(self):
		video_id = self.crawl_queue[0]
		logging.info("Process 1 queue item %s", video_id)
		
		has_seen = self.in_database(video_id)
		was_traversed = self.was_traversed(video_id)
		logging.info("\tHas seen: %s" % has_seen)
		logging.info("\tWas traversed: %s" % was_traversed)
		
	
		#if has_seen:
		#	self.update_entry(video_id)
		
		try:
			entry = self.yt_service.GetYouTubeVideoEntry(video_id=video_id)
			
			if not has_seen:
				self.add_entry(video_id, entry)
		
			if not was_traversed:
				self.traverse_video(video_id)
			
		except:
			logging.error(traceback.format_exc())
			logging.warning("Skipping %s due to YouTube service error" % video_id)
		
		
		return self.crawl_queue.pop(0)
	
	def traverse_video(self, video_id):
		logging.info("Traversing video %s" % video_id)
		related_feed = self.yt_service.GetYouTubeRelatedVideoFeed(video_id=video_id)
		response_feed = self.yt_service.GetYouTubeVideoResponseFeed(video_id=video_id)
		entries = related_feed.entry + response_feed.entry
		
		for entry in entries:
			id = entry.id.text.split("/")[-1]
			self.add_entry(id, entry, video_id)
			if len(self.crawl_queue) < 100000:
				self.add_crawl_queue(id)
			else:
				logging.warning("\tQueue too big, %s not added" % id)
		
		self.db.conn.execute("""UPDATE %s SET traversed=? WHERE
			id=?""" % self.TABLE_NAME, (1, video_id))
		
		logging.info("Done traversing %s" % video_id)
	
	def update_entry(self, video_id, referral_id=None):
		entry = self.yt_service.GetYouTubeVideoEntry(video_id=video_id)
		self.add_entry(video_id, entry)
	
	def add_entry(self, video_id, entry, referral_id=None):
		logging.info("Adding entry %s", video_id)
		d = ytextract.extract_from_entry(entry)
		logging.debug("\tGot metadata %s", d)
			
		if self.in_database(video_id):
			# Update an entry	
			logging.info("\tDatabase update row")
			self.db.conn.execute("""UPDATE %s SET 
				views=?, rating=?, rates=?, date_published=?,
				length=?, title=? WHERE id=?;""" % self.TABLE_NAME, 
				(d["views"], d["rating"], d["rates"], d["date_published"],
				d["length"], d["title"], video_id))
		else:
			# Add an entry
			logging.info("\tInsert row")
			self.db.conn.execute("""INSERT INTO %s
				(id, views, rating, rates, 
				date_published, length, title) VALUES
				(?,?,?,?,?,?,?)""" % self.TABLE_NAME, 
				(video_id, 	d["views"], d["rating"], d["rates"],
				d["date_published"], d["length"], d["title"]))
		
		if referral_id:
			logging.info("\tAdding referral id %s", referral_id)
			self.db.conn.execute("""UPDATE %s SET referred_by=? WHERE
				id=?""" % self.TABLE_NAME, (referral_id, video_id))
		
		logging.info("\tDone")
	
	def stop(self):
		logging.info("Stopping")
		self.running = False
	
	def write_state(self):
		logging.info("Writing state..")
		logging.info("\tSaving crawl queue..")
		f = open(self.CRAWL_QUEUE_FILE, "w")
		pickle.dump(self.crawl_queue, f)
		f.close()
		logging.info("\tOK")
		logging.info("\tDatabase commit..")
		self.db.conn.commit()
		logging.info("\tOK")
		
	
	def add_crawl_queue(self, video_id):
		logging.info("Adding %s to crawl queue", video_id)
		self.crawl_queue.append(video_id)
	
	def quit(self):
		logging.info("Quiting..")
		self.running = False
		self.write_state()
		self.db.close()

if __name__ == "__main__":
	LOG_FILE = "data/log"
	
	formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(module)s:%(funcName)s:%(lineno)d: %(message)s")
	logging.basicConfig(level=logging.INFO)
	rfh = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=67108864)
	rfh.setFormatter(formatter)
	logging.getLogger().addHandler(rfh)
	
	try:
		crawler = Crawler()
		if len(crawler.crawl_queue) == 0:
			crawler.add_crawl_queue("jNQXAC9IVRw")
		crawler.run()
	except:
		logging.error(traceback.format_exc())
