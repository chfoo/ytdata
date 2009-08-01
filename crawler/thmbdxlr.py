"""YouTube video thumbnail downloader"""

import database
import httplib
import os.path
import time
import logging


class Downloader:
	IMG_DIR = "data/thumbnails/"
	YT_IMG_URL = "http://i.ytimg.com/vi/%s/0.jpg"
	THROTTLE_TIME = .1 # seconds
	
	def __init__(self):
		self.db = database.Database()
		self.http = httplib.HTTPConnection("i.ytimg.com")
		
	def run(self):
		# fetch all to prevent locking forever
		rows = self.db.conn.execute("SELECT id from %s" % self.db.TABLE_NAME).fetchall() 
		self.db.close()
		
		counter = 0
		total = len(rows)
		for row in rows:
			counter += 1
			vid_id = row[0]
			
			filename = "%s%s.0.jpg" % (self.IMG_DIR, vid_id)
			if not os.path.exists(filename):
				self.http.request("GET", self.YT_IMG_URL % vid_id)
				
				response = self.http.getresponse()
				
				f = open(filename, "wb")
				f.write(response.read())
				f.close()
				
				logging.info("%d of %d retrieved" % (counter, total))
				time.sleep(self.THROTTLE_TIME)
		
		self.http.close()
			
def run():
	d = Downloader()
	d.run()
	
if __name__ == "__main__":
	logging.basicConfig(level=logging.INFO)
	run()
