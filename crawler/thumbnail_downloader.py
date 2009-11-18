"""YouTube video thumbnail downloader"""

import database
import httplib
import os.path
import time
import logging
import logging.handlers
import cStringIO as StringIO
import gzip

class Downloader:
	IMG_DIR = "data/thumbnails/"
	YT_IMG_URL = "http://i.ytimg.com/vi/%s/2.jpg" 
	# 0: either the frame for 1, 2, 3, or paid account thumbnail; 1: near start, 2: middle, 3: near end
	# opting for 2 since paid account thumbnails chosen by uploader isn't representative of hte video
	THROTTLE_TIME = .1 # seconds
	THROTTLE_MINOR_TIME = 60 # seconds
	THROTTLE_MAJOR_TIME = 300 # seconds
	
	def __init__(self):
		self.db = database.Database()
		self.http = httplib.HTTPConnection("i.ytimg.com")
		
	def run(self):
		start_i = 0
		limit = 100000
		headers = {}
		headers["Accept-encoding"] = "gzip"
		headers["User-Agent"] = " chfoo-crawler (gzip, http://www.student.cs.uwaterloo.ca/~chfoo/ytdata/)"
		
		counter = 0
		while True:
			# fetch to prevent locking forever
			rows = self.db.conn.execute("SELECT id FROM %s LIMIT %d OFFSET %d" 
			% (self.db.TABLE_NAME, limit, start_i)).fetchall() 
			self.db.close()
			
			if len(rows) == 0:
				logging.debug("no more rows")
				break
			
			for row in rows:
				counter += 1
				vid_id = row[0]
			
				filename = "%s%s.2.jpg" % (self.IMG_DIR, vid_id)
				if not os.path.exists(filename):
					logging.info("Fetching %s" % filename)
					
					self.http.request("GET", self.YT_IMG_URL % vid_id, 
					headers=headers)
				
					response = self.http.getresponse()
				
					if response.getheader("Content-Encoding", None) == "gzip":
						logging.debug("\tGzip encoding response")
						string_buf = StringIO.StringIO(response.read())
						g_o = StringIO.StringIO(gzip.GzipFile(fileobj=string_buf).read())
						response.read = g_o.read
				
					if response.status == 200:
				
						f = open(filename, "wb")
						f.write(response.read())
						f.close()
				
						logging.info("%d retrieved" % (counter))
					
						time.sleep(self.THROTTLE_TIME)
					else:
						logging.warning("Throttling (%s %s: %s)" % (response.status,
						response.reason, response.read()))
					
						time.sleep(self.THROTTLE_MAJOR_TIME)
			
			start_i += limit
			time.sleep(self.THROTTLE_MINOR_TIME)
			
		self.http.close()
			
def run():
	d = Downloader()
	d.run()
	
if __name__ == "__main__":

	LOG_FILE = "data/log.thumbnail_downloader"


	logger = logging.getLogger()
	logger.setLevel(logging.DEBUG)

	formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(module)s:%(funcName)s:%(lineno)d: %(message)s")
	rfh = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=4194304, backupCount=9)
	rfh.setLevel(logging.WARNING)
	rfh.setFormatter(formatter)
	logger.addHandler(rfh)

	console_formatter = logging.Formatter("%(name)s %(levelname)s: %(message)s")
	sh = logging.StreamHandler()
	sh.setLevel(logging.DEBUG)
	sh.setFormatter(console_formatter)
	logger.addHandler(sh)

	try:
		logging.debug("start run")
		run()
		logging.debug("run finished")
	except:
		logging.exception("run time error")
	
