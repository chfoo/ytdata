"""Functions to crawl YouTube data"""

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

import time
import datetime
import re
import logging
import threading
import traceback
import gdata.service
import urlparse
import cgi

class FeedDownloader(threading.Thread):
	"""Downloads all the `Entry` in a feed by paging though it as necessary
	
	:attributes:
		entires : `list`
			A list of all the entry objects
	"""
	
	FETCH_DELAY = 1 # seconds
	
	def __init__(self, feed_uri, yt_service, referred_by=None):
		threading.Thread.__init__(self)
		logging.debug("New download thread")
		self.setDaemon(True)
		self.feed_uri = feed_uri
		self.entries = []
		self.yt_service = yt_service
		self.referred_by = referred_by
		self.error_dict = None
	
	def run(self):
		feed_uri = self.feed_uri
		yt_service = self.yt_service
		
		while True:
			logging.debug("Grabbing feed %s" % feed_uri)
			
			try:
				current_feed = yt_service.GetYouTubeVideoFeed(uri=feed_uri)
			except gdata.service.RequestError, d:
				self.error_dict = d
				logging.exception("Error grabbing YouTube Video Feed")
				logging.warning("Skipping %s due to YouTube service error" % feed_uri)
				break
			
			if current_feed is None:
				logging.warning("Skipping %s due to YouTube service error" % feed_uri)
				break
		
			self.entries.extend(current_feed.entry)
			
			feed_uri = None
			for link in current_feed.link:
				if link.rel == "next":
					feed_uri = link.href
					break
			
			if feed_uri is None:
				break
			
			time.sleep(self.FETCH_DELAY)

def extract_from_entry(entry):
	"""Return a `dict` of useful info"""
	
#	print entry.__dict__
	d = {}
#	d["id"] = entry.id.text.rsplit("/", 1)[-1] # this isn't the real id!
# it differs on playlists

	for link in entry.link:
		if link.rel == "alternate":
			query = urlparse.urlparse(link.href)[4]
			qd = cgi.parse_qs(query)
			if "v" in qd:
				d["id"] = qd["v"][0]
			else:
				d["id"] = qd["video_id"][0]
	
	logging.debug("Extracting data from %s" % d["id"])
	
	if entry.media.title.text:
		d["title"] = entry.media.title.text.decode("utf-8")
	else:
		d["title"] = None
	
	if entry.rating:
		d["rating"] = float(entry.rating.average)
		d["rates"] = int(entry.rating.num_raters)
	else:
		d["rating"] = None
		d["rates"] = None
	
	if entry.published:
		d["date_published"] = convert_time(entry.published.text)
	else:
		d["date_published"] = None
	
	if entry.media.duration:
		d["length"] = int(entry.media.duration.seconds)
	else:
		d["length"] = None
	
	if entry.statistics:
		d["views"] = int(entry.statistics.view_count)
		d["favorite_count"] = int(entry.statistics.favorite_count)
	else:
		d["views"] = None
		d["favorite_count"] = None
	
	return d

def extract_from_user_entry(entry):
	d = {}
	d["username"] = unicode(entry.username.text)
	d["join_date"] = convert_time(entry.published.text)
	d["videos_watched"] = entry.statistics.video_watch_count
	d["subscribers"] = entry.statistics.subscriber_count
	d["views"] = entry.statistics.view_count
	
	for feed_link in entry.feed_link:
		if feed_link.rel == "http://gdata.youtube.com/schemas/2007#user.favorites":
			if feed_link.count_hint:
				d["favorites"] = feed_link.count_hint
			else:
				d["favorites"] = None
		elif feed_link.rel == "http://gdata.youtube.com/schemas/2007#user.subscriptions":
			if feed_link.count_hint:
				d["subscriptions"] = feed_link.count_hint
			else:
				d["subscriptions"] = None
	
	return d
	

def convert_time(s):
	"""Convert time into epoch/UTC seconds"""
	match = re.match("(\d+)-(\d+)-(\d+).?(\d+):(\d+):(\d+).*([-+]\d+):(\d+)", s)
	if match:
		groups = match.groups()
		d = map(int, groups)
		t = d[:6] + [0, TZInfo(*d[6:])]
		return int(time.mktime(datetime.datetime(*t).utctimetuple()) - time.mktime((1970,1,1,0,0,0,0,1,0)))
	else:
		match = re.match("(\d+)-(\d+)-(\d+).?(\d+):(\d+):(\d+)", s)
		groups = match.groups()
		t = map(int, groups)
		return int(time.mktime(datetime.datetime(*t).utctimetuple()) - time.mktime((1970,1,1,0,0,0,0,1,0)))

class TZInfo(datetime.tzinfo):
	def __init__(self, hour, minute):
		self.hour = hour
		self.minute = minute
	
	def utcoffset(self, dt):
		return datetime.timedelta(hours=self.hour, minutes=self.minute )
	
	def dst(self, dt):
		return None
	
	def tzname(self, dt):
		return None

if __name__ == "__main__":
	print time.gmtime(convert_time('2009-07-14T08:43:30.000Z'))
	print convert_time("2007-10-15T15:39:34.000-07:00")
	print time.gmtime(convert_time("2007-10-15T15:39:34.000-07:00"))


