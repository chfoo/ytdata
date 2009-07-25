import time
import datetime
import re

def extract_from_entry(entry):
#	print entry.__dict__
	d = {}
	d["title"] = entry.media.title.text
	if entry.rating:
		d["rating"] = entry.rating.average
		d["rates"] = entry.rating.num_raters
	else:
		d["rating"] = None
		d["rates"] = None
	d["date_published"] = convert_time(entry.published.text)
	if entry.media.duration:
		d["length"] = entry.media.duration.seconds
	else:
		d["length"] = None
	d["title"] = entry.media.title.text.decode("utf-8")
	if entry.statistics:
		d["views"] = entry.statistics.view_count
	else:
		d["views"] = None
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


