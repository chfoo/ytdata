# encoding=utf-8

"""Build HTML for statistics display"""

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
from lxml.html import builder as E
import sys
sys.path.append("../crawler/")
import time
import locale

import database

def html(html):
	cwd = os.getcwd()
	os.chdir("../crawler")
	db = database.Database()
	database_size = os.path.getsize(db.DB_FILE)
	os.chdir(cwd)
	
	total_videos = db.conn.execute("SELECT COUNT(*) FROM %s" % db.TABLE_NAME).fetchone()[0]
	total_length = db.conn.execute("SELECT SUM(length) FROM %s" % db.TABLE_NAME).fetchone()[0]
	total_views = db.conn.execute("SELECT SUM(views) FROM %s" % db.TABLE_NAME).fetchone()[0]
	total_hours = total_length / 3600.
	total_rates = db.conn.execute("SELECT SUM(rates) FROM %s" % db.TABLE_NAME).fetchone()[0]
	total_favs = db.conn.execute("SELECT SUM(favorite_count) FROM %s" % db.TABLE_NAME).fetchone()[0]
	avg_rating = db.conn.execute("SELECT AVG(rating) FROM %s" % db.TABLE_NAME).fetchone()[0]
	avg_views = float(total_views) / total_videos
	avg_length = float(total_length) / total_videos
	avg_favs = float(total_favs) / total_videos
	
	e = html.xpath("//div[@id='mainContent']")[0]
	e.extend([
		E.DIV(E.BIG("Of ", 
			E.BIG(E.STRONG("%d" % total_videos)),
			 " videos ",
			"on YouTube, there are ", E.BIG(E.STRONG("%d" % total_views)), " views ",
			"and ", E.BIG(E.STRONG("%d" % total_hours)), " hours of content")),
		E.P(
			"Aproximately %.1f%% of YouTube has been crawled. " % (total_videos / 140000000. * 100),
			u"At this rate, it’s going to take %.1f months for me to complete the crawl." % 
				((140000000 - total_videos) / (8.0 * 2629743.83 / 2.0)),
			),
		E.DIV("Statistics breakdown:",
			E.TABLE(
				E.TR(E.TD("Last updated"), 
					E.TD(time.strftime("%Y-%m-%d %H:%M:%S Z", 
						time.gmtime(os.path.getmtime("../crawler/" + db.DB_FILE))))),
				E.TR(E.TD("Database size (bytes)"), 
					E.TD("%d"  % database_size, {"class":"tableNumber"})),
				E.TR(E.TD("Videos"), 
					E.TD("%d" % total_videos, {"class":"tableNumber"})),
				E.TR(E.TD("Views"), 
					E.TD("%d" % total_views, {"class":"tableNumber"})),
				E.TR(E.TD("Length (seconds)"), 
					E.TD("%d" % total_length, {"class":"tableNumber"})),
				E.TR(E.TD("Length (years)"), 
					E.TD("%.2f" % (total_length / 31556926.),
						 {"class":"tableNumber"})),
				E.TR(E.TD("Rates"), 
					E.TD("%d" % total_rates, {"class":"tableNumber"})),
				E.TR(E.TD("Favourites"), 
					E.TD("%d" % total_favs, {"class":"tableNumber"})),
				E.TR(E.TD("Average rating per video"), 
					E.TD("%.2f" % avg_rating, {"class":"tableNumber"})),
				E.TR(E.TD("Average views per video"), 
					E.TD("%d" % avg_views, {"class":"tableNumber"})),
				E.TR(E.TD("Average seconds per video"), 
					E.TD("%d" % avg_length, {"class":"tableNumber"})),
				E.TR(E.TD("Average minutes per video"), 
					E.TD("%.2f" % (avg_length / 60.), {"class":"tableNumber"})),
				E.TR(E.TD("Average favourites per video"), 
					E.TD("%.2f" % avg_favs, {"class":"tableNumber"})),
				border="1",
			))
		]
		)
	#e = html.xpath("//head")[0]
	#e.append(E.META({"http-equiv":"refresh", "content":"20"}))
	
def num_format(num):
	pass
