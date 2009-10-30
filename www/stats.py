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
import cgi

import database

def html(html):
	form = cgi.FieldStorage(keep_blank_values=True)
	if "gen" in form:
	
		cwd = os.getcwd()
		os.chdir("../crawler")
		db = database.Database()
		database_size = os.path.getsize(db.DB_FILE)
		os.chdir(cwd)
	
		total_videos, total_exist, total_length, total_views, total_rates, \
		total_favs, avg_rating, avg_views, avg_length, avg_favs, \
		= db.conn.execute(
			"""SELECT
			COUNT(*),
			COUNT(length),
			SUM(length),
			SUM(views),
			SUM(rates),
			SUM(favorite_count),
			AVG(rating),
			AVG(views),
			AVG(length),
			AVG(favorite_count)
			FROM %s
			
			""" % (db.TABLE_NAME)).fetchone()
		
		users, avg_videos_watched, max_videos_watched = db.conn.execute(
		"""
			SELECT
			COUNT(*),
			AVG(videos_watched),
			MAX(videos_watched)
			FROM %s 
			""" % (db.USER_TABLE_NAME)).fetchone()
		
		total_deleted = total_videos - total_exist
		total_hours = total_length / 3600.
	
		e = html.xpath("//div[@id='mainContent']")[0]
		e.extend([
			E.DIV(E.BIG("Of ", 
				E.BIG(E.STRONG("%d" % total_videos)),
				 " videos ",
				"on YouTube, there are ", E.BIG(E.STRONG("%d" % total_views)), " views ",
				"and ", E.BIG(E.STRONG("%d" % total_hours)), " hours of content")),
			E.P(
				"Aproximately %.1f%% of YouTube videos has been crawled. " % (total_videos / 140000000. * 100),
				u"At this rate, it’s going to take %.1f months for me to complete the crawl." % 
					((140000000 - total_videos) / (10.0 * 2629743.83 / 2.0)),
				),
			E.P(
			E.DIV(
				E.DIV(u"​",
					style="width:%f%%;background:#ffa500;border:1px outset" % (total_videos / 140000000. * 100)),
				style="background:#bfbfbf;border:1px inset;;width:90%;margin:auto;")),
			E.DIV("Statistics breakdown:",
				E.TABLE(
					E.TR(E.TD("Last updated"), 
						E.TD(time.strftime("%Y-%m-%d %H:%M:%S UTC", 
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
					E.TR(E.TD("Deleted"), 
						E.TD("%d" % total_deleted, {"class":"tableNumber"})),
										
					E.TR(E.TD("Users"), 
						E.TD("%d" % users, {"class":"tableNumber"})),
					E.TR(E.TD("Average rating per video"), 
						E.TD("%.2f" % avg_rating, {"class":"tableNumber"})),
					E.TR(E.TD("Average views per video"), 
						E.TD("%.2f" % avg_views, {"class":"tableNumber"})),
					E.TR(E.TD("Average seconds per video"), 
						E.TD("%.2f" % avg_length, {"class":"tableNumber"})),
					E.TR(E.TD("Average minutes per video"), 
						E.TD("%.2f" % (avg_length / 60.), {"class":"tableNumber"})),
					E.TR(E.TD("Average favourites per video"), 
						E.TD("%.2f" % avg_favs, {"class":"tableNumber"})),
					E.TR(E.TD("Average videos watched per user"), 
						E.TD("%.2f" % avg_videos_watched, {"class":"tableNumber"})),
					E.TR(E.TD("Maximum videos watched for a user"), 
						E.TD("%d" % max_videos_watched, {"class":"tableNumber"})),
					border="1",
				)),
		#		E.DIV("Videos longer than 24 hours excluded"),
				
			]
			)
		#e = html.xpath("//head")[0]
		#e.append(E.META({"http-equiv":"refresh", "content":"20"}))
	
	else:
		e = html.xpath("//div[@id='mainContent']")[0]
		e.extend([
			E.A("Generate report", href="?gen")
			])
	
def num_format(num):
	pass
