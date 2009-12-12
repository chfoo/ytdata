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

NUM_VIDS = float(238134528) + 555555 * (time.time() - 1260624032)
CRAWL_RATE = 6.0

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
				"Aproximately %.1f%% of YouTube videos has been crawled. " % (total_videos / NUM_VIDS * 100),
				u"At this rate, it’s going to take %.1f months for me to complete the crawl." % 
					((NUM_VIDS - total_videos) / (CRAWL_RATE * 2629743.83 / 2.0)),
				),
			E.P(
			E.DIV(
				E.DIV(u"​",
					style="width:%f%%;background:#ffa500;border:1px outset" % (total_videos / NUM_VIDS * 100)),
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
		
			E.BR(),
			E.UL(
				E.LI("""On 2009 December 12 13:15, there are 238134528 videos
				"""),
				E.LI("""On 2009 December 3 14:00, there are 233178928 videos
				(from the related videos glitch).
				"""),
				E.LI("""On 2009 November 14/15, videos watched field was 
				removed for user channels on the main site. A few Google forum
				 posts by YouTubers 
				 states that statistics were 
				reset for some users. However, the stats are still available
				on the API. (""", 
					E.A("ref", href="http://www.google.com/support/forum/p/youtube/thread?tid=7ee68d76002010a9"), """)"""),
				E.LI("""Deleted videos and users are obtained through playlists 
				and favourites. Deleted videos are counted as not having a
				length. Users are also additionally obtained though subscribers
				scraped from the main website"""),
				E.LI("""Maximum videos watched is especially not accurate. A
				 specific user
				has their stats rising about 1 million videos per month (a
				rough estimate)"""),
				E.LI("""Currently unable to retrieve information about users
				who have do not have an alphanumeric/ASCII username. (How
				this is possible, I don't know..)"""),
				E.LI("""Length is especially not accurate. In one instance, a
				 video's 
				metadata has duration as 1 year, however, the actual video is
				only a few minutes. (The Flash player says an error occurred
				when it cannot read more data). As well, there are also
				scrambled videos that users decide not to delete. (The videos
				are like static on an analog television except with coloured 
				dots.)
				"""),
				E.LI("""Records are not updated once information has been 
				retrieved for them. So, the stats are not accurate"""),
				E.LI("""There is a blog post about the number of vids on 
				YouTube. The old API exposed that there were about 144 million
				videos. Related videos, on the main website for a recently 
				uploaded video where related videos were not generated, once
				had a count of 138 million videos during the start of this
				project. Google Search also 
				once exposed the number of videos. I do not remember the
				count; I probably mentioned in some instant message 
				conversation once and should be logged somewhere."""),
			),
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
