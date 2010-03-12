#!/usr/bin/env python
# encoding=utf-8

"""Generate static html info pages"""

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

import sys
import os
import optparse

import lxml.html.builder
E = lxml.html.builder

# import database

page_size = 10000
dest_dir = "cache/"

def run():
	cwd = os.getcwd()
	os.chdir("../crawler")
	sys.path.append("../crawler")
	import database
	db = database.Database()
	os.chdir(cwd)
	
	optparser = optparse.OptionParser()
	optparser.add_option("-e", dest="offset", help="Offset", default=0)
	optparser.add_option("-t", dest="type", help="type", default="v")
	options, args = optparser.parse_args()
	
	offset = options.offset
	db_type = options.type
	
	page = offset / page_size
	vid_i = offset
	
	
	while True:
		print "Building page %d (video number %d)" % (page, vid_i)
		
		prev_page = max(page - 1, 0)
		next_page = page + 1
		if db_type == "v":
			page_prefix = "video"
		else:
			page_prefix = "user"
		
		listing_div = E.DIV()
		html = E.HTML(
			E.HEAD(
				E.TITLE(u"YouTube Data API Crawl Info – Page %d" % page),
			),
			E.BODY(
				E.DIV(
					E.DIV(
						E.A(str(prev_page), href="%s.%s.html" % (page_prefix, prev_page)),
						" ",
						E.A(str(next_page), href="%s.%s.html" % (page_prefix, next_page)),
					),
					listing_div
				),
			),
		)
	
		if db_type == "v":
			for row in db.conn.execute("""SELECT 
				id, 
				title, 
				views, 
				rating, 
				date_published,
				length, 
				favorite_count FROM %s LIMIT %d OFFSET %d
			""" % (db.TABLE_NAME, page_size, vid_i)):
		
				v_id, title, views, rating, date_published, length, favorite_count = row
				if title is None:
					title = " "
				if views is None:
					views = -1
				if rating is None:
					rating = -1
				if length is None:
					length = -1
				if favorite_count is None:
					favorite_count = -1
			
	#			print vid_i, v_id,
	#			sys.stdout.flush()
			
				listing_div.append(
					E.DIV(
	#					str(vid_i),
	#					". ",
						E.A(title,
							href="http://youtu.be/%s" % v_id),
	#					" ",
	#					"%d/%f/%d %s/%d" % (views, rating, 
	#						favorite_count, date_published, length),
					)
				)
			
				vid_i += 1
		
		else:
			for row in db.conn.execute("""SELECT 
				username
				favorite_count FROM %s ORDER BY LOWER(username) LIMIT %d OFFSET %d
			""" % (db.USER_TABLE_NAME, page_size, vid_i)):
						
				listing_div.append(
					E.DIV(
						str(vid_i),
						". ",
						E.A(row[0],
							href="http://youtube.com/%s" % row[0]),
					)
				)
			
				vid_i += 1
		
		print "\t Writing ... ",
		sys.stdout.flush()
		
		if db_type == "v":
			fname = dest_dir + "video.%d.html" % page
		else:
			fname = dest_dir + "user.%d.html" % page
		
		f = open(fname, "w")
		f.write(lxml.html.tostring(html))
		f.close()
		page += 1
		
		print "OK"
		


if __name__ == "__main__":
	run()
