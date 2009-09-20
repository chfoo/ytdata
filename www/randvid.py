#!/usr/bin/env python

"""Pick random YouTube video id"""

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
import cgi
import cgitb
cgitb.enable()
import random
import glob
#import gzip
import bz2
FILE = "cache/video_ids.%08d.bz2"
FILE_GLOB = "cache/video_ids.*.bz2"
LINES = 100000
def get_random_id():
	
	filename = random.choice(glob.glob(FILE_GLOB))
	f = bz2.BZ2File(filename)
	
	# http://code.activestate.com/recipes/59865/
	line_num = 0
	it = ""

	while True:
		a_line = f.readline()
		line_num = line_num + 1
		if a_line != "":
			if random.uniform(0, line_num)<1:
				it = a_line
				global rand_pick_num
				rand_pick_num = int(filename.split(".")[-2]) * LINES + line_num
		else:
			break
	f.close()
	return it


if __name__ == "__main__":
	
	if len(sys.argv) > 1 and sys.argv[1] == "dump":
		cwd = os.getcwd()
		os.chdir("../crawler")
		sys.path.append("../crawler")
		import database
		db = database.Database()
		os.chdir(cwd)
		
		for name in glob.glob(FILE_GLOB):
			os.remove(name)
		
		k = 0
		f = bz2.BZ2File(FILE % k, "w")
		i = 0
		for row in db.conn.execute("SELECT id, title FROM %s" % db.TABLE_NAME):
			f.write(row[0])
			f.write(" ")
			if row[1]:
				f.write(row[1].encode("utf-8"))
			f.write("\n")
			i += 1
			if i >= LINES:
				i = 0
				k += 1
				f.close()
				f = bz2.BZ2File(FILE % k, "w")
				
		f.close()
		db.close()
	
	
	form = cgi.FieldStorage(keep_blank_values=True)

	try:
		id, title = get_random_id().split(" ", 1)
		
		if form.has_key("watch") or form.has_key("preview"):
			if form.has_key("watch"):
				print "Status: 303 See other"
				print "Location: http://youtube.com/watch?v=%s" % id
			else:
				print "Status: 200 OK"
			print "Content-Type: text/html; charset=utf-8"
			print
			print "<html><head><title>%s (%s  %s) </title>" % (title, id, rand_pick_num)
			print """<style>body{font-family:sans-serif;
					text-align:center</style></head>"""
			print """<body><h4>%s</h4>""" % (title)
			print """<a href="http://youtube.com/watch?v=%s">""" % id
			print """<img src="http://i.ytimg.com/vi/%s/0.jpg" /><br/> """ % id
			print """<img src="http://i.ytimg.com/vi/%s/1.jpg" />""" %id
			print """<img src="http://i.ytimg.com/vi/%s/2.jpg" />""" %id
			print """<img src="http://i.ytimg.com/vi/%s/3.jpg" />""" %id
			print """</a>"""
			print """<br/><br/>http://youtube.com/watch?v=%s """ % id
			print """<br/><br/><a href="./">[ ./ ]</a>"""
			print """</body></html>"""
		else:
			print "Status: 200 OK"
			print "Content-Type: text/plain"
			print
			print id
		
	except:
		print "Status: 500 Internal server error"
		print "Content-Type: text/html"
		print 
		print "<html><body><big>Internal server error</big><hr/>"
		cgitb.handler()
		print "</body></html>"
