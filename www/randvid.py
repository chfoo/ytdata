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

FILE = "cache/video_ids.txt"

def get_random_id():
	f = open(FILE)
	
	# http://code.activestate.com/recipes/59865/
	line_num = 0
	it = ""

	while True:
		a_line = f.readline()
		line_num = line_num + 1
		if a_line != "":
			if random.uniform(0, line_num)<1:
				it = a_line
		else:
			break
	return it


if __name__ == "__main__":
	
	if len(sys.argv) > 1 and sys.argv[1] == "dump":
		cwd = os.getcwd()
		os.chdir("../crawler")
		sys.path.append("../crawler")
		import database
		db = database.Database()
		os.chdir(cwd)
		f = open(FILE, "w")
		for row in db.conn.execute("SELECT id FROM %s" % db.TABLE_NAME):
			f.write(row[0])
			f.write("\n")
		f.close()
		db.close()
	
	
	form = cgi.FieldStorage(keep_blank_values=True)

	try:
		id = get_random_id()
		
		if form.has_key("watch"):
			print "Status: 303 See other"
			print "Location: http://youtube.com/watch?v=%s" % id
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
