#!/usr/bin/env python
# encoding=utf-8
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
#import bz2
import subprocess
import base64
#import tempfile
#FILE = "cache/video_ids.%08d.bz2"
#FILE_GLOB = "cache/video_ids.*.bz2"
#FILE = "cache/video_ids.%08d.7z"
#FILE_GLOB = "cache/video_ids.*.7z"
FILE = "cache/video_ids.%08d.binary"
FILE_GLOB = "cache/video_ids.*.binary"
LINES = 200000


def get_random_id():
	
	l = glob.glob(FILE_GLOB)
	l.sort()
	filename = random.choice(l)
#	f = bz2.BZ2File(filename)
	if os.path.exists(os.path.expanduser("~/bin/7za")):
		executable = os.path.expanduser("~/bin/7za")
	else:
		executable = None
		
#	p = subprocess.Popen(["7za", "x",  "-so", filename],
#		stdout=subprocess.PIPE, executable=executable)
#	f = p.stdout
	f = open(filename, "rb")
	
	if filename == l[-1]:
		# http://code.activestate.com/recipes/59865/
		line_num = 0
		it = ""

		while True:
#			a_line = f.readline()
			a_line = f.read(11)
			line_num = line_num + 1
			if a_line != "":
				if random.uniform(0, line_num)<1:
					it = a_line
					global rand_pick_num
					rand_pick_num = int(filename.split(".")[-2]) * LINES + line_num
			else:
				break
	else:
		rand_num = random.randint(0, LINES)
		line_num = 0
		while line_num < rand_num:
#			a_line = f.readline()
			a_line = f.read(8)
			line_num += 1
		
		global rand_pick_num
		rand_pick_num = int(filename.split(".")[-2]) * LINES + line_num
		it = f.read(8)#f.readline()
		
	f.close()
	return base64.urlsafe_b64encode(it)[:11]


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
		
		if len(sys.argv) >= 3:
#			skip_k = int(sys.argv[2])
			k = int(sys.argv[2])
		else:
#			skip_k = 0
			k = 0
#		k = 0
#		f = bz2.BZ2File(FILE % k, "w")
#		if not skip_k:
#		p = subprocess.Popen(["7za", "a", "-si", FILE % k],
#			stdin=subprocess.PIPE)
#		f = p.stdin
		f = open(FILE % k, "wb")
			
		i = 0
		for row in db.conn.execute("SELECT id, title FROM %s LIMIT -1 OFFSET %d" 
			% (db.TABLE_NAME, k * LINES)):
#		for row in db.conn.execute("SELECT id, title FROM %s ORDER BY id" % db.TABLE_NAME):
#		for row in db.conn.execute("SELECT id FROM %s" % db.TABLE_NAME):
#			if k >= skip_k:
			f.write(base64.urlsafe_b64decode("%s=" % str(row[0])))
#			f.write(" ")
#			if row[1]:
#				f.write(row[1][:8].encode("utf-8"))
#				
#				if len(row[1]) > 16:
#					f.write(u"…".encode("utf-8"))
#				
#				f.write("\n")
				
			i += 1
			
			if i >= LINES:
				i = 0
				k += 1
				
#				if k > skip_k:
#				p.communicate()
				f.close()
				
#				if k >= skip_k:
#				f = bz2.BZ2File(FILE % k, "w")
#				p = subprocess.Popen(["7za", "a", "-si", FILE % k],
#					stdin=subprocess.PIPE)
#				f = p.stdin
				f = open(FILE % k, "wb")
				
		db.close()
#		p.communicate()
#		f.close()
		
	
	
	form = cgi.FieldStorage(keep_blank_values=True)
	id = None
	try:
		
		if form.has_key("watch") or form.has_key("preview"):
			if form.has_key("watch"):
				print "Status: 303 See other"
#				sys.stdout.flush()
				s = get_random_id()
				id = s[:11]
				title = id # s[11:]
#				id = get_random_id()
				print "Location: http://youtube.com/watch?v=%s" % id
			else:
				print "Status: 200 OK"
			print "Content-Type: text/html; charset=utf-8"
			print
			if not id:
				s = get_random_id()
				id = s[:11]
				title = s # s[11:]
#				id = get_random_id()
#				title = id
#			sys.stdout.flush()
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
#			sys.stdout.flush()
			s = get_random_id()
			id = s[:11]
			title = s #s[11:]
			print id
#			print get_random_id()
		
	except:
		print "Status: 500 Internal server error"
		print "Content-Type: text/html; charset=utf-8"
		print 
		print "<html><body><big>Internal server error</big><hr/>"
		cgitb.handler()
		print "</body></html>"
