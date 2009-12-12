#!/usr/bin/env python
# encoding=utf-8
"""Browse user names"""

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
import subprocess
FILE = "cache/usernames.%08d.7z"
FILE_GLOB = "cache/usernames.*.7z"
LINES = 200000
LINES_PER_PAGE = 1000
import browse
browse.MAGNITUDE = 100

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
			skip_k = int(sys.argv[2])
		else:
			skip_k = 0
		k = 0
		
		if not skip_k:
			p = subprocess.Popen(["7za", "a", "-si", FILE % k],
				stdin=subprocess.PIPE)
			f = p.stdin
			
		i = 0
		for row in db.conn.execute("SELECT username FROM %s ORDER BY LOWER(username)" % db.USER_TABLE_NAME):
			if k >= skip_k:
				f.write(row[0].encode("utf-8"))
				f.write("\n")
				
			i += 1
			
			if i >= LINES:
				i = 0
				k += 1
				
				if k > skip_k:
					p.communicate()
				
				if k >= skip_k:
					p = subprocess.Popen(["7za", "a", "-si", FILE % k],
						stdin=subprocess.PIPE)
					f = p.stdin
				
		db.close()
		p.communicate()
		
		sys.exit()
	
	form = cgi.FieldStorage(keep_blank_values=True)

	try:
		
		page = int(form.getfirst("page", 0))
		i = page * LINES_PER_PAGE / LINES
		l = glob.glob(FILE_GLOB)
		l.sort()
		if page < 0 or i > len(l) - 1:
			print "Status: 404 Not found"
			print "Content-type: text/html"
			print
			print "<html><body><big><strong>404 Not found</big></strong>"
			print "<br/>Page index out of bounds"
			print """<br/><a href="?">Click here to go to first page</a>"""
			print "</body></html>"
		
		if os.path.exists(os.path.expanduser("~/bin/7za")):
			executable = os.path.expanduser("~/bin/7za")
		else:
			executable = None
		
		filename = l[i]
		p = subprocess.Popen(["7za", "x", "-so", filename],
			stdout=subprocess.PIPE, stderr=subprocess.PIPE, 
			executable=executable)
		f = p.stdout
		
		print "Status: 200 OK"
		print "Content-type: text/html; charset=utf-8"
		print
		
#		print out
		print "<html><head><title>YouTube Data API Crawl Usernames - Page %d (%s) </title>" % (page, filename)
		print """<style>body{font-family:sans-serif;}
				.num{font-family:monospace;font-size:small;}
				.username{font-family:monospace;}
				</style></head>"""
		print "<body>"
		
		browse.print_pager(page, len(l) * LINES / LINES_PER_PAGE, form)
		
		sys.stdout.flush()
#		if not form.has_key("thumb"):
#			print """<br/><form method="get" action="?">
#				<input type="hidden" name="page" value="%d"/>
#				<input type="hidden" name="thumb" />
#				<input type="submit" value="Turn on thumbnail image"/></form>""" % page
#		else:
#			print """<br/>Thumbnail image is on. 
#			<form method="get" action="?">
#				<input type="hidden" name="page" value="%d"/>
#				<input type="submit" value="Turn off"/></form>
#			<br/><br/>""" % page
			
		start_line = page * LINES_PER_PAGE % LINES
		end_line = start_line + LINES_PER_PAGE
		n = 1
		while True:
			line = f.readline()
			if line == "":
				print "<br/><small>EOF</small>"
				break
			
			if n <= start_line:
				n += 1
				continue
			elif n > end_line:
				break
			
			print """<span class="num" >%d.</span> """ % (n + i * LINES)
			print """<a href="http://www.youtube.com/user/%s">""" % line
			print """<span class="username">"""
			print line
			print """</span>"""
			print """</a>"""
			try:
				line.decode("ascii")
			except UnicodeDecodeError:
				print """ <strong>Unicode</strong> """ 
				print u"""This username isn’t ASCII! """.encode("utf-8")
				print "It is <em>so</em> not alphanumeric! "
				print """There is a glitch in the Matrix! Only """
				print line
				print """ can save us now!"""
			print "<br/>"
			n += 1
		
			if n % 128 == 0:
				sys.stdout.flush()
			
		browse.print_pager(page, len(l) * LINES / LINES_PER_PAGE, form)		
		
		print "<code><small>"
		print p.communicate()[1].replace("\n", "<br/>").replace(" ", "&nbsp;")
		print "</small></code>"
		
		print """</body></html>"""
		
	except:
		print "Status: 500 Internal server error"
		print "Content-Type: text/html; charset=utf-8"
		print 
		print "<html><body><big>Internal server error</big><hr/>"
		cgitb.handler()
		print "</body></html>"
	
