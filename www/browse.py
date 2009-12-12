#!/usr/bin/env python

"""Browse YouTube video listings"""

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
import glob
#import gzip
#import bz2
#import cStringIO as StringIO
import subprocess
#FILE_GLOB = "cache/video_ids.*.bz2"
FILE_GLOB = "cache/video_ids.*.7z"
LINES = 200000
LINES_PER_PAGE = 1000
MAGNITUDE = 1000



def print_pager(page, max_page, form=None):
	if form and form.has_key("thumb"):
		url_append_str = """&amp;thumb"""
	else:
		url_append_str = ""

	print "<br/><br/>"
	
	for p in range(0, page - 5, MAGNITUDE) + range(max(0, page - 5), page):
		print """<a href="?page=%d%s">%d</a>  """ % (p, url_append_str, p)
	
	print """ <strong>%d</strong> """ % page
	
	for p in range(page + 1, page + 5) + range(page + 5, max_page, MAGNITUDE):
		print """<a href="?page=%d%s">%d</a>  """ % (p, url_append_str, p)
	
	print "<br/><br/>"

if __name__ == "__main__":
	
	
	form = cgi.FieldStorage(keep_blank_values=True)

	try:
		
		page = int(form.getfirst("page", 0))
		is_bot = "bot" in form
		
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
#		f = bz2.BZ2File(filename)
		p = subprocess.Popen(["7za", "x", "-so", filename],
			stdout=subprocess.PIPE, stderr=subprocess.PIPE, 
			executable=executable)
		f = p.stdout
		
		print "Status: 200 OK"
		print "Content-type: text/html; charset=utf-8"
		print
		
#		print out
		if not is_bot:
			print "<html><head><title>YouTube Data API Crawl Browse - Page %d (%s) </title>" % (page, filename)
			print """<style>body{font-family:sans-serif;}
					.num,.vidId{font-family:monospace;font-size:small;}
					</style></head>"""
			print "<body>"
		
			print_pager(page, len(l) * LINES / LINES_PER_PAGE, form)
		
			sys.stdout.flush()
			if not form.has_key("thumb"):
				print """<br/><form method="get" action="?">
					<input type="hidden" name="page" value="%d"/>
					<input type="hidden" name="thumb" />
					<input type="submit" value="Turn on thumbnail image"/></form>""" % page
			else:
				print """<br/>Thumbnail image is on. 
				<form method="get" action="?">
					<input type="hidden" name="page" value="%d"/>
					<input type="submit" value="Turn off"/></form>
				<br/><br/>""" % page
		
		else:
			print """%s([""" % form.getfirst("callback", "callback")
			
		start_line = page * LINES_PER_PAGE % LINES
		end_line = start_line + LINES_PER_PAGE
		n = 1
		while True:
			line = f.readline()
			if line == "" and not is_bot:
				print "<br/><small>EOF</small>"
				break
			
			if n <= start_line:
				n += 1
				continue
			elif n > end_line:
				break
			id = line[:11]
			title = line[11:]
			
			if not is_bot:
				print """<span class="num" >%d.</span> """ % (n + i * LINES)
				if form.has_key("thumb"):
					print """<img src="http://i.ytimg.com/vi/%s/1.jpg" />""" %id
				print """<span class="vidId"> %s </span>""" % id
				print """<a href="http://youtube.com/watch?v=%s">""" % id
		#		print """<img src="http://i.ytimg.com/vi/%s/0.jpg" /><br/> """ % id
		#		print """<img src="http://i.ytimg.com/vi/%s/2.jpg" />""" %id
		#		print """<img src="http://i.ytimg.com/vi/%s/3.jpg" />""" %id
				print title
				print """</a><br/>"""
	#			print """<br/><br/>http://youtube.com/watch?v=%s """ % id
			else:
				print """\"%s\",""" % id
			
			n += 1
			
			if n % 128 == 0:
				sys.stdout.flush()
			
			
#		f.close()
		
		if not is_bot:
			print_pager(page, len(l) * LINES / LINES_PER_PAGE, form)		
		
			print "<code><small>"
			print p.communicate()[1].replace("\n", "<br/>").replace(" ", "&nbsp;")
			print "</small></code>"
		
			print """</body></html>"""
		else:
			print """null])"""
		
	except:
		print "Status: 500 Internal server error"
		print "Content-Type: text/html; charset=utf-8"
		print 
		print "<html><body><big>Internal server error</big><hr/>"
		cgitb.handler()
		print "</body></html>"
