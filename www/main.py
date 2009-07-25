"""Build webpage by dispatching to appropriate modules"""

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

import time
import lxml.html
from lxml.html import builder as E

import stats

def run():
	start_time = time.time()
	html = E.HTML(
		E.HEAD(
			E.TITLE("YouTube API Data Crawl"),
			E.LINK({"rel":"stylesheet", "type":"text/css", "href":"static/style.css"}),
			),
		E.BODY(
			E.DIV(
				E.DIV("YouTube API Data Crawl Statistics", id="banner"),
				E.UL(
					E.LI(E.A("Home", href=".")),
					E.LI(E.A("Downloads", href="./cache/")),
					E.LI(E.A("Watch Random YouTube Video", href="randvid.cgi?watch")),
					{"class":"navbar"},
				),
				id="header"), 
			E.DIV(id="mainContent"), 
			E.DIV("Page generated on ", time.strftime("%Y-%m-%d %H:%M:%S %Z"),
			" in %.3f seconds" % (time.time() - start_time), id="footer"))
		)
	
	stats.html(html)
	
	print "Status: 200 OK"
	print "Content-Type: text/html"
	print
	print lxml.html.tostring(html, pretty_print=True, encoding="utf-8")
	
	
