#!/usr/bin/env python

"""Play YouTube videos"""

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

if __name__ == "__main__":
	
	
	form = cgi.FieldStorage(keep_blank_values=True)

	try:
		print "Status: 200 OK"
		print "Content-Type: text/html; charset=utf-8"
		print
		print """<html><head><title>Play all YouTube videos<title>"""
		print """<script type="text/javascript" src="static/jquery.min.js"></script>"""
		print """<script type="text/javascript" src="static/swfobject.js"></script>"""
		print """<script type="text/javascript" src="static/play.js"></script>"""
		print """</head>"""
		print """<body>"""
		print """<div id="mainContent"><noscript>JavaScript is needed</noscript></div>"""
		print """</body></html>"""
	
	except:
		print "Status: 500 Internal server error"
		print "Content-Type: text/html; charset=utf-8"
		print 
		print "<html><body><big>Internal server error</big><hr/>"
		cgitb.handler()
		print "</body></html>"
