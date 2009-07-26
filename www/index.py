#!/usr/bin/env python

"""CGI HTML wrapper to handle error display"""

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

import cgi
import cgitb
cgitb.enable()

import sys
sys.path.append("../crawler")
import main

def run(debug=False):
	form = cgi.FieldStorage(keep_blank_values=True)
	if debug or "debug" in form:
		print "Status: 200 OK"
		print "Content-Type: text/plain"
		print
		print cgi.FieldStorage(keep_blank_values=True)
		cgi.print_environ_usage()
		cgi.print_environ()
	try:
		main.run()
	except:
		print "Status: 500 Internal server error"
		print "Content-Type: text/html"
		print 
		print "<html><body><big><strong>500 Internal server error</strong></big>"
		cgitb.handler()
		print "</body></html>"

if __name__ == "__main__":
	run()

