#!/usr/bin/env python

import cgi
import cgitb
cgitb.enable()

import sys
sys.path.append("../crawler")
import main

def run(debug=False):
	if debug:
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

