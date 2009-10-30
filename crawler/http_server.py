"""Access Crawler interface"""

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

import threading
import BaseHTTPServer
import time
import cgi
import cgitb
import logging
cgitb.enable()
import urlparse

class Server(threading.Thread):
	
	
	def __init__(self):
		threading.Thread.__init__(self)
		self.setDaemon(True)
	
	def run(self):
		port = 18093
		logging.info("HTTP server starting...")
		http_server = BaseHTTPServer.HTTPServer(("", port), HTTPRequestHandler)
		logging.info("\tPort %d" % port)
		http_server.crawler = self.crawler
		http_server.serve_forever()

#		while True:
#			http_server.handle_request()
#			time.sleep(5)
			
					
class HTTPRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
#	def do_POST(self):
#		self.do_GET()

	def do_GET(self):
		try:
			query = cgi.parse_qs(urlparse.urlparse(self.path).query)
		
			self.send_response(200, "OK")
			self.send_header("content-type", "text/html")
			self.end_headers()
			self.wfile.write("""<html><body>""")
			self.wfile.write(self.__dict__)
#			self.wfile.write(form.__dict__)
			r = self.server.crawler.vids_crawled_session / (time.time() - self.server.crawler.start_time)
			self.wfile.write("""%f videos per second; %d this session""" %
				(r, self.server.crawler.vids_crawled_session))
			self.wfile.write("""<br/>""")
			self.wfile.write("""<form action="?" method="get">
				<input type="text" name="insert" />
				<input type="submit" value="Insert"/>
				</form>""")
		
			if "insert" in query:
				i = query["insert"][0]
				if i.startswith("http://"):
					self.wfile.write("Adding feed %s" % i)
					self.server.crawler.add_uri_to_crawl(i)
				else:
					self.wfile.write("Adding video %s" % i)
					self.server.crawler.add_uri_to_crawl(None, video_id=i)
		
			self.wfile.write("""</body></html>""")
	
		except:
			logging.exception("HTTP request error")
			self.send_response(500, "Error")
			self.end_headers()
			self.wfile.write(cgitb.handler)
		
if __name__  == "__main__":
	server = Server()
	server.run()
	
	
	
