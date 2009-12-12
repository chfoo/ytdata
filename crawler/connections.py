"""Connection manager and pooler"""

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

import logging
import httplib
#import httplib2 # http://code.google.com/p/httplib2/
import time
import cStringIO as StringIO
import gzip

class HTTPClient:
	"""HTTPClient wrapper"""
	
	NUM_CONNECTIONS = 4
	
	def __init__(self):
		self.connections = []
		self.in_use = {}
		
#		logging.info("Starting up %d connections" % self.NUM_CONNECTIONS)
#		for i in range(self.NUM_CONNECTIONS):
#			self.init_connection()
		
		self.i = 0
	
	def init_connection(self):
		logging.debug("Setup connection...")
		conn = httplib.HTTPConnection("gdata.youtube.com")
#		conn = httplib2.HTTPConnectionWithTimeout("gdata.youtube.com")
		self.connections.append(conn)
		conn.connect()
		logging.debug("\tOK")
	
	def request(self, method, url, data=None, headers={}):
		logging.info("HTTP request %s" % url)		

		start_time = time.time()
		end_time = time.time() + 60 * 5
		while time.time() < end_time:
			if len(self.connections) < self.NUM_CONNECTIONS:
				self.init_connection()
				self.i = 0
			
			if self.i >= len(self.connections):
				self.i = 0
			
			try:
				i = self.i
				connection = self.connections[i]
			except:
				logging.exception("Index error")
				time.sleep(1)
				continue
	
			if connection not in self.in_use or \
			not self.in_use[connection]:
				try:
					headers["Accept-encoding"] = "gzip"
#					
#					headers["User-Agent"] = headers.get("User-Agent", "") + \
					headers["User-Agent"] = " chfoo-crawler (gzip, http://www.student.cs.uwaterloo.ca/~chfoo/ytdata/)"
					
					logging.debug("HTTP request [%d] %s %s %s %s" % (i, method, url, data, headers))
					self.in_use[connection] = True
#					connection.request(method, str(url), data, headers)
					connection.putrequest(method, str(url), 
						skip_accept_encoding=True)
					for key, value in headers.iteritems():
						connection.putheader(key, value)
					connection.endheaders()
					break
			
				except httplib.HTTPException:
					# Probably got disconnected
					logging.debug("\tHTTP exception")
					logging.exception("HTTP exception")
					connection.close()
					if connection in self.in_use:
						del self.in_use[connection] 
					if connection in self.connections:
						self.connections.remove(connection)
			
			logging.debug("\tHTTP Wait 1")
			self.i += 1
			time.sleep(0.01)
		
		r2_time = time.time()
		while time.time() - r2_time < 60 and time.time() < end_time:
			try:
				response = connection.getresponse()
				logging.debug("\tGot response")
				self.in_use[connection] = False
				logging.debug("\t %s" % response.getheaders())
				
				if response.getheader("Content-Encoding", None) == "gzip":
					logging.debug("\tGzip encoding response")
					string_buf = StringIO.StringIO(response.read())
					g_o = StringIO.StringIO(gzip.GzipFile(fileobj=string_buf).read())
					
#					string_buf.close()
					
#					class DummyResponse:
#						def __del__(self):
#							logging.debug("%s __del__" % self)
#						
#					dummy_response = DummyResponse()
#					dummy_response.file = g_o
#					dummy_response.read = g_o.read
#					dummy_response.getheader = response.getheader
#					dummy_response.getheaders = response.getheaders
#					dummy_response.msg = response.msg
#					dummy_response.version = response.version
#					dummy_response.status = response.status
#					dummy_response.reason = response.reason
					
#					return dummy_response
					response.read = g_o.read
					return response
				else:
					return response
			
			except httplib.ResponseNotReady:
				logging.debug("\tHTTP response not ready")
			
			except: #httplib.HTTPException:
				# Probably got disconnected
				logging.debug("\tHTTP exception")
				logging.exception("HTTP exception")
				time.sleep(10)
				break
			
			
			logging.debug("\tHTTP Wait 2")
			time.sleep(0.01)
		
		# Retry request
		connection.close()
		if connection in self.in_use:
			del self.in_use[connection] 
		if connection in self.connections:
			self.connections.remove(connection)
		return self.request(method, url, data, headers)

if __name__ == "__main__":
	
	
	logger = logging.getLogger()
	logger.setLevel(logging.DEBUG)
	
	c = HTTPClient()
	
	response = c.request("get", "http://gdata.youtube.com/feeds/api/users/chfoo0/playlists")
	print response.read()
	
	response = c.request("get", "http://gdata.youtube.com/feeds/api/users/chfoo0")
	print response.read()
	
	
