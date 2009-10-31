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
import time

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
		self.connections.append(conn)
		conn.connect()
		logging.debug("\tOK")
	
	def request(self, method, url, data=None, headers={}):
		while True:
			if len(self.connections) < self.NUM_CONNECTIONS:
				self.init_connection()
				self.i = 0
			
			if self.i >= self.NUM_CONNECTIONS:
				self.i = 0
			
			i = self.i
			connection = self.connections[i]
			if connection not in self.in_use or \
			not self.in_use[connection]:
				try:
#					headers["Accept-encoding"] = "gzip"
#					
#					headers["User-Agent"] = headers.get("User-Agent", "") + \
#						"chfoo-crawler (gzip, http://www.student.cs.uwaterloo.ca/~chfoo/ytdata/)"
					
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
		while time.time() - r2_time < 60:
			try:
				response = connection.getresponse()
				logging.debug("\tGot response")
				self.in_use[connection] = False
#				logging.debug(response.getheaders())
				if response.getheader("Content-Encoding", None) == "gzip":
					logging.debug("\tGzip encoding response")
					string_buf = StringIO.StringIO(response.read())
					g_o = gzip.GzipFile(fileobj=string_buf)
					
					class DummyResponse:
						pass
					dummy_response = DummyResponse()
					dummy_response.file = g_o
					dummy_response.read = lambda b: dummy_response.file.read(b)
					dummy_response.getheader = response.getheader
					dummy_response.getheaders = response.getheaders
					dummy_response.msg = response.msg
					dummy_response.version = response.version
					dummy_response.status = response.status
					dummy_response.reason = response.reason
				
				else:
					return response
			except httplib.ResponseNotReady:
				logging.debug("\tHTTP response not ready")
			
			except httplib.HTTPException:
				# Probably got disconnected
				logging.debug("\tHTTP exception")
				logging.exception("HTTP exception")
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

