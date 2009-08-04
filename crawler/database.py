"""sqlite database handler"""

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

import sqlite3

class Database:
	DB_FILE = "./data/ytdata.db"
	COLUMNS = (
		"id TEXT UNIQUE",
		"views NUMBER",
		"rating NUMBER",
		"rates NUMBER",
		"date_published NUMBER",
		"length NUMBER",
		"referred_by TEXT",
		"title TEXT",
		"favorite_count NUMBER",
		"traversed",)
	TABLE_NAME = "vidtable1"
	USER_COLUMNS = (
		"username TEXT UNIQUE",
		"join_date NUMBER",
		"videos_watched NUMBER",
		"uploaded NUMBER",
		"subscribers NUMBER",
		"subscriptions NUMBER",
		"traversed",
		"favorites NUMBER",
		"views NUMBER",
		)
	USER_TABLE_NAME = "usertable1"
	PLAYLIST_COLUMNS = (
		"id",
		"published",
		"title",
		"traversed",
	)
	PLAYLIST_TABLE_NAME = "playlisttable1"
	
	def __init__(self):
		self.connection = sqlite3.connect(self.DB_FILE)
		self.conn = self.connection
	
	def create_table(self, num="1"):
		self.conn.execute("CREATE TABLE vidtable%s(%s)" % (num, ",".join(self.COLUMNS)))
		self.conn.execute("CREATE TABLE usertable%s(%s)" % (num, ",".join(self.USER_COLUMNS)))
		
		self.conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS video_id ON %s (id)" % self.TABLE_NAME)
		self.conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS user_id ON %s (username)" % self.USER_TABLE_NAME)
		
	
	def close(self):
		self.connection.close()

if __name__ == "__main__":
	db = Database()
	db.create_table()
	db.close()


