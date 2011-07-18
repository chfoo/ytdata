#!/usr/bin/env python

import sqlite3
import base64

import database

def run():
	db_ = database.Database()
	src_conn = sqlite3.connect(db_.DB_FILE)
	dest_file = open('./data/ytdata_basic.bin', 'wb')
#	dest_conn = sqlite3.connect('./data/ytdata_basic.db')
#	dest_conn.execute('CREATE TABLE IF NOT EXISTS vid_table_basic ( id BLOB UNIQUE )')
#	dest_conn.execute('CREATE TABLE IF NOT EXISTS user_table_basic ( user TEXT UNIQUE )')
	
	i = 0
	for row in src_conn.execute("""select id from vidtable1"""):
		if i % 10000 == 0:
			print i
	
		s = buffer(base64.urlsafe_b64decode('%s=' % str(row[0])))
#		print row[0], hex(s)
#		dest_conn.execute("INSERT INTO vid_table_basic VALUES (?)",
#			(s, )
#		 )
		dest_file.write(s)
		
		i += 1
	
#	dest_conn.commit()


if __name__ == "__main__":
	run()
