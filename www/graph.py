"""Generate a directed graph of the data"""

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

import os
import sys
sys.path.append("../crawler/")
import sqlite3
import random

import yapgvb # use too much memory
#import pydot # slow

import database

DEST_FILE = "cache/graph.png"
DOT_FILE = "cache/graph.dot"
def generate_graph():
	cwd = os.getcwd()
	os.chdir("../crawler")
	db = database.Database()
	os.chdir(cwd)
	
	graph = yapgvb.Digraph("YouTube Videos")
#	graph.overlap = "false"
	#graph.fontsize = 8.0
	#graph = pydot.Graph(graph_name="YouTube Videos", graph_type="digraph")
	node_table = {}
	title_table = {}
	
#	def get_node(name):
#		if name in node_table:
#			return node_table[name]
#		else:
#			node = pydot.Node(name)
#			node_table[name] = node
#			return node
	
#	f = open(DOT_FILE, "w")
#	f.write("digraph G {\n")
	
	cursor = db.conn.execute("SELECT id, referred_by, title FROM %s" % db.TABLE_NAME)
	rows = cursor.fetchall()
	counter = 0
#	while True: #counter < 1000:
	print "Loop.."
	for row in rows:
		counter += 1
#		row = cursor.fetchone()
		if row is None:
			break
		
		v_id = row[0]#.encode("utf-8")
		r_id = row[1]
		title = row[2]
#		if r_id:
#			r_id = r_id.encode("utf-8")
		
		if counter % 10000 == 0:
			print counter, v_id, r_id
		#print v_id, r_id
		
		#node = graph.add_node(v_id)
		#node = graph.add_node(get_node(v_id))
		#node.fontsize = 8
		
#		if r_id:
			#r_node = graph.add_node(r_id)
			#r_node = get_node(r_id)
			#edge = pydot.Edge(node, r_node)
			#graph.add_edge(edge)
			#graph.add_edge(node, r_node)
#			f.write("\t\"%s\" -> \"%s\";\n" % (r_id, v_id)) 
#		else:
#			f.write("\t\"%s\";\n" % v_id)
		
		node_table[v_id] = r_id
#		if title:
#			title = title.encode("utf-8")
		title_table[v_id] = title
	
	def get_title(video_id):
		t = title_table[video_id]
		return u"%s\n%s".encode("utf-8") % (video_id, t)
		
	
	def traverse_children(parent_id, parent_node, depth):
		if depth > 0:
			for child_id in get_children(parent_id):
				print child_id
				label = title_table[child_id]
				child_node = graph.add_node(child_id.encode("utf-8"), label=get_title(child_id))
				graph.add_edge(parent_node, child_node)
				traverse_children(child_id, child_node, (depth - 1))
	
	def get_children(video_id):
		l = []
		for pair in node_table.iteritems():
			key, value = pair
			if value == video_id:
				l.append(key)
		return l
	
	target_id = random.choice(node_table.keys())
	node = graph.add_node(target_id.encode("utf-8"), label=get_title(target_id))
	node.shape = "circle"
	#graph.add_edge(parent_node, node)
	
	traverse_children(node_table[target_id], node, 2)
	
	
	
	db.close()
	
	

	#f.write("}\n")
	#f.close()
	
	graph.write(DOT_FILE)
	
	graph.layout(yapgvb.engines.neato)
	
	graph.render(DEST_FILE)
	
	

if __name__ == "__main__":
	generate_graph()
