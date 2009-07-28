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
#DOT_FILE = "cache/graph.dot"
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
	
#	def get_node(name):
#		if name in node_table:
#			return node_table[name]
#		else:
#			node = pydot.Node(name)
#			node_table[name] = node
#			return node
	
#	f = open(DOT_FILE, "w")
#	f.write("digraph G {\n")
	
	cursor = db.conn.execute("SELECT id, referred_by FROM %s" % db.TABLE_NAME)
	rows = cursor.fetchall()
	counter = 0
#	while True: #counter < 1000:
	print "Loop.."
	for row in rows:
		counter += 1
#		row = cursor.fetchone()
		if row is None:
			break
		
		v_id = row[0].encode("utf-8")
		r_id = row[1]
		if r_id:
			r_id = r_id.encode("utf-8")
		
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
	
	target_id = random.choice(node_table.keys())
	parent_id = node_table[target_id]
	
	parent_node = graph.add_node(parent_id, label=parent_id)
	node = graph.add_node(target_id, label=target_id)
	node.shape = "circle"
	#graph.add_edge(parent_node, node)
	
	print "target:", target_id, "parent:", node_table[target_id]
	
	vids_used = [target_id]
	for key in node_table.keys():
		value = node_table[key]
		
		if value == target_id:
			print "child:", value
			child_node = graph.add_node(value, label=value)
			graph.add_edge(node, child_node)
		elif value == parent_id:
			print "sibling:", key
			sibling_node = graph.add_node(key, label=key)
			graph.add_edge(parent_node, sibling_node)
			
	
	db.close()
	
	

	#f.write("}\n")
	#f.close()
	
	graph.layout(yapgvb.engines.neato)
	
	graph.render(DEST_FILE)
	
	#graph.write("cache/graph.dot")
	

if __name__ == "__main__":
	generate_graph()
