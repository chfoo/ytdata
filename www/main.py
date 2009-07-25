
import time
import lxml.html
from lxml.html import builder as E

import stats

def run():
	start_time = time.time()
	html = E.HTML(
		E.HEAD(
			E.TITLE("YouTube API Data Crawl"),
			E.LINK({"rel":"stylesheet", "type":"text/css", "href":"static/style.css"}),
			),
		E.BODY(
			E.DIV(
				E.DIV("YouTube API Data Crawl Statistics", id="banner"),
				E.UL(
					E.LI(E.A("Home", href=".")),
					E.LI(E.A("Downloads", href="./cache/")),
					E.LI(E.A("Watch Random YouTube Video", href="randvid.cgi?watch")),
					{"class":"navbar"},
				),
				id="header"), 
			E.DIV(id="mainContent"), 
			E.DIV("Page generated on ", time.strftime("%Y-%m-%d %H:%M:%S %Z"),
			" in %.3f seconds" % (time.time() - start_time), id="footer"))
		)
	
	stats.html(html)
	
	print "Status: 200 OK"
	print "Content-Type: text/html"
	print
	print lxml.html.tostring(html, pretty_print=True, encoding="utf-8")
	
	
