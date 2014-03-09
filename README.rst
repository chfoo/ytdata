ytdata
======

Historical 2009-2010 YouTube API metadata statistics crawler.

Here lies some very old Python 2.5 code that crawled the YouTube API.
It crawled and saved these stats:

* id TEXT UNIQUE,
* views NUMBER
* rating NUMBER
* rates NUMBER
* date_published NUMBER
* length NUMBER
* referred_by TEXT
* title TEXT
* favorite_count NUMBER
* username TEXT PRIMARY KEY
* videos_watched NUMBER


Report
------

Of 105527188 videos on YouTube, there are 696902604315 views and
6538953 hours of content.

=================================== =======================
Key                                 Value
=================================== =======================
Last updated                        2010-08-10 01:54:41 UTC
Database size (bytes)               11958804480
Videos                              105527188
Views                               696902604315
Length (seconds)                    23540233514
Length (years)                      745.96
Rates                               1610011351
Favourites                          1179844974
Deleted                             4591502
Users                               10478210
Average rating per video            4.55
Average views per video             7018.90
Average seconds per video           233.22
Average minutes per video           3.89
Average favourites per video        12.35
Average videos watched per user     3239.53
Maximum videos watched for a user   6043637
=================================== =======================


Dataset
-------

The dataset is available as a SQLite database dump file at: https://archive.org/details/YouTubeCrawlSurveyDataset2009-2010 .

