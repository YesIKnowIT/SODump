A tool to extract historical data from the StackOverflow archive
maintained by the Internat Archive Wayback Machine at 
https://web.archive.org/web/*/stackoverflow.com

Disclaimer
==========
This project is basically a big hack that started as a simple script using
`wget`. I don't make any guarantees whatsoever either regarding the program
work, or the data provided here.

Use at your own risks.
Read the LICENCE file.


Requirements
============
This requires Python 3.5 with Request, BeautifulSoup and Sqlite3 installed

It was tested on Linux Debian. The tool makes extensive use of the standard
Python multiprocessing library, so depending on you OS it may--or may not--work.

How to run
==========
python3.5 master.py

The `master.py` file spawns a couple of processes to:

* download the cached file list from the Internet Archive CDX server,
* download the individual files from the WaybackMachine, 
* parse them with BieautifulSoup to gather data
* and store them in SQLite database

Which data?
===========
StackOverflow, Inc openly provides several ways to access their raw DB data.
Either interactively through SEDE, or by downloading their quarterly DB dump.

But there is one missing data: the page views. This tool was designed to extract
the page view count from cached data on the Internet Archive.

Currently, the tool gathers the retrieval data, view count, question id, and associated tags for all cached StackOverflow web pages.

A dump of the DB as CSV is updated form my home server in the `data` directory of this repository.
