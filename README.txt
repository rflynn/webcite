
Setup:
	make database

Example use:

	./crawl.py --verbose --max-urls=50 http://example.com/
	make graph-links

Website toolkit for quantifying website things, diagnosing various issues

	Check for...
		broken links
		common files that should exist
			favicon.ico
		sloppy files that shouldn't exist
			version control files

