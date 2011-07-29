#!/usr/bin/env python
# -*- coding:utf-8 -*-
# ex: set ts=4 noet:

"""
Web Crawler/Spider

Finds broken links, produce graphviz visualization
"""

# TODO:
"""
	* check database; if external host pages were checked "recently" and ok, don't bother re-checking
	* move from synchronous to asyncore <http://docs.python.org/library/asyncore.html#asyncore-example-basic-http-client>
	* urllib2 automatically handles 3xx redirects; can i access them?
	* add request/bandwidth rate limiting
	* how to handle anchors?
		* split anchor, fetch base url, ensure anchor exists?
	* perhaps we should record all url anchors when we parse, then we don't need to fetch
	* how to handle query strings?
	* save doctype schema
	* save html ns and lang
		<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-Transitional.dtd">
		<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
	* <object> dependencies...
"""

import re, sys, time, math
import urllib2
import urlparse
import optparse
from cgi import escape
from traceback import format_exc
from Queue import Queue, Empty as QueueEmpty
from BeautifulSoup import BeautifulSoup
import itertools
import sqlite3

__version__ = '0.1'
__copyright__ = 'Copyright 2011 Ryan Flynn'
__license__ = 'MIT'
__author__ = 'Ryan Flynn'

USAGE = "%prog [options] <url>"
VERSION = "%prog v" + __version__

# pretend to be IE9
AGENT = 'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)'

MAX_CONTENT_SIZE = 1024 * 1024

DBConn = sqlite3.connect('data/db.crawl.sqlite3.bin')
# singleton database connection
def db_conn():
	global DBConn
	return DBConn

class URL:
	def __init__(self, url, depth=0, urlfrom=None):
		self.url = url
		self.depth = depth # how many layers deep was i found?
		self.error = None
		self.resp = None
		self._db_url_id = None
		self.ahref = set()	# URLs that i link to
		# dependencies...
		# TODO: should i merge these? unless i'm going to do something separately
		# they may as well go in the same sets
		self.img = set()
		self.link = set()
		self.style = set()
		self.script = set()
		self.embed = set()
		self.frame = set()
		self.iframe = set()

	def is_error(self):
		return self.resp.error or self.resp.code >= 400

	# extract relevant information from fetch page
	# on error code will be set appropriately and content will be empty
	def fetched(self, resp):
		self.resp = resp
		soup = BeautifulSoup(resp.content)
		for tag,attr,save in [
			('a',     'href', self.ahref),
			('link',  'href', self.link),
			('img',   'src',  self.img),
			('style', 'src',  self.style),
			('script','src',  self.script),
			('embed','src',   self.embed),
			('frame', 'src',  self.frame),
			('iframe','src',  self.iframe) ]:
			tags = soup(tag)
			for t in tags:
				a = t.get(attr)
				if a:
					url = urlparse.urljoin(self.url, escape(a))
					save.add(url)
			save = frozenset(save)

	def all_links(self):
		return itertools.chain.from_iterable([
			self.ahref, self.img, self.link, self.style, self.script, self.embed, self.frame, self.iframe])

	def dependencies(self):
		return itertools.chain.from_iterable([
			self.img, self.link, self.style, self.script, self.embed, self.frame, self.iframe])

	def errors(self):
		return self.broken_links() or self.dependency_errors()

	def broken_links(self, urlobjs):
		return any(urlobjs[to].is_error() for to in self.ahref)

	def dependency_errors(self, urlobjs):
		return any(urlobjs[dep].is_error() for dep in self.dependencies())

	def __str__(self):
		return unicode('(%s, depth=%u, code=%u, size=%u)' % (self.url, self.depth, self.resp.code, self.resp.size()))
	def __repr__(self):
		return str(self)

	def db_url_id(self, db):
		"""
		insert self into url table, cache resulting id
		"""
		if not self._db_url_id:
			u = urlparse.urlparse(self.url)
			cur = db.cursor()
			sel = cur.execute('select id from url where scheme=? and host=? and path=? and params=? and query=? and fragment=?', u).fetchone()
			if sel:
				self._db_url_id = sel[0]
			else:
				cur.execute('insert into url(scheme,host,path,params,query,fragment)values(?,?,?,?,?,?)', u)
				self._db_url_id = cur.lastrowid
				#db.commit()
			#print('url=%s u=%s id=%s' % (self.url, u, self._db_url_id))
		assert self._db_url_id
		return self._db_url_id

	def save_to_db(self, run_id, urlobjs):
		"""
		save myself to the database;
		the results of fetching me, my headers, my dependencies and links
		"""
		db = db_conn()
		url_id = self.db_url_id(db)
		cur = db.cursor()
		cur.execute('insert into url_fetch (run_id,url_id,datetime,result,msec,bytes) values (?,?,?,?,?,?)',
			(run_id, url_id, int(self.resp.start), self.resp.result, self.resp.msec, self.resp.size()))
		fetch_id = db.execute('select last_insert_rowid()').fetchone()[0]
		cur.executemany('insert into url_fetch_header(url_fetch_id,header,value) values (?,?,?)',
			([(fetch_id, h, v) for h,v in self.resp.headers ]))
		# save dependencies
		for dep in self.dependencies():
			dep_id = urlobjs[dep].db_url_id(db)
			cur.execute('insert into url_depend (url_fetch_id,url_target_id) values (?,?)',
				(fetch_id, dep_id))
		# save outbound links
		for to in self.ahref:
			to_id = urlobjs[to].db_url_id(db)
			cur.execute( 'insert into url_link(url_fetch_id,url_target_id) values (?,?)',
				(fetch_id, to_id))
		#db.commit()

class Crawler:

	def __init__(self, root, max_depth, hostmask=[], max_urls=0, max_bytes=0, max_url_sec=5, verbose=False):
		self.verbose = verbose
		self.root = root
		self.max_depth = float('inf') if max_depth == 0 else max_depth
		self.max_urls  = float('inf') if max_urls  == 0 else max_urls
		self.max_bytes = float('inf') if max_bytes == 0 else max_bytes
		self.max_url_sec = max_url_sec
		self.host = urlparse.urlparse(root)[1]
		self.urls = {root : URL(root)}
		self.links = 0
		self.follow_hosts = set(hostmask)
		# track status
		self.urlcnt = 0
		self.bytecnt = 0

		# ensure we follow the root host
		self.follow_hosts.add(self.host.lower())

		db = db_conn()
		db.execute("""
insert into spider_run (url_id, url_max_sec, start_time, hosts_allowed)
values (?,?,?,?)
		""", (self.urls[root].db_url_id(db), max_url_sec,
			int(time.time()), ','.join(sorted(self.follow_hosts))))
		self.run_id = list(db.execute('select last_insert_rowid()'))[0][0]

	# allow partial host masks
	def host_allowed(self, host):
		hostl = host.lower()
		return any(hostl.endswith(h) for h in self.follow_hosts)

	def should_spider(self, url):
		scheme, host = urlparse.urlparse(url)[:2]
		if scheme.lower() not in ('http','https','ftp'):
			return False # skip stuff like 'mailto'
		return True

	def crawl(self):
		q = Queue()
		q.put(self.root)
		while self.urlcnt < self.max_urls:
			try:
				url = q.get_nowait()
			except QueueEmpty:
				break
			self.urlcnt += 1
			try:
				host = urlparse.urlparse(url)[1]
				if not self.should_spider(url):
					continue
				allowed = self.host_allowed(host)
				page = Fetcher(timeout=self.max_url_sec, verbose=self.verbose)
				page.fetch(url, allowed)
				self.bytecnt += page.size()
				urlobj = self.urls[url]
				urlobj.fetched(page)
				if allowed:
					# add any unseen links and dependencies
					for urlto in urlobj.all_links():
						if urlto not in self.urls:
							self.urls[urlto] = URL(urlto, urlobj.depth + 1, url)
							q.put(urlto)
				urlobj.save_to_db(self.run_id, self.urls)
				if urlobj.depth > self.max_depth:
					print >> sys.stderr, '# LIMIT REACHED max_depth (%s > %s)' % (
						urlobj.depth, self.max_depth)
					break
			except Exception, e:
				print "ERROR: Can't process url '%s' (%s)" % (url, e)
				print format_exc()
			if self.urlcnt % 20 == 0:
				try:
					db_conn().commit()
				except sqlite3.OperationalError:
					pass

		# announce a limit we've hit, if any
		if self.urlcnt >= self.max_urls:
			print >> sys.stderr, '# LIMIT REACHED max_urls (%s >= %s)' % (self.urlcnt, self.max_urls)
		if self.bytecnt >= self.max_bytes:
			print >> sys.stderr, '# LIMIT REACHED max_bytes (%s >= %s)' % (bytecnt, self.max_bytes)

class Fetcher:
	def __init__(self, timeout, verbose=False):
		self.verbose = verbose
		self.timeout = timeout
		self.error = None
		self.headers = []
		self.code = 0
		self.content = u''
		self.start = 0
		self.msec = 0
		self.header_size = 0
		self.result = 0 # 0=not tried, -1=discon, -2=timeout

	def is_html(self):
		c = [v for h,v in self.headers if h == 'content-type']
		return any(
			v.startswith('text/html') or v.startswith('application/xhtml')
				for v in c)

	def size(self):
		return self.header_size + int(self.content_size())

	def header(self, key, default=''):
		key = key.lower()
		h = [v for h,v in self.headers if h == key]
		if h:
			return h[0]
		return default

	def content_size(self):
		if self.content:
			return len(self.content)
		return self.header('content-length', 0)

	def fetch(self, url, full=True):
		self.start = time.time()
		try:
			# fetch header
			req = urllib2.Request(url)
			req.get_method = lambda : 'HEAD'
			req.add_header('User-Agent', AGENT)
			req.add_header('Accept', '*/*')
			req.add_header('Accept-Encoding:', 'compress, gzip')
			self.code = 200
			self.result = 200
			res = urllib2.urlopen(req, None, self.timeout)
			self.headers = []
			for h in res.info().headers:
				try:
					k,v = h.rstrip('\r\n').split(': ',1)
					self.headers.append((k.lower(),v))
				except:
					pass
			self.header_size = sum(len(h) for h in res.info().headers)
			# fetch contents if necessary
			if self.is_html() and full:
				req.get_method = lambda : 'GET'
				handle = urllib2.build_opener()
				h = handle.open(req)
				self.content = unicode(h.read(MAX_CONTENT_SIZE), 'utf-8', errors='replace')
				h.close()
			if self.verbose:
				print '# %s %s' % (self.code, url)
		except urllib2.HTTPError, error:
			self.code = error.code
			self.result = error.code
			print >> sys.stderr, '# %d %s' % (error.code, error.url)
		except urllib2.URLError, error:
			self.error = error
			self.result = -1
			print >> sys.stderr, '# ERR %s %s' % (url, error.reason)
		except Exception, x:
			self.error = x
			self.result = -2
			print >> sys.stderr, '# ERR %s' % (format_exc(),)
		finally:
			self.msec = int((time.time() - self.start) * 1000)

def parse_options():
	"""
	parse_options() -> opts, args

	Parse any command-line options given returning both
	the parsed options and arguments.
	"""

	parser = optparse.OptionParser(usage=USAGE, version=VERSION)

	parser.add_option('-q', '--quiet', action='store_true', default=False, help='Enable quiet mode')
	parser.add_option('-v', '--verbose', action='store_true', default=False, help='Display each URL as it is processed')
	parser.add_option('-d', '--depth', action='store', type='int', default=30, dest='depth', help='Maximum depth to traverse')
	parser.add_option('--host', action='append', default=[], help='Additional host domain(s) to crawl')
	parser.add_option('--url-timeout', action='store', default=5, help='Maximum time in seconds to wait for a URL response')
	parser.add_option('--max-urls', action='store', type='int', default=0, help='Maximum URLs to fetch')
	parser.add_option('--max-bytes', action='store', type='int', default=0, help='Maximum bytes to download')
	parser.add_option('--max-time', action='store', default=0, help='Maximum time in seconds to wait for a URL response')

	opts, args = parser.parse_args()

	if len(args) < 1:
		parser.print_help()
		raise SystemExit, 1

	return opts, args

def main():
	opts, args = parse_options()

	url = args[0]

	depth = opts.depth
	max_urls = opts.max_urls
	max_bytes = opts.max_bytes
	max_url_sec = opts.url_timeout

	sTime = time.time()

	print '# Started %s' % (time.strftime('%x %X'),)
	print '# Crawling %s (Max Depth: %d)' % (url, depth)

	crawler = Crawler(url, depth, hostmask=opts.host, max_urls=max_urls, max_url_sec=max_url_sec, verbose=opts.verbose)
	crawler.crawl()

	eTime = time.time()
	tTime = eTime - sTime

	print '# Links:	 %d' % (len(crawler.urls),)
	print '# Followed: %d' % (sum(u.resp != None for u in crawler.urls.values()),)
	print '# Stats:	(%d/s after %0.2fs)' % (
			int(math.ceil(float(len(crawler.urls)) / tTime)), tTime)

	#crawler.results_to_graphviz()

	db_conn().close()

if __name__ == "__main__":
	main()

