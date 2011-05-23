#!/usr/bin/env python
# -*- coding:utf-8 -*-
# ex: set ts=4 noet:

"""
Extract data from the latest spider run and output a graphviz .dot file
color-coding URLs red,orange,yellow or gray depending on what's broken
"""

import sqlite3
import re
import sys

RunAdjust = 0
if sys.argv[1:]:
	RunAdjust = int(sys.argv[1])

Conn = sqlite3.connect('spider.sqlite3.bin')
Cur = Conn.cursor()

Run_Id,URL_Timeout,Root_Host,Root_URL,Hosts_Allowed = Cur.execute("""
	select
		r.id,
		r.url_max_sec,
		u.host,
		u.scheme || '://' || u.host || u.path || u.params || (case when u.query == '' then '' else '?' || u.query end) || (case when u.fragment == '' then '' else '#' || u.fragment end),
		hosts_allowed
	from spider_run r
	join url u on u.id = r.url_id
	where r.id = (select max(id) from spider_run)+?""", (RunAdjust,)).fetchone()

print '# Run_Id=%s' % (Run_Id,)

print """
digraph {
	fontname="Arial" fontsize=11
	node [fontname="Arial",fontsize=9,color=green3,style=filled]
	edge [arrowsize=0.7,color=gray40]
	ratio=compress
	label="Root URL: %s\\nHosts: %s\\nTimeout: %u sec"
	{ rank = min;
    Legend [shape=none, margin=0, label=<
    <table border="0" cellborder="0" cellspacing="0" cellpadding="4">
     <tr><td bgcolor="white">Legend</td></tr>
     <tr><td>OK</td></tr>
     <tr><td bgcolor="gray90">Not Fetched</td></tr>
     <tr><td bgcolor="yellow2">Broken Link(s)</td></tr>
     <tr><td bgcolor="orange">Dependency Error(s)</td></tr>
     <tr><td bgcolor="firebrick1">4xx/5xx Error</td></tr>
     <tr><td bgcolor="mediumorchid2">Connect/Timeout</td></tr>
    </table>
   >];
  }
""" % (Root_URL, Hosts_Allowed, URL_Timeout)

Urls = Cur.execute("""
-- for each url
--   status
--   sum broken dependencies
--   sum broken links
select
	u.id
	,u.scheme || '://' || u.host
	,u.scheme || '://' || u.host || u.path || u.params || (case when u.query == '' then '' else '?' || u.query end) || (case when u.fragment == '' then '' else '#' || u.fragment end)
	,u.path || u.params || (case when u.query == '' then '' else '?' || u.query end) || (case when u.fragment == '' then '' else '#' || u.fragment end)
	,sum( f.result > 0 and f.result <  400) = sum(1)-- i was fetched
	,sum( f.result < 0) > 0							-- conn/timeout error
	,sum( f.result >= 400) > 0						-- fetched, returned 4xx/5xx
	,sum(df.result < 0 or df.result >= 400) > 0		-- broken dependencies
	,sum(lf.result < 0 or lf.result >= 400) > 0		-- broken links
from url u
left join url_fetch  f on f.url_id = u.id
left join url_depend d on d.url_fetch_id = f.id
left join url_link   l on l.url_fetch_id = f.id
left join url_fetch df on df.url_id = d.url_target_id
left join url_fetch lf on lf.url_id = l.url_target_id
where f.run_id = ? or f.run_id is null -- pull URLs, possibly external, that we didn't fetch
group by u.id
order by u.host, u.path;""", (Run_Id,)).fetchall()

# cluster by scheme://host
CurrHost = None
CurrDir = None
for id,host,url,path,was_fetched,err_conn,res,dep,link in Urls:
	#print '# url=%s fetched=%s err_conn=%s res=%s...' % (url, was_fetched, err_conn, res)
	esc_host = re.sub('\W', '_', host)
	dir, file = path[:path.rfind('/')+1], path[path.rfind('/')+1:]
	if host != CurrHost:
		if CurrHost:
			if host == Root_Host and CurrDir:
				print '\t\t}'
			print '\t}'
		print '\tsubgraph cluster_%s {' % (esc_host,)
		print '\tpenwidth=0.5'
		print '\tlabel="%s"' % (host,)
		CurrHost = host
		CurrDir = None
	if dir != CurrDir:
		if host == Root_Host:
			esc_dir = re.sub('\W', '_', dir)
			print '\t\tsubgraph cluster_%s_%s {' % (esc_host, esc_dir)
			print '\t\tpenwidth=0.2'
			print '\t\tlabel="%s"' % (dir,)
		CurrDir = dir
	bg = ' color=firebrick1' if res else \
		 ' color=orange' if dep else \
		 ' color=yellow2' if link else \
		 ' color=mediumorchid2' if err_conn else \
		 ' color=gray90' if not was_fetched else ''
	if host != Root_Host:
		file = path
	print '\t\t"%d" [label="%s" URL="%s"%s]' % (id, file, url, bg)
if host == Root_Host and CurrDir:
	print '\t\t}'
print '\t}'

Depends = Cur.execute("""
	select
		f.url_id,
		d.url_target_id
	from url_fetch f
	join url_depend d on d.url_fetch_id = f.id
	where f.run_id = ?""", (Run_Id,))
for x,y in Depends:
	print '\t"%s" -> "%s"' % (y,x)

Links = Cur.execute("""
	select
		f.url_id,
		l.url_target_id
	from url_fetch f
	join url_link l on l.url_fetch_id = f.id
	where f.run_id = ?""", (Run_Id,))
for x,y in Links:
	print '\t"%s" -> "%s"' % (x,y)

print '}'

