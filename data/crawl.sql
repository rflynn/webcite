-- 
-- 
-- 

drop table if exists url;
create table url
(
	id		integer primary key autoincrement,
	scheme		text not null,
	host		text not null,
	path		text not null,
	params		text not null,
	query		text not null,
	fragment	text not null,
	unique (scheme, host, path, params, query, fragment) on conflict ignore
);

-- log each run of the spider
drop table if exists spider_run;
create table spider_run
(
	id		integer primary key autoincrement,
	url_id		integer unsigned,
	url_max_sec	integer unsigned,
	start_time	integer unsigned not null,
	hosts_allowed	text not null,
	foreign key (url_id) references url(id)
);

-- log each fetch of each URL; when it occured, how long it took and the result
drop table if exists url_fetch;
create table url_fetch
(
	id		integer primary key autoincrement,
	run_id		integer,
	url_id		integer,
	datetime	integer unsigned not null, -- SELECT strftime('%s','now');
	result		integer not null default 0, -- http code/0=not tried/-1=discon/-2=timeout
	msec		integer unsigned not null default 0,
	bytes		integer unsigned not null default 0,
	foreign key (run_id) references run(id),
	foreign key (url_id) references url(id)
);

-- for each successful URL HEAD, record each header
drop table if exists url_fetch_header;
create table url_fetch_header
(
	url_fetch_id	integer unsigned,
	header		text not null,
	value		text not null,
	foreign key (url_fetch_id) references url_fetch(id)
);

-- record which external dependencies each URL has
drop table if exists url_depend;
create table url_depend
(
	url_fetch_id	integer unsigned,
	url_target_id	integer unsigned,
	foreign key (url_fetch_id) references url_fetch(id),
	foreign key (url_target_id) references url(id)
);

-- record which url links to what
drop table if exists url_link;
create table url_link
(
	url_fetch_id	integer unsigned,
	url_target_id	integer unsigned,
	foreign key (url_fetch_id) references url_fetch(id),
	foreign key (url_target_id) references url(id)
);

