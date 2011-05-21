
graph: graph.svg
	./graph-dep-link.py > graph.dot && fdp -Tsvg -o graph.svg graph.dot

database: spider.sqlite3.bin
	sqlite3 spider.sqlite3.bin < spider.sql

