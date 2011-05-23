
graph-links:
	./graph-links.py > graph-links.dot && fdp -Tsvg -o graph-links.svg graph-links.dot

database:
	$(MAKE) -C data database

