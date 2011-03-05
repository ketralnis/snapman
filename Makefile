all: README.html

README.html: README.md
	markdown -o README.html README.md

clean:
	rm -f README.html *.pyc
