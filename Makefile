
pg1 = /opt/apps/pgsql92mode1
pg2 = /opt/apps/pgsql92mode2

X1 = -DHAVE_ROWDATA -I$(pg1)/include/internal -I$(pg1)/include/server

CFLAGS = -O -g -Wall

all: rowdump1 rowdump2

rowdump1: rowdump.c
	$(CC) -I$(pg1)/include $(CFLAGS) -o $@ $< -L$(pg1)/lib -Wl,-rpath=$(pg1)/lib -lpq $(X1)

rowdump2: rowdump.c
	$(CC) -I$(pg2)/include $(CFLAGS) -o $@ $< -L$(pg2)/lib -Wl,-rpath=$(pg2)/lib -lpq

clean:
	rm -f rowdump1 rowdump2 time.tmp README.html

html: README.html

README.html: README.rst
	rst2html $< > $@


