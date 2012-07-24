
rowcopy
=======

SImple program that runs SELECT query, converts result rows
into TAB-separated (COPY format) line and prints to stdout.

It suppords different ways of loading data from libpq,
thus it is useful for testing libpq protocol parsing
speed.

Usage::

  rowdump [-z|-s|-f|-x] [-d CONNSTR] [-c SQLCOMMAND]

switches::

  -f  Load full resultset into one PGresult, then process that
  -s  Single-row mode - each row in separate PGresult
  -z  Single-row with direct access - PQgetRowData()
  -x  Single-row with simulated fast PGresult copy

