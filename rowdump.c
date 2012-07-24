#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>

#include <libpq-fe.h>

#ifdef HAVE_ROWDATA
#include <internal/libpq-int.h>
#endif

struct Context {
	PGconn *db;
	int count;

	char *buf;
	int buflen;
	int bufpos;
};

/* print error and exit */
static void die(PGconn *db, const char *msg)
{
	if (db)
		fprintf(stderr, "%s: %s\n", msg, PQerrorMessage(db));
	else
		fprintf(stderr, "%s\n", msg);
	exit(1);
}

/* write out buffer */
static void out_flush(struct Context *ctx)
{
	int out;
	if (!ctx->buf)
		return;

	out = write(1, ctx->buf, ctx->bufpos);
	if (out != ctx->bufpos)
		die(NULL, "failed to write file");
	ctx->bufpos = 0;
	ctx->buflen = 0;
	free(ctx->buf);
	ctx->buf = NULL;
}

/* add char to buffer */
static void out_char(struct Context *ctx, char c)
{
	if (ctx->bufpos + 1 > ctx->buflen) {
		if (!ctx->buf) {
			ctx->buflen = 16;
			ctx->buf = malloc(ctx->buflen);
			if (!ctx->buf)
				die(NULL, "failed to allocate buffer");
		} else {
			ctx->buflen *= 2;
			ctx->buf = realloc(ctx->buf, ctx->buflen);
			if (!ctx->buf)
				die(NULL, "failed to resize buffer");
		}
	}

	ctx->buf[ctx->bufpos++] = c;
}

/* quote string for copy */
static void proc_value(struct Context *ctx, const char *val, int vlen)
{
	int i;
	char c;

	for (i = 0; i < vlen; i++) {
		c = val[i];
		switch (c) {
		case '\\':
			out_char(ctx, '\\');
			out_char(ctx, '\\');
			break;
		case '\t':
			out_char(ctx, '\\');
			out_char(ctx, 't');
			break;
		case '\n':
			out_char(ctx, '\\');
			out_char(ctx, 'n');
			break;
		case '\r':
			out_char(ctx, '\\');
			out_char(ctx, 'r');
			break;
		default:
			out_char(ctx, c);
			break;
		}
	}
}

/* quote one row for copy from regular PGresult */
static void proc_row(struct Context *ctx, PGresult *res, int tup)
{
	int n = PQnfields(res);
	const char *val;
	int i, vlen;

	ctx->count++;

	for (i = 0; i < n; i++) {
		if (i > 0)
			out_char(ctx, '\t');
		if (PQgetisnull(res, tup, i)) {
			out_char(ctx, '\\');
			out_char(ctx, 'N');
			continue;
		}

		vlen = PQgetlength(res, tup, i);
		val = PQgetvalue(res, tup, i);
		proc_value(ctx, val, vlen);
	}
	out_char(ctx, '\n');
	out_flush(ctx);
}

/* load everything to single PGresult */
static void exec_query_full(struct Context *ctx, const char *q)
{
	PGconn *db = ctx->db;
	PGresult *r;
	ExecStatusType s;
	int i;

	ctx->count = 0;

	if (!PQsendQuery(db, q))
		die(db, "PQsendQuery");

	/* get next result */
	r = PQgetResult(db);
	s = PQresultStatus(r);
	if (s != PGRES_TUPLES_OK)
		die(db, PQresStatus(s));

	for (i = 0; i < PQntuples(r); i++) {
		proc_row(ctx, r, i);
		ctx->count++;
	}
	PQclear(r);
}

/* load each row as PGresult */
static void exec_query_single_row(struct Context *ctx, const char *q)
{
	PGconn *db = ctx->db;
	PGresult *r;
	ExecStatusType s;

	ctx->count = 0;

	if (!PQsendQuery(db, q))
		die(db, "PQsendQuery");

	if (!PQsetSingleRowMode(db))
		die(NULL, "PQsetSingleRowMode");

	/* loop until all resultset is done */
	while (1) {
		/* get next result */
		r = PQgetResult(db);
		if (!r)
			break;
		s = PQresultStatus(r);
		switch (s) {
			case PGRES_TUPLES_OK:
				//printf("query successful, got %d rows\n", ctx->count);
				ctx->count = 0;
				break;
			case PGRES_SINGLE_TUPLE:
				/* process first (only) row */
				proc_row(ctx, r, 0);
				break;
			default:
				fprintf(stderr, "result: %s\n", PQresStatus(s));
				exit(1);
				break;
		}

		PQclear(r);
	}
}

#ifdef HAVE_ROWDATA

/* load row data directly from network buffer */
static void proc_row_zcopy(struct Context *ctx, PGresult *res, PGdataValue *cols)
{
	int n = PQnfields(res);
	const char *val;
	int i, vlen;

	ctx->count++;

	for (i = 0; i < n; i++) {
		if (i > 0)
			out_char(ctx, '\t');
		if (cols[i].len == -1) {
			out_char(ctx, '\\');
			out_char(ctx, 'N');
			continue;
		}

		vlen = cols[i].len;
		val = cols[i].value;
		proc_value(ctx, val, vlen);
	}
	out_char(ctx, '\n');
	out_flush(ctx);
}

/* load rows directly from network buffer */
static void exec_query_zero_copy(struct Context *ctx, const char *q)
{
	PGconn *db = ctx->db;
	PGresult *r;
	ExecStatusType s;
	PGdataValue *cols;

	ctx->count = 0;

	if (!PQsendQuery(db, q))
		die(db, "PQsendQuery");

	if (!PQsetSingleRowMode(db))
		die(NULL, "PQsetSingleRowMode");

	/* loop until all resultset is done */
	while (PQgetRowData(db, &r, &cols)) {
		proc_row_zcopy(ctx, r, cols);
	}

	/* get final result */
	r = PQgetResult(db);
	s = PQresultStatus(r);
	switch (s) {
	case PGRES_TUPLES_OK:
		//printf("query successful, got %d rows\n", ctx->count);
		ctx->count = 0;
		break;
	default:
		printf("result: %s\n", PQresStatus(s));
		break;
	}
	PQclear(r);
}

/* create new PGresult as fast as possible */
static char *fake_copy(PGresult *res, PGdataValue *cols, char **data_p)
{
	PGresult_data *hdr = res->curBlock;
	int n = PQnfields(res);
	int slen;
	int datalen;
	int hdrlen;
	char *buf;

	/* structure length */
	slen = sizeof(PGresult);

	/* row headers */
	hdrlen = res->curOffset;

	/* row data */
	datalen = cols[n-1].value - cols[0].value;
	if (cols[n-1].len > 0)
		datalen += cols[n-1].len;

	/* malloc & copy */
	buf = malloc(slen + hdrlen + datalen);
	if (!buf)
		return NULL;
	memcpy(buf, res, slen);
	memcpy(buf + slen, hdr, hdrlen);
	memcpy(buf + slen + hdrlen, cols[0].value, datalen);

	/* return also data offset */
	*data_p = buf + slen + hdrlen;

	return buf;
}

/* use cols for offsets only, load actual data from *data */
static void proc_row_fake(struct Context *ctx, PGresult *res, PGdataValue *cols, char *data)
{
	int n = PQnfields(res);
	const char *val;
	int i, vlen;
	const char *xstart = cols[0].value;

	ctx->count++;

	for (i = 0; i < n; i++) {
		if (i > 0)
			out_char(ctx, '\t');
		if (cols[i].len == -1) {
			out_char(ctx, '\\');
			out_char(ctx, 'N');
			continue;
		}

		vlen = cols[i].len;

		/* recalcuate value into *data */
		val = data + (cols[i].value - xstart);

		proc_value(ctx, val, vlen);
	}
	out_char(ctx, '\n');
	out_flush(ctx);
}

/* emulate optimized PGresult */
static void exec_query_fake_copy(struct Context *ctx, const char *q)
{
	PGconn *db = ctx->db;
	PGresult *r;
	ExecStatusType s;
	PGdataValue *cols;

	ctx->count = 0;

	if (!PQsendQuery(db, q))
		die(db, "PQsendQuery");

	if (!PQsetSingleRowMode(db))
		die(NULL, "PQsetSingleRowMode");

	/* loop until all resultset is done */
	while (PQgetRowData(db, &r, &cols)) {
		char *data = NULL;
		void *copy = fake_copy(r, cols, &data);
		proc_row_fake(ctx, r, cols, data);
		free(copy);
	}

	/* get final result */
	r = PQgetResult(db);
	s = PQresultStatus(r);
	switch (s) {
	case PGRES_TUPLES_OK:
		//printf("query successful, got %d rows\n", ctx->count);
		ctx->count = 0;
		break;
	default:
		printf("result: %s\n", PQresStatus(s));
		break;
	}
	PQclear(r);
}

#else

static void exec_query_fake_copy(struct Context *ctx, const char *q)
{
	die(NULL, "PQgetRowData() not available");
}

static void exec_query_zero_copy(struct Context *ctx, const char *q)
{
	die(NULL, "PQgetRowData() not available");
}

#endif

static const char usage_str[] =
"usage: rowdump [-z|-s|-f|-x] [-d CONNSTR] [-c SQLCOMMAND]\n"
"switches:\n"
"  -f  Load full resultset at once\n"
"  -s  Single-row mode\n"
"  -z  Single-row with direct access (PQgetRowData())\n"
"  -x  Single-row with fast PGresult copy\n"
;


static void usage(int err)
{
	printf("%s\n", usage_str);
	exit(err);
}

int main(int argc, char *argv[])
{
	const char *connstr = "dbname=postgres";
	const char *q = "show all";
	PGconn *db;
	struct Context ctx;
	char c;
	int exec_type = 'f';

	while ((c = getopt(argc, argv, "c:d:zsxfh")) != -1) {
		switch (c) {
		case 'c':
			q = optarg;
			break;
		case 'd':
			connstr = optarg;
			break;
		case 'z':
		case 's':
		case 'f':
		case 'x':
			exec_type = c;
			break;
		case 'h':
			usage(0);
			break;
		default:
			usage(1);
			break;
		}
	}

	db = PQconnectdb(connstr);
	if (!db || PQstatus(db) == CONNECTION_BAD)
		die(db, "connect");

	memset(&ctx, 0, sizeof(ctx));
	ctx.db = db;
	switch (exec_type) {
	case 'x':
		exec_query_fake_copy(&ctx, q);
		break;
	case 'z':
		exec_query_zero_copy(&ctx, q);
		break;
	case 'f':
		exec_query_full(&ctx, q);
		break;
	case 's':
		exec_query_single_row(&ctx, q);
		break;
	}

	PQfinish(db);

	return 0;
}

