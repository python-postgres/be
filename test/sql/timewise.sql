SET TIMEZONE TO MST;

CREATE OR REPLACE FUNCTION date_properties(d date) RETURNS setof text LANGUAGE python AS
$python$

def main(d):
	return ['date: ' + str(d)] + [
		': '.join((attname, str(getattr(d, attname))))
		for attname in [
			'millennium',
			'century',
			'decade',
			'year',
			'quarter',
			'week',
			'month',
			'day',
			'epoch',
			'dayofweek',
			'dayofyear',
		]
	]
$python$;

SELECT date_properties('2000-01-01'::date);
SELECT date_properties('1000-01-01'::date);
SELECT date_properties('1000-12-30'::date);
SELECT date_properties('3000-12-30'::date);
SELECT date_properties('1-1-0001'::date);

CREATE OR REPLACE FUNCTION time_properties(d time) RETURNS setof text LANGUAGE python AS
$python$

def main(d):
	return ['time: ' + str(d)] + [
		': '.join((attname, str(getattr(d, attname))))
		for attname in [
			'hour',
			'minute',
			'second',
			'millisecond',
			'microsecond',
			'epoch',
		]
	]
$python$;

SELECT time_properties('0:0:0.0'::time);
SELECT time_properties('23:59:59'::time);
SELECT time_properties('10:30:00.123'::time);
SELECT time_properties('12:30:35.22222'::time);


CREATE OR REPLACE FUNCTION timetz_properties(d timetz) RETURNS setof text LANGUAGE python AS
$python$

def main(d):
	return ['timetz: ' + str(d)] + [
		': '.join((attname, str(getattr(d, attname))))
		for attname in [
			'timezone',
			'hour',
			'minute',
			'second',
			'millisecond',
			'microsecond',
			'epoch',
		]
	]
$python$;

SELECT timetz_properties('0:0:0.0'::timetz);
SELECT timetz_properties('23:59:59'::timetz);
SELECT timetz_properties('10:30:00.123'::timetz);
SELECT timetz_properties('12:30:35.22222'::timetz);

SELECT timetz_properties('0:0:0.0+0700'::timetz);
SELECT timetz_properties('0:0:0.0-0700'::timetz);
SELECT timetz_properties('23:59:59+300'::timetz);
SELECT timetz_properties('23:59:59-300'::timetz);
SELECT timetz_properties('11:30:00.123+1245'::timetz);
SELECT timetz_properties('15:30:35.22222-0352'::timetz);



CREATE OR REPLACE FUNCTION timestamp_properties(d timestamp) RETURNS setof text LANGUAGE python AS
$python$

def main(d):
	return ['timestamp: ' + str(d)] + [
		': '.join((attname, str(getattr(d, attname))))
		for attname in [
			'millennium',
			'century',
			'decade',
			'year',
			'quarter',
			'week',
			'month',
			'day',
			'epoch',
			'hour',
			'minute',
			'second',
			'millisecond',
			'microsecond',
			'dayofweek',
			'dayofyear',
		]
	]
$python$;

SELECT timestamp_properties('2000-01-01 0:0:0.0'::timestamp);
SELECT timestamp_properties('1990-06-01 23:59:59'::timestamp);
SELECT timestamp_properties('2020-10-10 10:30:00.123'::timestamp);
SELECT timestamp_properties('2045-12-20 12:30:35.22222'::timestamp);

SELECT timestamp_properties('2200-01-01 0:0:0.0'::timestamp);
SELECT timestamp_properties('1000-01-01 0:0:0.0'::timestamp);
SELECT timestamp_properties('1500-02-21 23:59:59'::timestamp);
SELECT timestamp_properties('2499-12-31 23:59:59'::timestamp);
SELECT timestamp_properties('2100-01-01 11:30:00.123'::timestamp);
SELECT timestamp_properties('2235-01-01 15:30:35.22222'::timestamp);


CREATE OR REPLACE FUNCTION timestamptz_properties(d timestamptz) RETURNS setof text LANGUAGE python AS
$python$

def main(d):
	return ['timestamptz: ' + str(d)] + [
		': '.join((attname, str(getattr(d, attname))))
		for attname in [
			'timezone',
			'millennium',
			'century',
			'quarter',
			'decade',
			'year',
			'week',
			'month',
			'day',
			'epoch',
			'hour',
			'minute',
			'second',
			'millisecond',
			'microsecond',
			'dayofweek',
			'dayofyear',
		]
	]
$python$;

SELECT timestamptz_properties('2000-01-01 0:0:0.0'::timestamptz);
SELECT timestamptz_properties('1990-06-01 23:59:59'::timestamptz);
SELECT timestamptz_properties('2020-10-10 10:30:00.123'::timestamptz);
SELECT timestamptz_properties('2045-12-20 12:30:35.22222'::timestamptz);

SELECT timestamptz_properties('2200-01-01 0:0:0.0+0800'::timestamptz);
SELECT timestamptz_properties('1000-01-01 0:0:0.0-0800'::timestamptz);
SELECT timestamptz_properties('1500-02-21 23:59:59+300'::timestamptz);
SELECT timestamptz_properties('2499-12-31 23:59:59-300'::timestamptz);
SELECT timestamptz_properties('2100-01-01 11:30:00.123+1245'::timestamptz);
SELECT timestamptz_properties('2235-01-01 15:30:35.22222-0352'::timestamptz);



CREATE OR REPLACE FUNCTION interval_properties(d interval) RETURNS setof text LANGUAGE python AS
$python$

def main(d):
	return ['interval: ' + str(d)] + [
		': '.join((attname, str(getattr(d, attname))))
		for attname in [
			'millennium',
			'century',
			'decade',
			'year',
			'quarter',
			'month',
			'day',
			'epoch',
			'hour',
			'minute',
			'second',
			'millisecond',
			'microsecond',
		]
	]
$python$;

SELECT interval_properties('0'::interval);
SELECT interval_properties('1 second'::interval);
SELECT interval_properties('1 day'::interval);
SELECT interval_properties('1 year'::interval);

SELECT interval_properties('4 weeks 7 days 2 hours 7 minutes 2 seconds 10232 microseconds'::interval);
SELECT interval_properties('1 month'::interval);
SELECT interval_properties('85 years'::interval);
SELECT interval_properties('1 century'::interval);
SELECT interval_properties('1 millennium'::interval);
SELECT interval_properties('3 months 6 days'::interval);
