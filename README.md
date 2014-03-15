JSONAlchemy
==

[![Build Status](https://travis-ci.org/mwhite/JSONAlchemy.png?branch=master)](https://travis-ci.org/mwhite/JSONAlchemy)
[![Coverage Status](https://coveralls.io/repos/mwhite/JSONAlchemy/badge.png?branch=master)](https://coveralls.io/r/mwhite/JSONAlchemy?branch=master)
[![Dependency Status](https://gemnasium.com/mwhite/JSONAlchemy.png)](https://gemnasium.com/mwhite/JSONAlchemy)
[![Code Health](https://landscape.io/github/mwhite/JSONAlchemy/master/landscape.png)](https://landscape.io/github/mwhite/JSONAlchemy/master)
[![Stories in Ready](https://badge.waffle.io/mwhite/jsonalchemy.png?label=ready&title=Ready)](https://waffle.io/mwhite/jsonalchemy)

JSONAlchemy is an experimental [SQLAlchemy](http://www.sqlalchemy.org) (0.8+)
extension that lets you create a structured view of properties in a Postgres
[JSON](http://www.postgresql.org/docs/9.3/static/datatype-json.html) column
given a [JSON Schema](http://json-schema.org).

It makes use of some specialized features of PostgreSQL (9.2+), but it should be
mostly adaptable to other databases with similar functionality.

Usage
--

See the tests for full working examples and additional options.

You have a table with a Postgres JSON column.

```sql
INSERT INTO forms (type_id, data) VALUES
    (1, '{"foo": {"bar": 5}, "baz": "spam"}'),
    (1, '{"foo": {"bar": 6}, "baz": "eggs"}'),
    (1, '{"baz": "eggs"}'),
    (2, '{"foo": {"bar": "type 2 is ignored"}, "baz": 7}');
```

You know a JSON Schema for a subset of the data.

```python
>>> q = session.query(Form).filter(Form.type_id == 1)
>>> schema = {
...     'type': 'object',
...     'properties': {
...          'foo': {
...              'type': 'object',
...              'properties': {
...                  'bar': {
...                      'type': 'integer'
...                  }
...              }
...          },
...          'baz': {
...              'type': 'string'
...          }
...     }
... }
```

(`Form` is an SQLAlchemy declarative model.  You can use an existing model, or
use SQLAlchemy's [database
introspection](http://docs.sqlalchemy.org/en/rel_0_9/core/reflection.html).)

JSONAlchemy lets you create a structured view of your JSON data backed by a
unique [partial
index](http://www.postgresql.org/docs/9.3/static/indexes-partial.html) for each
property. 

```python
>>> from jsonalchemy import CreateJSONView
>>> create_json_view = CreateJSONView('my_view', q, Form.data, schema)
>>> session.execute(create_json_view)
```

Voila!

```sql
=# SELECT * FROM my_view;
 forms_id | data.foo.bar | data.baz 
----------+--------------+------------
        3 |              | eggs
        2 |            6 | eggs
        1 |            5 | spam
```

Due to a
[bug / missing feature in Postgres](http://postgresql.1045698.n5.nabble.com/No-Index-Only-Scan-on-Partial-Index-td5773024.html)
, queries on created views will not trigger super-fast [index-only
scans](https://wiki.postgresql.org/wiki/Index-only_scans) using the created
partial indexes, but this is a temporary situation.

### Supported data types

Postgres type | JSON representation | JSON Schema definition
--- | --- | ---
text | string | type: 'string'
int | integer | type: 'integer'
float | float | type: 'number'
decimal | string | type: 'string', format: 'decimal'
boolean | boolean |  type: 'boolean'
timestamp with timezone | [milliseconds since epoch, or ISO8601 or RFC 2822 string][datetime] | type: 'string', format: 'date-time'
timestamp without timezone | same as above | type: 'string', format: 'date-time-no-tz'
date | same as above | type: 'string', format: 'date'
PostGIS point (Geometry) | "\<lng\>,\<lat\>" | type: 'string', format': 'geopoint'

 [datetime]: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Date

### JSON Schema support

The `oneOf`, `allOf`, and `anyOf` fields are supported. (They have no
special interpretation from the perspective of this tool.)

Support for defining objects as arrays (of simple values, or of complex objects)
does not yet exist.

All properties must have a single type defined.

License
--

Copyright 2014 Michael White

Released under the MIT License. See LICENSE.txt.
