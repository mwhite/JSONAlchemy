JSONAlchemy
==

JSONAlchemy makes it easier to use a relational database to deal with data that
you might otherwise use a NoSQL database for, such as nested JSON
data, JSON data with missing values, and multi-tenant or similar JSON data
with different schemas for each tenant.

When using PostgreSQL 9.3+ with a JSON or JSONB column, JSONAlchemy
lets you create a traditional table interface for accessing a subset of a JSON table
by using a view specified in terms of a [JSON Schema](http://json-schema.org)
and a query on the table.

Each property in the view is backed by a unique [partial
index](https://www.postgresql.org/docs/current/indexes-partial.html) to ensure
optimum query performance using [index-only
scans](https://wiki.postgresql.org/wiki/Index-only_scans).

JSONAlchemy is implemented as an [SQLAlchemy](http://www.sqlalchemy.org) extension.

Usage
--

First, create a table with a Postgres JSON column.

```sql
INSERT INTO forms (type_id, data) VALUES
    (1, '{"foo": {"bar": 5}, "baz": "spam"}'),
    (1, '{"foo": {"bar": 6}, "baz": "eggs"}'),
    (1, '{"baz": "eggs"}'),
    (2, '{"foo": {"bar": "type 2 is ignored"}, "baz": 7}');
```

Then, define a JSON Schema for a subset of the data.

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

(`Form` is an SQLAlchemy declarative model.  SQLAlchemy's [database
introspection](http://docs.sqlalchemy.org/en/rel_0_9/core/reflection.html) can
be used if SQLAlchemy models are not used for the tables being handled.)

Finally, create a view of the JSON data using an application-level pseudo-DDL
statement in an SQLAlchemy session.

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

See the tests for full working examples and additional options.

### Supported data types

Postgres type | JSON representation | JSON Schema definition
--- | --- | ---
text | string | type: 'string'
int | integer | type: 'integer'
float | float | type: 'number'
decimal | string | type: 'string', format: 'decimal'
boolean | boolean |  type: 'boolean'
timestamp with timezone | milliseconds since epoch or ISO 8601 string | type : 'string', format: 'date-time'
timestamp without timezone | same as above | type: 'string', format: 'date-time-no-tz'
date | same as above | type: 'string', format: 'date'
PostGIS point (Geometry) | "\<lng\>,\<lat\>" | type: 'string', format': 'geopoint'

### JSON Schema support

The `oneOf`, `allOf`, and `anyOf` fields are supported. (They have no
special interpretation from the perspective of this tool.)

Support for defining objects as arrays (of simple values, or of complex objects)
does not yet exist.

All properties must have a single type defined.

License
--

Copyright 2014-2020 Michael White

Released under the MIT License. See LICENSE.txt.
