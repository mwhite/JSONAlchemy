JSONAlchemy
==

[![Build Status](https://travis-ci.org/mwhite/JSONAlchemy.png?branch=master)](https://travis-ci.org/mwhite/JSONAlchemy)
[![Coverage Status](https://coveralls.io/repos/mwhite/JSONAlchemy/badge.png?branch=master)](https://coveralls.io/r/mwhite/JSONAlchemy?branch=master)

JSONAlchemy is an [SQLAlchemy](http://www.sqlalchemy.org) (0.8+) extension that
automates a way of setting up structured access to unstructured JSON data when
you know its likely [JSON Schema](http://json-schema.org/).

It makes use of some specialized features of PostgreSQL (9.2+), although it should be
adaptable to other databases with similar functionality.

* [JSON data type][0] (for storing JSON data)
* [PLV8][1] (for easily extracting values from stored JSON data)
* [Partial Indexes][2] (for fast retrieval of data, while allowing different
  subsets of data to have different types for different values within the JSON)
* [Views][3] (standard SQL views)

 [0]: http://www.postgresql.org/docs/9.3/static/datatype-json.html
 [1]: http://code.google.com/p/plv8js/wiki/PLV8
 [2]: http://www.postgresql.org/docs/9.3/static/indexes-partial.html
 [3]: http://www.postgresql.org/docs/9.3/static/sql-createview.html

Why?
--

Even if you always know your data's schema, it isn't always possible to store it
in a structured table.  Migrations are hard, and sometimes you want to keep your
old data in its original format.

This is especially true in a multi-tenant application where each tenant creates
data of different schemas.  JSONAlchemy can integrate with
[MultiAlchemy](http://github.com/mwhite/MultiAlchemy) to provide a complete
solution for handling multi-tenant semi-structured data.

You shouldn't have to use a NoSQL, no-ACID database when all you want is
schemaless storage!

Usage
--

JSONAlchemy can be used with existing SQLAlchemy models, or you can use
SQLAlchemy's database introspection to dynamically create models.

Here are some SQL and Python commands demonstrating the minimal usage of
JSONAlchemy, with some cleaned up echoed SQL statements showing the basics of
what's going on:


```sql
CREATE TABLE form_types (
	id SERIAL NOT NULL, 
	name VARCHAR(200), 
	PRIMARY KEY (id)
);
CREATE TABLE forms (
	id SERIAL NOT NULL, 
	type_id INTEGER, 
	data JSON, 
	PRIMARY KEY (id), 
	FOREIGN KEY(type_id) REFERENCES form_types (id)
);

INSERT INTO form_types (name) VALUES ('type 1'), ('type 2');
INSERT INTO forms (type_id, data) VALUES
    (1, '{"foo": {"bar": 5}, "baz": "spam"}'),
    (1, '{"foo": {"bar": 6}, "baz": "eggs"}'),
    (1, '{"baz": "eggs"}'),
    (2, '{"foo": {"bar": "type 2 is ignored"}, "baz": 7}'),
    (2, '{"foo": {"bar": "meta-syntactic variable"}, "baz": 8}');
```

```python
>>> from sqlalchemy import *
>>> from sqlalchemy.ext.declarative import declarative_base
>>> from sqlalchemy.orm import create_session
>>> import jsonalchemy
>>> Base = declarative_base()
>>> engine = create_engine("postgresql://user:pass@host/db")
>>> metadata = MetaData(bind=engine)
>>> class Form(Base):
...     __table__ = Table('forms', metadata, autoload=True)
...
>>> class FormType(Base):
...     __table__ = Table('form_types', metadata, autoload=True)
...
>>> jsonalchemy.install_plv8_json(engine)
CREATE OR REPLACE FUNCTION
json_string(data json, key text) RETURNS TEXT AS $$
    ...
$$ LANGUAGE plv8 IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION
json_int(data json, key text) RETURNS INT AS $$
    ...
$$ LANGUAGE plv8 IMMUTABLE STRICT;

... [additional functions for each Postgres type]
>>> session = create_session(bind=engine)
>>> q = session.query(Form).filter(Form.type_id == 1)
>>> create_json_view = jsonalchemy.CreateJSONView('my_view', q, Form.data, {
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
... })
>>> session.execute(create_json_view)
CREATE VIEW my_view AS 
    SELECT 
        forms.id AS forms_id, 
        json_int(forms.data, 'foo.bar') AS "data.foo.bar", 
        json_string(forms.data, 'baz') AS "data.baz"
    FROM forms 
    WHERE forms.type_id = 1;

CREATE INDEX "forms_xo0CaFvZkASAUA_hedKxNj3i5hgeQ" 
    ON forms (json_int(data, 'foo.bar')) 
    WHERE type_id = 1;

CREATE INDEX "forms_xo0CaFvZkASAUA_eyUAQr6QJxPdHA"
    ON forms (json_string(data, 'baz'))
    WHERE type_id = 1;
```

This creates partial indexes limited by the query passed as the second argument
to `CreateJSONView`. The indexes have a unique name that encodes the limiting
query and the indexed expression, to avoid ever creating duplicate indexes.

Additionally, after executing the statements, `create_view.columns` contains a
list of the columns in the view.

You can also pass a list of valid [`date_part`
names](http://www.postgresql.org/docs/9.3/static/functions-datetime.html#FUNCTIONS-DATETIME-EXTRACT)
to `CreateJSONView` as `extract_date_parts` to have them extracted as additional
columns for any date or datetime columns.

```sql
=# SELECT * FROM my_view;
 forms_id | data.foo.bar | data.baz 
----------+--------------+------------
        3 |              | eggs
        2 |            6 | eggs
        1 |            5 | spam
```

### Supported data types

Postgres type | JSON representation | JSON Schema definition
--- | --- | ---
text | string | type: 'string'
int | integer | type: 'integer'
float | float | type: 'number'
decimal | string | type: 'string', format: 'decimal'
boolean | boolean |  type: 'boolean'
timestamp with timezone | [milliseconds since epoch, or ISO8601 or RFC 2822 string][datetime] | type: 'string', format: 'date-time'
timestamp without timezone | same as above | type: 'string, format: 'date-time-no-tz'
date | same as above | type: 'string', format: 'date'
PostGIS point (Geometry) | "\<lng\>,\<lat\>" | type: 'string', format': 'geopoint'

 [datetime]: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Date

### JSON Schema support

The basic JSON Schema format for an uncomplicated type definition is supported.

The `oneOf`, `allOf`, and `anyOf` fields are also supported. (They have no
special interpretation from the perspective of this tool.)

Support for defining objects as arrays (of simple values, or of complex objects)
does not yet exist.

All properties must have a single type defined.

### Caveats

Due to a [bug](http://postgresql.1045698.n5.nabble.com/No-Index-Only-Scan-on-Partial-Index-td5773024.html) / lack of a feature in Postgres, queries on created views will not
trigger super-fast [Index-only
scans](https://wiki.postgresql.org/wiki/Index-only_scans), but this should
change eventually.

License
--

Copyright 2012 Michael White

Released under the MIT License. See LICENSE.txt.
