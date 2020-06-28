JSONAlchemy
==

JSONAlchemy makes it easier to use a relational database to deal with data that
you might otherwise use a NoSQL database for, such as nested JSON
data, JSON data with missing values, and multi-tenant or multi-user JSON data
with different schemas for each tenant.

When using PostgreSQL 9.4+ with a JSON or JSONB column, JSONAlchemy
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
    (1, '{"foo": {"bar": 6}, "baz": "quux"}'),
    (1, '{"baz": "quux"}'),
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
        3 |              | quux
        2 |            6 | quux
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
timestamp with timezone | milliseconds since epoch or ISO 8601 string | type : 'string', format: 'date-time'
timestamp without timezone | same as above | type: 'string', format: 'date-time-no-tz'
date | same as above | type: 'string', format: 'date'
PostGIS point (Geometry) | "\<lng\>,\<lat\>" | type: 'string', format': 'geopoint'

### Views of arrays of objects

Arrays of objects in the JSON data handled by JSONAlchemy are supported using
the `array` JSON Schema type.

JSONAlchemy creates a separate table for each array property containing one row
for each array value.

```sql
INSERT INTO forms (type_id, data) VALUES
    (2, '{"id": 1, "foo": [{"bar": 5, "baz": "spam"}, {"bar": 7, "baz": "foobar"}]}'),
    (2, '{"id": 2, "foo": [{"bar": 6, "baz": "hjkl"}, {"bar": 8, "baz": "foobar"}]}'),
    (2, '{"id": 3, "baz": "quux"}');
```

Specify the ID property name in a non-standard `id_property` field in the schema
one level above the array property to indicate the ID to be used in the join
condition.

```python
>>> q = session.query(Form).filter(Form.type_id == 2)
>>> schema = {
...     'type': 'object',
...     'id_property': 'id',
...     'properties': {
...          'id': {
...              'type': 'integer'
...          },
...          'foo': {
...              'type': 'array',
...              'items': {
...                  'bar': {
...                      'type': 'integer'
...                  },
...                  'baz': {
...                      'type': 'string'
...                  }
...              }
...          },
...     }
... }
>>> create_json_view = CreateJSONView('my_view', q, Form.data, schema,
...                                   use_materialized_view=True)
>>> session.execute(create_json_view)
```

A view containing all values for the array property will be created named
according to the pattern `<view_name>_<property_name>`.

```sql
=# SELECT my_view."data.id", my_view_foo."foo.bar", my_view_foo."foo.baz"
     FROM my_view LEFT JOIN my_view_foo ON my_view.forms_id =
         my_view_foo.parent_id;
 data.id | foo.bar | foo.baz 
---------+---------+----------
       1 |       5 | spam 
       1 |       7 | foobar 
       2 |       6 | hjkl
       2 |       8 | foobar
       3 |         |
```

Arrays of objects within arrays of objects are not supported.

The array view uses a materialized view under the hood to enable queries to be
backed by functional indexes due to limitations of JSON indexing functions in
SQL databases.  To avoid using a materialized view and lose the benefits of
indexes on the array data, pass `use_materialized_view=False` to
`CreateJSONView`.

API
--

```python
class CreateJSONView(DDLElement):
    """
    An SQLAlchemy DDL element that constructs a standard view of JSON properties
    and creates associated partial indexes that back the view.

    Arguments:

    name -- the view name as a string
    query -- an SQLAlchemy Query indicating the table to select from and filters
    to apply
    json_column -- the SQLAlchemy Column of the table to draw data from
    json_schema -- a JSON Schema object that defines the structure of the JSON
        column
    extract_date_parts -- a list of fields corresponding to SQL timestamp
        subfields (e.g. 'year', 'month', 'day') to create as separate calculated
        columns for each datetime or date column in the JSON.  Date part columns will
        be named as <datetime_column>_year, etc.

        See
        http://www.postgresql.org/docs/8.1/static/functions-datetime.html#FUNCTIONS-DATETIME-EXTRACT
    indexes -- create indexes for the columns in the view (Default: True)
    replace -- drop the existing view with this name (Default: False)
    drop_existing_indexes -- drop existing indexes backing the view (Default: False)
    use_materialialized_view -- use a materialized instead of non-materialized
        view for intermediate views when creating an additional view of a
        JSON property containing an array of objects (Default: False)
    """
    def __init__(self, name, query, json_column, json_schema,
                 extract_date_parts=None, indexes=True, replace=False,
                 drop_existing_indexes=False, use_materialized_view=False):
```


### JSON Schema support

The `oneOf`, `allOf`, and `anyOf` fields are supported. (They have no
special interpretation from the perspective of this tool.)

Support for defining objects as arrays of simple values does not yet exist.

All properties must have a single type defined.

License
--

Copyright 2014-2020 Michael White

Released under the MIT License. See LICENSE.txt.
