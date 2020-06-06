#Given an SQLAlchemy Query on a table which has a JSON column and a JSON
#Schema of the data for that column, create a view with that query and with
#"foo.bar.baz" columns for the JSON properties, creating partial indexes
#limited to the conditions of the query for each JSON property.

import os
import operator
import itertools
from collections import namedtuple

from sqlalchemy import func, inspection, cast
from sqlalchemy import types
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import Index, Column
from sqlalchemy.orm import defer
from sqlalchemy.sql.ddl import CreateIndex, DDLElement
from sqlalchemy.ext.compiler import compiles

from .util import (SQL_FUNCTIONS, CreateView, visit_create_view,
                   compile_element, short_hash, merge_dicts)

try:
    GEOMETRY_IMPORTED = True
    from geoalchemy2.types import Geometry
except ImportError:
    GEOMETRY_IMPORTED = False
    Geometry = types.String
    

__all__ = [
    'CreateJSONView',
    'InvalidJSONSchemaError',
    'JSONSchemaConflict',
    'GEOMETRY_IMPORTED'
]


class InvalidJSONSchemaError(Exception):
    pass


class JSONSchemaConflict(Exception):
    pass


class CreateJSONView(DDLElement):
    """
    extract_date_parts -- a list of fields corresponding to SQL timestamp
    subfields to create as separate calculated columns.
    http://www.postgresql.org/docs/8.1/static/functions-datetime.html#FUNCTIONS-DATETIME-EXTRACT
    """
    def __init__(self, name, query, json_column, json_schema,
            extract_date_parts=None, indexes=True,
            replace=False, drop_existing_indexes=False):
        self.name = name
        self.query = query
        self.json_column = json_column
        self.json_schema = json_schema
        self.extract_date_parts = extract_date_parts
        self.indexes = indexes
        self.replace = replace
        self.drop_existing_indexes = False

        self.columns = None


@compiles(CreateJSONView)
def visit_create_json_view(element, ddlcompiler, **kwargs):
    name = element.name
    base_query = element.query
    json_column = element.json_column
    json_schema = element.json_schema
    extract_date_parts = element.extract_date_parts

    columns = []
    properties = get_properties(json_schema)
    element.columns = []
    for p in properties:
        if isinstance(p, Array):
            continue

        json_column_type = json_column.prop.columns[0].type
        if (
            isinstance(json_column_type, postgresql.JSONB) or 
            json_column_type == postgresql.JSONB
        ):
            json_func = p.jsonb_func
        else:
            json_func = p.json_func

        column = json_func(json_column, *p.path.split('.'))
        column_label = "%s.%s" % (json_column.name, p.path)

        if extract_date_parts and isinstance(p, (DateTime, DateTimeNoTZ, Date)):
            for part in extract_date_parts:
                part_column = func.date_part_immutable(part, column)
                columns.append(part_column.label(
                    "%s_%s" % (column_label, part)))
            p.date_parts = extract_date_parts

        columns.append(column.label(column_label))
        p.column_name = column_label
        element.columns.append(p)

    # Don't include columns in the the view that are part of an == condition in
    # the WHERE clause.
    for where in inspection.inspect(base_query).whereclause:
        if where.operator == operator.eq:
            for expr in [where.left, where.right]:
                if isinstance(expr, Column):
                    # Curiously, we can't seem to defer the Column object
                    # expr._Annotated__element itself.
                    column_name = expr._Annotated__element.name
                    base_query = base_query.options(defer(column_name))

    # Don't include the JSON column in the view.
    base_query = base_query.options(defer(json_column)) 
    query = base_query.add_columns(*columns)

    create_view = CreateView(name, query, replace=element.replace)
    create_indexes = CreateIndexes(
            base_query, columns, drop_existing=element.drop_existing_indexes)

    view_sql = visit_create_view(create_view, ddlcompiler)
    indexes_sql = visit_create_indexes(create_indexes, ddlcompiler)

    return view_sql + "; " + indexes_sql


class CreateIndexes(DDLElement):
    def __init__(self, query, expressions, concurrently=True,
                 drop_existing=False):
        self.query = query
        self.expressions = expressions
        #self.concurrently = concurrently
        self.drop_existing = drop_existing


@compiles(CreateIndexes)
def visit_create_indexes(element, ddlcompiler, **kw):
    query = element.query
    selectable = inspection.inspect(query).selectable

    sqls = [SQL_FUNCTIONS]
    for expr in element.expressions:
        name = get_partial_index_name(expr, query)
        # Postgres should be smart enough to use single-column indexes even if
        # there are additional where clauses, see
        # http://www.postgresql.org/docs/8.3/static/indexes-bitmap-scans.html
        # To create a multiple-column index, prepend
        # inspect(query).whereclause.
        index = Index(name, expr, postgresql_where=selectable._whereclause)
        create_index_sql = ddlcompiler.visit_create_index(CreateIndex(index))

        #if element.concurrently:
            #create_index_sql = create_index_sql.replace(
                #'CREATE INDEX', 'CREATE INDEX CONCURRENTLY')

        if element.drop_existing:
            sql = "DROP INDEX IF EXISTS %(name)s;"
            # todo: concurrently
        else:
            sql = ""

        sql += """;
        DO $$

        BEGIN

        IF NOT EXISTS (
            SELECT 1
            FROM   pg_class c
            WHERE  c.relname = '%(name)s'
            ) THEN

            %(create_index)s;
        END IF;

        END$$;
        """ % {
            'name': name,
            'create_index': create_index_sql
        }
        sqls.append(sql)

    return ";".join(sqls)


def get_partial_index_name(expr, query):
    sql = compile_element(query.statement, query.session.bind.dialect)
    # hack hack hack
    where_sql = sql[sql.find('WHERE'):]
    where_hash = short_hash(where_sql.encode('utf-8'))

    tablename = inspection.inspect(query).selectable._froms[0].name
    compiled_expr = compile_element(expr, query.session.bind.dialect)
    expr_hash = short_hash(compiled_expr.encode('utf-8'))
    return "%s_%s_%s" % (tablename, where_hash, expr_hash)


class Object(object):
    jsonb_func = None
    json_func = None

    def __init__(self, path, enum=None, title=None):
        self.path = path
        self.enum = enum
        self.title = title

    def __repr__(self):
        return "%s(path=%r, enum=%r, title=%r)" % (
                self.__class__.__name__, self.path, self.enum, self.title)

    def __eq__(self, other):
        return (type(self) == type(other) and self.path == other.path and
                self.enum == other.enum and self.title == other.title)

class String(Object):
    jsonb_func = func.jsonb_string
    json_func = func.json_string

class Decimal(Object):
    jsonb_func = func.jsonb_decimal
    json_func = func.json_decimal

class Float(Object):
    jsonb_func = func.jsonb_float
    json_func = func.json_float

class Integer(Object):
    jsonb_func = func.jsonb_int
    json_func = func.json_int

class Boolean(Object):
    jsonb_func = func.jsonb_bool
    json_func = func.json_bool

class DateTime(Object):
    jsonb_func = func.jsonb_datetime_tz
    json_func = func.json_datetime_tz

class DateTimeNoTZ(Object):
    jsonb_func = func.jsonb_datetime
    json_func = func.json_datetime

class Date(Object):
    jsonb_func = func.jsonb_date
    json_func = func.json_date

#class Time(Object):
    #json_type = types.Time

class Geopoint(Object):
    jsonb_func = func.jsonb_geopoint
    json_func = func.json_geopoint


ArrayProperty = namedtuple('ArrayProperty', ['path', 'items'])
Array = namedtuple('Array', ['path', 'items'])


def get_properties(schema):
    return list(iter_properties(schema))


def iter_properties(schema, path=''):
    type = schema.get('type')
    format = schema.get('format')

    if type is None or isinstance(type, (list, tuple)) or type == 'any':
        raise InvalidJSONSchemaError("Unsupported type value: %s." % type)
    elif type == 'object':
        for name, value in get_schema_properties(schema).items():
            new_path = path + ('.' if path else '') + name
            for x in iter_properties(value, new_path):
                yield x
    else:
        if type == 'array':
            itemtype = schema['items']['type']
            if itemtype == 'object':
                obj = Array(path, schema['items'])
            else:
                # todo:
                obj = ArrayProperty(path, itemtype, schema['items'].get('enum', []))
        else:
            if type == 'number':
                our_type = Float
            elif type == 'string' and format == 'decimal':
                our_type = Decimal
            elif type == 'integer':
                our_type = Integer
            elif type == 'boolean':
                our_type = Boolean
            elif type == 'string' and format == 'date-time':
                our_type = DateTime
            elif type == 'string' and format == 'date-time-no-tz':
                our_type = DateTimeNoTZ
            elif type == 'string' and format == 'date':
                our_type = Date
            #elif type == 'string' and format == 'time':
                #our_type = Time
            elif type == 'string' and format == 'geopoint':
                our_type = Geopoint
            elif type == 'string':
                our_type = String
            else:
                raise InvalidJSONSchemaError("Unrecognized (type, format):"
                        + " (%s, %s)" % (type, format))
            obj = our_type(path, schema.get('enum'), schema.get('title'))

        yield obj


def get_schema_properties(schema):
    # todo: apply this to internal schema nodes too (for current known uses, it
    # shouldn't really matter, since we're not generating any schemas that use
    # the quantifiers, and we're only using the quantifiers to combine
    # top-level schemas.
    properties_list = [schema.get('properties', {})]
    properties_list = list(itertools.chain(properties_list,
        *map(lambda key: [ v.get('properties', {})
                           for v in schema.get(key, [])],
             ['oneOf', 'allOf', 'anyOf'])
    ))

    if len(properties_list):
        for d in properties_list[1:]:
            try:
                properties_list[0] = merge_dicts(
                        properties_list[0], d,
                        keys=['type', 'format', 'properties'])
            except Exception as e:
                raise JSONSchemaConflict("A conflict prevented two schemas from "
                        "being merged.  Original exception: " + str(e))

    if not properties_list[0]:
        raise InvalidJSONSchemaError("Properties can't be empty.")

    return properties_list[0]

