#Given an SQLAlchemy Query on a table which has a JSON column and a JSON
#Schema of the data for that column, create a view with that query and with
#"foo.bar.baz" columns for the JSON properties, creating partial indexes
#limited to the conditions of the query for each JSON property.

import os
import operator
import itertools
from collections import namedtuple

from sqlalchemy import func, inspection
from sqlalchemy.schema import Index, Column
from sqlalchemy.orm import defer
from sqlalchemy.sql.ddl import CreateIndex, DDLElement
from sqlalchemy.ext.compiler import compiles

from .util import (CreateView, visit_create_view, compile_element, short_hash,
        merge_dicts)

__all__ = [
    'install_plv8_json',
    'CreateJSONView',
    'InvalidJSONSchemaError',
    'JSONSchemaConflict'
]

class InvalidJSONSchemaError(Exception):
    pass

class JSONSchemaConflict(Exception):
    pass

def install_plv8_json(engine):
    _install(engine, 'plv8_json.sql')

def install_plv8_json_postgis(engine):
    _install(engine, 'plv8_json_postgis.sql')

def _install(engine, path):
    conn = engine.connect()
    t = conn.begin()
    dirname = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(dirname, 'sql', path)) as f:
        sql = f.read()
        conn.execute(sql)
    t.commit()
    conn.close()


class CreateJSONView(DDLElement):
    def __init__(self, name, query, json_column, json_schema, indexes=True,
                 replace=False, drop_existing_indexes=False):
        self.name = name
        self.query = query
        self.json_column = json_column
        self.json_schema = json_schema
        self.indexes = indexes
        self.replace = replace
        self.drop_existing_indexes = False


@compiles(CreateJSONView)
def visit_create_json_view(element, ddlcompiler, **kwargs):
    name = element.name
    base_query = element.query
    json_column = element.json_column
    json_schema = element.json_schema

    columns = []
    for path, property in get_properties(json_schema):
        if isinstance(property, Array):
            pass

        columns.append(property.json_func(json_column, path).\
                label("%s.%s" % (json_column.name, path)))


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

    sqls = []
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
    where_hash = short_hash(where_sql)

    tablename = inspection.inspect(query).selectable._froms[0].name
    compiled_expr = compile_element(expr, query.session.bind.dialect)
    expr_hash = short_hash(compiled_expr)
    return "%s_%s_%s" % (tablename, where_hash, expr_hash)


class Type(object):
    json_func = None
    json_array_func = None

    def __init__(self, enum=None, title=None):
        self.enum = enum
        self.title = title

class String(Type):
    json_func = func.json_string
    json_array_func = func.json_string_array

class Decimal(Type):
    json_func = func.json_decimal
    json_array_func = func.json_decimal_array

class Float(Type):
    json_func = func.json_float
    json_array_func = func.json_float_array

class Integer(Type):
    json_func = func.json_int
    json_array_func = func.json_int_array

class Boolean(Type):
    json_func = func.json_bool
    json_array_func = func.json_bool_array

class DateTime(Type):
    json_func = func.json_datetime
    json_array_func = func.json_datetime_array

class DateTimeNoTZ(Type):
    json_func = func.json_datetime_no_tz
    json_array_func = func.json_datetime_no_tz_array

class Date(Type):
    json_func = func.json_date
    json_array_func = func.json_date_array

#class Time(Type):
    #json_func = func.json_time
    #json_array_func = func.json_time_array

class Geopoint(Type):
    json_func = func.json_geopoint
    json_array_func = func.json_geopoint_array


ArrayProperty = namedtuple('ArrayProperty', ['items'])
Array = namedtuple('Array', ['items'])


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
                obj = Array(schema['items'])
            else:
                # todo:
                obj = ArrayProperty(itemtype, schema['items'].get('enum', []))
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
            obj = our_type(schema.get('enum'), schema.get('title'))

        yield (path, obj)


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

