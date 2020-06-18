from collections import namedtuple
from dataclasses import dataclass
import itertools
import os
import operator
import re
from typing import Any

import sqlalchemy
from sqlalchemy import func, cast, Table
from sqlalchemy import types
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import Index, Column
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import defer, mapper
# from sqlalchemy.orm.session import Session
from sqlalchemy.sql.ddl import CreateIndex, DDLElement
from sqlalchemy.sql.selectable import Alias 
# from sqlalchemy.sql import expression
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.ext.declarative import declarative_base

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
    An SQLAlchemy DDL element that constructs a standard view of JSON properties
    and creates associated partial indexes that back the view.

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
        self.name = name
        self.query = query
        self.json_column = json_column
        self.json_schema = json_schema
        self.extract_date_parts = extract_date_parts
        self.indexes = indexes
        self.replace = replace
        self.drop_existing_indexes = False
        self.use_materialized_view = use_materialized_view

        self.columns = None


@compiles(CreateJSONView)
def visit_create_json_view(element, ddlcompiler, **kwargs):
    query = element.query
    json_column = element.json_column
    view_name = element.name
    use_materialized_view = element.use_materialized_view

    columns = []
    child_view_sqls = []

    properties = get_properties(element.json_schema)
    element.columns = []

    for prop in properties:
        prop.query = query
        column, sql = process_property(ddlcompiler, view_name, query, columns,
                                       prop, json_column, element,
                                       use_materialized_view=use_materialized_view)
        if column is not None:
            columns.append(column)
            element.columns.append(prop)
        else:
            child_view_sqls.append(sql)

    view_and_indexes_sql = get_view_and_indexes_sql(
        ddlcompiler,
        view_name, query, json_column, None, columns,
        element.drop_existing_indexes, element.replace,
        use_materialized_view=False)

    return "%s;\n %s" % (
        view_and_indexes_sql,
        ';\n'.join(child_view_sqls)
    )


def get_view_and_indexes_sql(ddlcompiler, view_name, query, json_column,
                             parent_id_property, columns, drop_existing_indexes,
                             replace_view, is_array_view=False, array_prop=None,
                             use_materialized_view=False):

    json_column_type = json_column.prop.columns[0].type
    if (
        isinstance(json_column_type, postgresql.JSONB) or 
        json_column_type == postgresql.JSONB
    ):
        json_type = 'jsonb'
    else:
        json_type = 'json'

    # Don't include columns in the view that are part of an == condition in the
    # WHERE clause.
    for where in inspect(query).whereclause:
        if where.operator == operator.eq:
            for expr in [where.left, where.right]:
                if isinstance(expr, Column):
                    # Curiously, we can't seem to defer the Column object
                    # expr._Annotated__element itself.
                    column_name = expr._Annotated__element.name
                    query = query.options(defer(column_name))

    if not is_array_view:
        query = query.add_columns(*columns)
        # Don't include the JSON column in the view.
        # query = query.options(defer(json_column)) 

        subquery = None
        json_view_sql = ''
    else:
        json_extract_path = getattr(func, '%s_extract_path' % json_type)
        json_array_elements = getattr(func, '%s_array_elements' % json_type)
        array_json_label = "%s_json" % array_prop.path
        array_json_column = json_array_elements(
            json_extract_path(json_column, *array_prop.path.split('.'))
        ).label(array_json_label)
        query = query.add_column(array_json_column)
        # query = query.options(defer(json_column)) 
        class_ = query._query_entity_zero().mapper.class_
        primary_key = inspect(class_).primary_key[0].name
        query = query.options(defer(primary_key))

        tablename = inspect(query).selectable._froms[0].name
        json_view_name = "%s_%s" % (tablename, array_json_label)
        json_view_sql = visit_create_view(
            CreateView(json_view_name, query, replace=replace_view,
                       materialized=use_materialized_view),
            ddlcompiler
        )
       
        subquery = query.subquery()
        subquery.query = query

        Base = declarative_base()

        json_view_json_col_name = "%s_%s" % (tablename, json_column.key)

        view_table = Table(
            json_view_name, Base.metadata,
            # Dummy primary key.
            Column('id', sqlalchemy.Integer, primary_key=True),
            Column(array_json_label, json_column_type),
            Column(json_view_json_col_name, json_column_type)
        )

        class View(object):
            pass

        mapper(View, view_table, properties={
            'id': view_table.c.id,
            array_json_label: view_table.c[array_json_label],
            json_view_json_col_name: view_table.c[json_view_json_col_name]
        })

        query = query.session.query(View)\
            .options(defer(View.id))\
            .options(defer(getattr(View, json_view_json_col_name)))\
            .options(defer(getattr(View, array_json_label)))\
            # .select_entity_from(subquery)

        new_columns = []
        for column in columns:
            if column.path.startswith(array_prop.path):
                new_column = column.json_func(
                    getattr(View, array_json_label),
                    *column.path.split('.')[1:]
                ).label(column.path)
                query = query.add_columns(new_column)
            else:
                new_column = column.json_func(
                    getattr(View, json_view_json_col_name),
                    *column.path.split('.')
                ).label(PARENT_ID_LABEL)
                query = query.add_columns(new_column)
            new_columns.append(new_column)
        columns = new_columns

    query.subquery = subquery

    create_view = CreateView(view_name, query, replace=replace_view)
    view_sql = visit_create_view(create_view, ddlcompiler)

    if not use_materialized_view:
        create_indexes = CreateIndexes(
                query, columns, drop_existing=drop_existing_indexes)
        indexes_sql = visit_create_indexes(create_indexes, ddlcompiler)
    else:
        indexes_sql = ''

    return ';\n'.join([
        json_view_sql,
        view_sql,
        indexes_sql
    ])


def process_property(ddlcompiler, view_name, query, columns, prop, json_column,
                     element, path=None, is_array_property=False,
                     use_materialized_view=False):
    path = prop.path

    if isinstance(prop, Array):
        properties = list(iter_properties(prop.items, path=prop.path))

        parent_id_property = prop.parent_id_property
        parent_schema = prop.parent_schema

        if parent_id_property is None:
            raise InvalidJSONSchemaError("Missing id_property")

        view_name = "%s_%s" % (view_name, path.replace('.', '__'))
        schema = parent_schema['properties'][parent_id_property]
       
        type = schema.get('type')
        obj_type = get_object_type(type, schema.get('format'))
        obj = obj_type(''.join(path.split('.')[:-1] + [parent_id_property]),
                       schema.get('enum'), schema.get('title'),
                       parent_id_property=None)
        obj.is_parent_id = True

        properties.append(obj)

        prop_columns = []

        for prop_ in properties:
            column, _ = process_property(ddlcompiler, view_name, query, columns,
                                      prop_, json_column, element,
                                      path=prop_.path, is_array_property=True)
            prop_columns.append((prop_, column))
        
        sql = get_view_and_indexes_sql(ddlcompiler, view_name, query,
                                       json_column, parent_id_property,
                                       list(map(operator.itemgetter(1),
                                                prop_columns)), True, True,
                                       is_array_view=True,
                                       array_prop=prop,
                                       use_materialized_view=use_materialized_view)

        return None, sql

    json_column_type = json_column.prop.columns[0].type
    if (
        isinstance(json_column_type, postgresql.JSONB) or 
        json_column_type == postgresql.JSONB
    ):
        json_func = prop.jsonb_func
    else:
        json_func = prop.json_func

    if is_array_property:
        column_path = '.'.join(path.split('.')[1:])
    else:
        column_path = path

    column = json_func(json_column, *column_path.split('.'))
    column_label = (PARENT_ID_LABEL if prop.is_parent_id else "%s.%s" %
                    (json_column.name, column_path))
    
    extract_date_parts = element.extract_date_parts
    if extract_date_parts and isinstance(prop, (DateTime, DateTimeNoTZ, Date)):
        for part in extract_date_parts:
            part_column = func.date_part_immutable(part, column)
            columns.append(part_column.label("%s_%s" % (column_label, part)))
        prop.date_parts = extract_date_parts

    column = column.label(column_label)
    column.path = path
    column.json_func = json_func

    prop.column_name = column_label

    return column, ''


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
    selectable = inspect(query).selectable
    # subquery_query = query.subquery.query if query.subquery else None
    # subquery_query_selectable = inspect(query).selectable

    sqls = [SQL_FUNCTIONS]
    for expr in element.expressions:
        name = get_partial_index_name(expr, query)
        # Postgres should be smart enough to use single-column indexes even if
        # there are additional where clauses, see
        # http://www.postgresql.org/docs/8.3/static/indexes-bitmap-scans.html
        # To create a multiple-column index, prepend
        # inspect(query).whereclause.
        # where = (subquery_query_selectable._whereclause if subquery_query else
                 # selectable._whereclause)
        index = Index(name, expr, postgresql_where=selectable._whereclause)
        try:
            create_index_sql = ddlcompiler.visit_create_index(CreateIndex(index))
        except Exception:
            import pdb; pdb.set_trace()


        #if element.concurrently:
            #create_index_sql = create_index_sql.replace(
                #'CREATE INDEX', 'CREATE INDEX CONCURRENTLY')

        if element.drop_existing:
            sql = "DROP INDEX IF EXISTS %(name)s;" % {
                'name': name
            }
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

    return ";\n".join(sqls)


def get_partial_index_name(expr, query):
    sql = compile_element(query.statement, query.session.bind.dialect)
    # hack hack hack
    where_sql = sql[sql.find('WHERE'):]
    where_hash = short_hash(where_sql.encode('utf-8'))

    if query.subquery is not None:
        selectable = inspect(query.subquery).selectable
        selectable = selectable.original
        tablename = selectable._froms[0].name
    else:
        tablename = inspect(query).selectable._froms[0].name
    compiled_expr = compile_element(expr, query.session.bind.dialect)
    expr_hash = short_hash(compiled_expr.encode('utf-8'))
    return "%s_%s_%s" % (tablename, where_hash, expr_hash)


class Object(object):
    jsonb_func = None
    json_func = None

    def __init__(self, path, enum=None, title=None, parent_id_property=None):
        self.path = path
        self.enum = enum
        self.title = title
        self.parent_id_property = parent_id_property
        self.is_parent_id = False

    def __repr__(self):
        return "%s(path=%r, enum=%r, title=%r, parent_id_property=%r)" % (
            self.__class__.__name__, self.path, self.enum, self.title,
            self.parent_id_property)

    def __eq__(self, other):
        return (type(self) == type(other) and self.path == other.path and
                self.enum == other.enum and self.title == other.title and
                self.parent_id_property == other.parent_id_property)

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


@dataclass
class Array:
    path: Any
    items: Any
    query: Any = None
    is_parent_id: Any = False


ArrayProperty = namedtuple('ArrayProperty', ['path', 'items'])


PARENT_ID_LABEL = 'parent_id'


def get_properties(schema):
    return list(iter_properties(schema))


def iter_properties(schema, path='', parent_id_property=None):
    type = schema.get('type')

    if type is None or isinstance(type, (list, tuple)) or type == 'any':
        raise InvalidJSONSchemaError("Unsupported type value: %s" % type)
    elif type == 'object':
        parent_id_property = schema.get('id_property')

        for name, value in get_schema_properties(schema).items():
            new_path = path + ('.' if path else '') + name
            for x in iter_properties(value, path=new_path,
                                     parent_id_property=parent_id_property):
                x.parent_id_property = parent_id_property
                x.parent_schema = schema

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
            obj_type = get_object_type(type, schema.get('format'))
            obj = obj_type(path, schema.get('enum'), schema.get('title'),
                           parent_id_property=parent_id_property)

        yield obj


def get_object_type(type, format):
    if type == 'number':
        return Float
    elif type == 'string' and format == 'decimal':
        return Decimal
    elif type == 'integer':
        return Integer
    elif type == 'boolean':
        return Boolean
    elif type == 'string' and format == 'date-time':
        return DateTime
    elif type == 'string' and format == 'date-time-no-tz':
        return DateTimeNoTZ
    elif type == 'string' and format == 'date':
        return Date
    #elif type == 'string' and format == 'time':
        #our_type = Time
    elif type == 'string' and format == 'geopoint':
        return Geopoint
    elif type == 'string':
        return String
    else:
        raise InvalidJSONSchemaError("Unrecognized (type, format):"
                + " (%s, %s)" % (type, format))

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

