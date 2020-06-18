import hashlib
import base64

from sqlalchemy.inspection import inspect
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.ddl import DDLElement
from sqlalchemy.sql.compiler import SQLCompiler

from psycopg2.extensions import adapt as sqlescape


__all__ = [
    'SQL_FUNCTIONS',
    'CreateView',
    'CreateIndexes',
    'compile_element',
    'short_hash',
    'merge_dicts'
]


SQL_FUNCTIONS = """
CREATE OR REPLACE FUNCTION
date_part_immutable(text, anyelement) RETURNS DOUBLE PRECISION 
    AS 'SELECT date_part($1, $2)'
    LANGUAGE SQL
    IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION jsonb_string(data jsonb, VARIADIC path text[]) RETURNS TEXT AS $$
    BEGIN
        RETURN jsonb_extract_path_text(data, VARIADIC path)::text;
    EXCEPTION WHEN OTHERS THEN
        RETURN null;
    END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION json_string(data json, VARIADIC path text[]) RETURNS TEXT AS $$
    BEGIN
        RETURN json_extract_path_text(data, VARIADIC path)::text;
    EXCEPTION WHEN OTHERS THEN
        RETURN null;
    END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION jsonb_int(data jsonb, VARIADIC path text[]) RETURNS INTEGER AS $$
    BEGIN
        RETURN jsonb_extract_path_text(data, VARIADIC path)::int;
    EXCEPTION WHEN OTHERS THEN
        RETURN null;
    END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION json_int(data json, VARIADIC path text[]) RETURNS INTEGER AS $$
    BEGIN
        RETURN json_extract_path_text(data, VARIADIC path)::int;
    EXCEPTION WHEN OTHERS THEN
        RETURN null;
    END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION jsonb_float(data jsonb, VARIADIC path text[]) RETURNS FLOAT AS $$
    BEGIN
        RETURN jsonb_extract_path_text(data, VARIADIC path)::float;
    EXCEPTION WHEN OTHERS THEN
        RETURN null;
    END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION json_float(data json, VARIADIC path text[]) RETURNS FLOAT AS $$
    BEGIN
        RETURN json_extract_path_text(data, VARIADIC path)::float;
    EXCEPTION WHEN OTHERS THEN
        RETURN null;
    END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION jsonb_decimal(data jsonb, VARIADIC path text[]) RETURNS DECIMAL AS $$
    BEGIN
        RETURN jsonb_extract_path_text(data, VARIADIC path)::text::decimal;
    EXCEPTION WHEN OTHERS THEN
        RETURN null;
    END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION json_decimal(data json, VARIADIC path text[]) RETURNS DECIMAL AS $$
    BEGIN
        RETURN json_extract_path_text(data, VARIADIC path)::text::decimal;
    EXCEPTION WHEN OTHERS THEN
        RETURN null;
    END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION jsonb_bool(data jsonb, VARIADIC path text[]) RETURNS BOOLEAN AS $$
    BEGIN
        RETURN jsonb_extract_path(data, VARIADIC path)::bool;
    EXCEPTION WHEN OTHERS THEN
        RETURN null;
    END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION json_bool(data json, VARIADIC path text[]) RETURNS BOOLEAN AS $$
    BEGIN
        RETURN json_extract_path(data, VARIADIC path)::bool;
    EXCEPTION WHEN OTHERS THEN
        RETURN null;
    END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION jsonb_datetime(data jsonb, VARIADIC path text[]) RETURNS TIMESTAMP AS $$
    BEGIN
        RETURN jsonb_extract_path_text(data, VARIADIC path)::timestamp;
    EXCEPTION WHEN OTHERS THEN
        RETURN null;
    END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION json_datetime(data json, VARIADIC path text[]) RETURNS TIMESTAMP AS $$
    BEGIN
        RETURN json_extract_path_text(data, VARIADIC path)::timestamp;
    EXCEPTION WHEN OTHERS THEN
        RETURN null;
    END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION jsonb_datetime_tz(data jsonb, VARIADIC path text[]) RETURNS TIMESTAMP WITH TIME ZONE AS $$
    BEGIN
        RETURN jsonb_extract_path_text(data, VARIADIC path)::timestamptz;
    EXCEPTION WHEN OTHERS THEN
        RETURN null;
    END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION json_datetime_tz(data json, VARIADIC path text[]) RETURNS TIMESTAMP WITH TIME ZONE AS $$
    BEGIN
        RETURN json_extract_path_text(data, VARIADIC path)::timestamptz;
    EXCEPTION WHEN OTHERS THEN
        RETURN null;
    END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION jsonb_date(data jsonb, VARIADIC path text[]) RETURNS DATE AS $$
    BEGIN
        RETURN jsonb_extract_path_text(data, VARIADIC path)::date;
    EXCEPTION WHEN OTHERS THEN
        RETURN null;
    END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION json_date(data json, VARIADIC path text[]) RETURNS DATE AS $$
    BEGIN
        RETURN json_extract_path_text(data, VARIADIC path)::date;
    EXCEPTION WHEN OTHERS THEN
        RETURN null;
    END;
$$ LANGUAGE plpgsql IMMUTABLE;

DO $do$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM   pg_extension e
        WHERE  e.extname = 'postgis'
    ) THEN
        CREATE OR REPLACE FUNCTION jsonb_geopoint(data jsonb, VARIADIC path text[]) RETURNS
        GEOMETRY AS $json_geopoint$
            BEGIN
                RETURN ST_MakePoint(
                    SPLIT_PART(
                        jsonb_extract_path_text(data, VARIADIC path)::text, 
                        ',', 1
                    )::float,
                    SPLIT_PART(
                        jsonb_extract_path_text(data, VARIADIC path)::text,
                        ',', 2
                    )::float
                );
            EXCEPTION WHEN OTHERS THEN
                RETURN null;
            END;
        $json_geopoint$ LANGUAGE plpgsql IMMUTABLE;
        
        CREATE OR REPLACE FUNCTION json_geopoint(data json, VARIADIC path text[]) RETURNS
        GEOMETRY AS $json_geopoint$
            BEGIN
                RETURN ST_MakePoint(
                    SPLIT_PART(
                        json_extract_path_text(data, VARIADIC path)::text, 
                        ',', 1
                    )::float,
                    SPLIT_PART(
                        json_extract_path_text(data, VARIADIC path)::text,
                        ',', 2
                    )::float
                );
            EXCEPTION WHEN OTHERS THEN
                RETURN null;
            END;
        $json_geopoint$ LANGUAGE plpgsql IMMUTABLE;
    END IF;
END$do$;

"""


class CreateView(DDLElement):
    def __init__(self, name, query, replace=False, materialized=False):
        self.name = name
        self.query = query
        self.replace = replace
        self.materialized = materialized


@compiles(CreateView)
def visit_create_view(element, ddlcompiler, **kw):
    name = element.name
    materialized = element.materialized
    select = inspect(element.query).selectable

    if element.replace:
        sql = 'DROP VIEW IF EXISTS %s;\n' % element.name
    else:
        sql = ''

    sql += "%s VIEW %s AS %s;" % (
        'CREATE' if not materialized else 'CREATE MATERIALIZED',
        name,
        ddlcompiler.sql_compiler.process(select, literal_binds=True)
    )

    if materialized:
        sql += "\nREFRESH MATERIALIZED VIEW %s;" % element.name

    return SQL_FUNCTIONS + ';' + sql


def short_hash(str):
    hash = base64.urlsafe_b64encode(
            hashlib.sha1(str).digest()[:10])[:-2].decode('utf-8')
    hash = hash.replace('-', '')
    return hash

def compile_element(element, dialect):
    #statement = query.statement
    comp = SQLCompiler(dialect, element)
    comp.compile()
    enc = dialect.encoding
    params = {}
    for k,v in comp.params.items():
        if isinstance(v, str):
            v = v.encode(enc)
        params[k] = sqlescape(v)

    return comp.string % params
    return (comp.string.encode(enc) % params).decode(enc)


def merge_dicts(dict1, dict2, keys=None, merge_lists=True):
    """
    Recursively merge dict2 into dict1, returning the new dict1.

    keys -- keys to care about when checking for conflicts.  Other keys take
        the value from the second dict.
    merge_lists -- whether to ignore conflicts between list values and merge
        instead
    """
    for key, value in dict1.items():
        if key in dict2:
            d2_value = dict2[key]
            if value != d2_value and (keys is None or key not in keys):
                if isinstance(value, list) and merge_lists:
                    value = value + d2_value
                elif isinstance(value, dict):
                    value = merge_dicts(value, d2_value, keys, merge_lists)
                else:
                    raise Exception("Dicts had conflicting values for %s: %s, %s" % (
                        key, value, d2_value))
            else:
                value = d2_value
            dict1[key] = value

    for key, value in dict2.items():
        dict1.setdefault(key, value)
    return dict1

