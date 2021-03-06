import pytest
import random
import json
import re
import copy
import datetime
import dateutil.parser
from functools import partial
from decimal import Decimal

from sqlalchemy import *
from sqlalchemy import exc
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects import postgresql

from jsonalchemy import (CreateJSONView as _CreateJSONView,
        InvalidJSONSchemaError, JSONSchemaConflict)

CreateJSONView = partial(_CreateJSONView, replace=True)

def parse_date(string):
    if len(string) == 16:
        string += 'Z'

    datetime = dateutil.parser.parse(string)
    return datetime

SCHEMAS = {
    'string': {
        "title": "A String",
        "type": "string",
        "enum": ["unicorns", "penguins", "pythons"],
        "_python_type": str
    },
    'decimal': {
        "title": "A Decimal",
        "type": "string",
        "format": "decimal",
        "enum": list(map(Decimal, ["1.0", "2.5", "10.99234234"])),
        "_python_type": Decimal
    },
    'float': {
        "title": "A Float",
        "type": "number",
        "enum": [1.0, 2.5, 10.99],
        "_python_type": float
    },
    'integer': {
        "title": "An Integer",
        "type": "integer",
        "enum": [1, 2, 3, 4],
        "_python_type": int
    },
    'boolean': {
        "title": "A Boolean",
        "type": "boolean",
        "enum": [True, False],
        "_python_type": bool
    },
    'datetime': {
        "title": "A Datetime",
        "type": "string",
        "format": "date-time",
        "enum": list(map(parse_date, [
            "2007-04-05T14:31Z",
            "2005-03-02T12:30-02:00",
            "2005-04-05T17:45Z"
        ])),
        "_python_type": datetime.datetime
    },
    'datetime-no-tz': {
        "title": "A Datetime with no timezone",
        "type": "string",
        "format": "date-time",
        "enum": list(map(parse_date, [
            "2007-04-05T14:31",
            "2005-03-02T12:30",
            "2005-04-05T17:45"
        ])),
        "_python_type": datetime.datetime
    },
    'date': {
        "title": "A Date",
        "type": "string",
        "format": "date",
        "enum": list(map(lambda s: parse_date(s).date(), [
            "2007-04-05",
            "2005-03-02",
            "2005-04-05"
        ])),
        "_python_type": datetime.date
    },
    #'time': {
        #"title": "A Time",
        #"type": "string",
        #"format": "time",
        #"enum": ["14:31Z", "12:30-02:00", "17:45"]
    #},
    'geopoint': {
        "title": "A Geopoint",
        "type": "string",
        "format": "geopoint",
        "enum": [
            "-71.1043443253471, 42.3150676015829",
            "-72.1043443253471, 43.3150676015829",
            "-70.1043443253471, 44.3150676015829",
        ],
        "_python_type": object  # doesn't map back to a python type
    }
}

class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        elif isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()

        return json.JSONEncoder.default(self, obj)


@pytest.fixture(scope="module")
def engine(request):
    engine = create_engine(
        'postgresql://postgres:postgres@localhost/jsonalchemy_test',
        # echo=True
    )
    request.addfinalizer(lambda: engine.dispose())
    return engine


@pytest.fixture(scope="module")
def models(engine):
    Base = declarative_base()

    class Tenant(Base):
        __tablename__ = 'tenants'
        id = Column(Integer, primary_key=True)
        name = Column(String(200))

    class FormType(Base):
        __tablename__ = 'form_types'
        id = Column(Integer, primary_key=True)
        name = Column(String(200))

    class Form(Base):
        __tablename__ = 'forms'
        id = Column(Integer, primary_key=True)
        tenant_id = Column(Integer, ForeignKey('tenants.id'), index=True)
        tenant = relationship(Tenant, backref='forms')
        type_id = Column(Integer, ForeignKey('form_types.id'), index=True)
        type = relationship(FormType, backref='forms')
        data = Column(postgresql.JSONB)

    class foo(object):
        pass
    models = foo()
    models.Tenant = Tenant
    models.FormType = FormType
    models.Form = Form
    engine.execute("CREATE EXTENSION IF NOT EXISTS postgis")
    engine.execute("DROP TABLE IF EXISTS forms CASCADE")
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    engine.execute("INSERT INTO tenants (name) VALUES ('mike'), ('bob')")
    engine.execute("INSERT INTO form_types (name) VALUES ('type 1'), ('type 2'), "
                   "('type 3')")

    # test data set 1: all types, no nulls, no nesting
    data = []
    for i in range(10):
        data.append(dict((k, random.choice(v['enum'])) for k, v in
            SCHEMAS.items()))

    engine.execute("INSERT INTO forms (tenant_id, type_id, data) VALUES " +
        ",".join(["(1, 1, '%s')" % json.dumps(
                    foo, cls=JSONEncoder)
                  for foo in data]))

    # test data set 2: nesting, some missing, and some nulls!
    json_data = []
    for i in range(10000):
        data = {}
        if random.choice([True, False]):
            data['foo'] = {
                'bar': random.choice([1, 2, 5, None])
            }
        if random.choice([True, False]):
            data['eggs'] = {
                'spam': random.choice(["asdf", "hjkl", "baz"])
            }
        json_data.append(data)
    engine.execute("INSERT INTO forms (tenant_id, type_id, data) VALUES " +
        ",".join(["(1, 2, '%s')" % json.dumps(data, cls=JSONEncoder) 
                  for data in json_data]))

    # test data set 3: array of objects
    json_data = []
    for i in range(100):
        data = {
            'id': i,
            'array': [
                {
                    'baz': random.choice(range(4)),
                    'quux': random.choice([True, False])
                },
                {
                    'baz': random.choice(range(4)),
                    'quux': random.choice([True, False])
                }
            ]
        }
        json_data.append(data)
    engine.execute("INSERT INTO forms (tenant_id, type_id, data) VALUES " +
                   ",".join(["(1, 3, '%s')" % json.dumps(data, cls=JSONEncoder)
                             for data in json_data]))

    return models


@pytest.fixture(scope="module")
def session(engine):
    return sessionmaker(bind=engine)()


def test_basic_types(session, models):
    """Tests all basic types in a non-nested schema."""
    q = session.query(models.Form)\
            .filter(models.Form.tenant_id == 1, models.Form.type_id == 1)
    
    create_view = CreateJSONView('foo', q, models.Form.data, {
        'type': 'object',
        'properties': SCHEMAS
    })
    session.execute(create_view)

    result = list(session.execute('SELECT * from foo'))
    assert len(result)
    assert len(result[0]) == len(SCHEMAS) + 2

    for row in result:
        for k, v in [i for i in row.items() if i[0] not in ('forms_id',
                                                            'forms_data')]:
            prop = k.split('.')[1]
            python_type = SCHEMAS[prop]['_python_type']
            assert isinstance(v, python_type)
            if python_type != object:
                assert v in SCHEMAS[prop]['enum']


def test_date_part_columns_are_created(session, models):
    q = session.query(models.Form)\
            .filter(models.Form.tenant_id == 1, models.Form.type_id == 1)
    
    create_view = CreateJSONView('foo', q, models.Form.data, {
        'type': 'object',
        'properties': {
            'datetime': SCHEMAS['datetime']
        }
    }, extract_date_parts=['year', 'month', 'day'])
    session.execute(create_view)
    #result = list(session.execute(
        #'SELECT "data.datetime_year", "data.datetime_month", '
        #'"data.datetime_day" FROM foo'))
    result = list(session.execute("""SELECT
        "data.datetime_year", "data.datetime_month", "data.datetime_day"
    FROM foo"""))
    assert all(
        r["data.datetime_year"] in map(float, [2005, 2007]) and
        r["data.datetime_month"] in map(float, [3, 4]) and
        r["data.datetime_day"] in map(float, [2, 5]) for r in result)


def test_array_of_objects(session, models):
    q = session.query(models.Form)\
        .filter(models.Form.tenant_id == 1, models.Form.type_id == 3)

    session.execute('set search_path to "$user", public')
    create_view = CreateJSONView('foobar', q, models.Form.data, {
        'type': 'object',
        'id_property': 'id',
        'properties': {
            'id': {
                'type': 'integer'
            },
            'array': {
                'type': 'array',
                'items': {
                    'type': 'object',
                    'properties': {
                        'baz': {
                            'type': 'integer'
                        },
                        'quux': {
                            'type': 'boolean'
                        }
                    }
                }
            }
        }
    }, use_materialized_view=True)

    session.execute(create_view)
    session.commit()

    assert list(session.execute("""SELECT definition FROM pg_views where
                               viewname='foobar_array'"""))[0][0]
    assert list(session.execute("""SELECT definition FROM pg_views where
                               viewname='foobar'"""))[0][0]
    assert list(session.execute("""SELECT definition FROM pg_matviews where
                               matviewname='forms_array_json'"""))[0][0]
    results = list(session.execute("""
        SELECT foobar."data.id", foobar_array."array.baz",
            foobar_array."array.quux"
        FROM foobar JOIN foobar_array ON foobar."data.id" = foobar_array.parent_id
    """))

    assert len(results) == 100 * 2
    for result in results:
        assert result[0] in range(100)
        assert result[1] in range(5)
        assert result[2] in [True, False]

        
def test_nested_data_with_nulls(session, models):
    """
    Tests nested data, and both types of nulls:
    - where there was no value for that property in the JSON
    - where the value for that property was null
    """
    schema = {
        'type': 'object',
        'properties': {
            'foo': {
                'type': 'object',
                'properties': {
                    'bar': {
                        'type': 'integer'
                    }
                }
            },
            'eggs': {
                'type': 'object',
                'properties': {
                    'spam': {
                        'type': 'string'
                    }
                }
            }
        }
    }
    q = session.query(models.Form)\
            .filter(models.Form.tenant_id == 1, models.Form.type_id == 2)
    
    create_view = CreateJSONView('foo2', q, models.Form.data, schema)
    session.execute(create_view)

    result = list(session.execute('SELECT * from foo2'))
    assert len(result)
    assert len(result[0]) == 2 + 2

    assert any(r['data.foo.bar'] is None for r in result)
    assert any(isinstance(r['data.foo.bar'], int) for r in result)
    assert any(r['data.eggs.spam'] is None for r in result)
    assert any(isinstance(r['data.eggs.spam'], str) for r in result)
    assert all(r['data.foo.bar'] is None or \
                isinstance(r['data.foo.bar'], int) for r in result)
    assert all(r['data.eggs.spam'] is None or \
                isinstance(r['data.eggs.spam'], str) for r in result)


def _test_quantifiers(schema, models, session):
    q = session.query(models.Form)\
            .filter(models.Form.tenant_id == 1,
                    models.Form.type_id == 1)
    view_name = 'foo_%s' % (id(schema))
    create_view = CreateJSONView(view_name, q, models.Form.data, schema)
    session.execute(create_view)
    result = list(session.execute("SELECT * from %s" % view_name))
    assert len(result)
    for row in result:
        assert len(row) == 2 + 2
        assert isinstance(row['data.string'], str)
        assert isinstance(row['data.decimal'], Decimal)

QUANTIFIER_SCHEMAS = [
    {
        "properties": {
            "string": SCHEMAS['string']
        }
    },
    {
        "properties": {
            "decimal": SCHEMAS['decimal']
        }
    }
]

def test_jsonschema_all_of(session, models):
    _test_quantifiers({
        "type": "object",
        "allOf": QUANTIFIER_SCHEMAS
    }, models, session)

def test_jsonschema_one_of(session, models):
    _test_quantifiers({
        "type": "object",
        "oneOf": QUANTIFIER_SCHEMAS
    }, models, session)

def test_jsonschema_any_of(session, models):
    _test_quantifiers({
        "type": "object",
        "anyOf": QUANTIFIER_SCHEMAS
    }, models, session)


def test_conflicting_schema_properties(session, models):
    schema = {
        "type": "object",
        "oneOf": [
            {
                "properties": {
                    "boolean": SCHEMAS['boolean']
                }
            },
            {
                "properties": {
                    "boolean": SCHEMAS['string']
                }
            }
        ]
    }
    with pytest.raises(JSONSchemaConflict):
        create_view = CreateJSONView(None, None, None, schema)
        session.execute(create_view)


def test_invalid_schema_property_types(session, models):
    schema = {
        "type": "object",
        "properties": {
            "foo": {
                "type": "any"
            }
        }
    }
    with pytest.raises(InvalidJSONSchemaError):
        create_view = CreateJSONView(None, None, None, schema)
        session.execute(create_view)

    schema = {
        "type": "object",
        "properties": {
            "foo": {
                "type": ["string", "integer"]
            }
        }
    }
    with pytest.raises(InvalidJSONSchemaError):
        create_view = CreateJSONView(None, None, None, schema)
        session.execute(create_view)

    schema = {
        "type": "object",
        "properties": {
            "foo": {}
        }
    }
    with pytest.raises(InvalidJSONSchemaError):
        create_view = CreateJSONView(None, None, None, schema)
        session.execute(create_view)

def test_create_json_view_returns_table_columns(session, models):
    q = session.query(models.Form)\
            .filter(models.Form.tenant_id == 1, models.Form.type_id == 1)
    
    create_view = CreateJSONView('foo', q, models.Form.data, {
        'type': 'object',
        'properties': SCHEMAS
    })
    session.execute(create_view)
    # I'm lazy.
    columns = create_view.columns
    paths = [c.path for c in columns]
    enums = [c.enum for c in columns]
    titles = [c.title for c in columns]
    assert len(columns) == len(SCHEMAS)   # + 1.  Leaving out primary key
                                          # because it's not useful for BI
                                          # schema generation, which is what
                                          # this test is aimed at
    assert len(paths) == len(set(paths))
    assert len(enums) == len(set(
        [tuple(e) if isinstance(e, list) else e for e in enums]))
    assert len(titles) == len(set(titles))


def test_can_change_type_of_column_in_existing_view(session, models):
    q = session.query(models.Form)\
            .filter(models.Form.tenant_id == 1, models.Form.type_id == 1)
    
    create_view = CreateJSONView('foo', q, models.Form.data, {
        'type': 'object',
        'properties': SCHEMAS
    })
    session.execute(create_view)

    schemas = copy.deepcopy(SCHEMAS)
    schemas['string']['type'] = 'integer'

    create_view = CreateJSONView('foo', q, models.Form.data, {
        'type': 'object',
        'properties': schemas
    })
    session.execute(create_view)


def test_cant_replace_view_without_using_replace(session, models):
    q = session.query(models.Form)\
            .filter(models.Form.tenant_id == 1, models.Form.type_id == 1)
    
    create_view = CreateJSONView('foo', q, models.Form.data, {
        'type': 'object',
        'properties': SCHEMAS
    })
    session.execute(create_view)

    with pytest.raises(exc.ProgrammingError):
        try:
            create_view = _CreateJSONView('foo', q, models.Form.data, {
                'type': 'object',
                'properties': SCHEMAS
            })
            session.execute(create_view)
        finally:
            session.rollback()


@pytest.mark.xfail
def test_partial_index_creation(session, models):
    # This will fail (no index-only scans, just index scans) until this
    # Postgres issue is fixed:
    # http://postgresql.1045698.n5.nabble.com/No-Index-Only-Scan-on-Partial-Index-td5773024.html
    q = session.query(models.Form)\
            .filter(models.Form.tenant_id == 1, models.Form.type_id == 1)
    
    create_view = CreateJSONView('foo', q, models.Form.data, {
        'type': 'object',
        'properties': SCHEMAS
    })
    session.execute(create_view)

    indexes = []
    for k, v in SCHEMAS.items():
        result = list(session.execute(
            'EXPLAIN ANALYZE SELECT "%s" from foo' % ("data." + k)))
        matchobj = re.search('Index Only Scan using (.+?) on', result[0][0])
        indexes.append(matchobj.group(1))
    assert len(indexes) == len(set(indexes))


def test_old_partial_index_deletion(session, models):
    pass


def test_disabling_index_creation(session, models):
    pass


def test_postgis_json(session, models):
    schema = {
        'type': 'object',
        'properties': {
            'geopoint': {
                "title": "A Geopoint",
                "type": "string",
                "format": "geopoint",
            }
        }
    }
    q = session.query(models.Form)\
            .filter(models.Form.tenant_id == 1,
                    models.Form.type_id == 1)
    view_name = 'foo_%s' % (id(schema))
    create_view = CreateJSONView(view_name, q, models.Form.data, schema)
    session.execute(create_view)

    # check that the postgis function casting and index is working by getting
    # count of points within a bounding box that does not include all points
    within_count = list(session.execute("""
        SELECT COUNT(*) FROM %s
        WHERE ST_Within(ST_SetSRID("data.geopoint", 4326),
            ST_GeometryFromText(
                'POLYGON((-72 42, -72 43, -71 43, -71 42, -72 42))', 4326))
    """ % view_name))[0][0]

    all_count = list(session.execute("SELECT COUNT(*) FROM %s" % view_name))[0][0]
    assert 0 < within_count < all_count
