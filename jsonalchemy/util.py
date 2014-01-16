import hashlib
import base64

from sqlalchemy.inspection import inspect
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.ddl import DDLElement
from sqlalchemy.sql.compiler import SQLCompiler

from psycopg2.extensions import adapt as sqlescape


__all__ = [
    'CreateView',
    'CreateIndexes',
    'compile_element',
    'short_hash',
    'merge_dicts'
]


class CreateView(DDLElement):
    def __init__(self, name, query, replace=False):
        self.name = name
        self.query = query
        self.replace = replace


@compiles(CreateView)
def visit_create_view(element, ddlcompiler, **kw):
    select = inspect(element.query).selectable
    if element.replace:
        create = "CREATE OR REPLACE"
    else:
        create = "CREATE"

    return "%s VIEW %s AS %s" % (
        create,
        element.name,
        ddlcompiler.sql_compiler.process(select, literal_binds=True)
    )


def short_hash(str):
    return base64.urlsafe_b64encode(
            hashlib.sha1(str).digest()[:10])[:-2]

def compile_element(element, dialect):
    #statement = query.statement
    comp = SQLCompiler(dialect, element)
    comp.compile()
    enc = dialect.encoding
    params = {}
    for k,v in comp.params.iteritems():
        if isinstance(v, unicode):
            v = v.encode(enc)
        params[k] = sqlescape(v)

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

