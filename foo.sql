CREATE EXTENSION IF NOT EXISTS plv8;



CREATE OR REPLACE FUNCTION json_string(data json, key text) RETURNS TEXT AS $$ var ret = data, keys = key.split('.'), len = keys.length; for (var i = 0; i < len; ++i) { if (ret) { ret = ret[keys[i]] }; } if (typeof ret === "undefined") { ret = null; } else if (ret) { ret = ret.toString(); } return ret;


$$ LANGUAGE plv8 IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION json_int(data json, key text) RETURNS INT AS $$ var ret = data, keys = key.split('.'), len = keys.length; for (var i = 0; i < len; ++i) { if (ret) { ret = ret[keys[i]] } } if (typeof ret === "undefined") { ret = null; } else { ret = parseInt(ret, 10); if (isNaN(ret)) { ret = null; } } return ret; $$ LANGUAGE plv8 IMMUTABLE STRICT;


CREATE TABLE form_types ( id SERIAL NOT NULL, name VARCHAR(200), PRIMARY KEY (id) );


CREATE TABLE tenants ( id SERIAL NOT NULL, name VARCHAR(200), PRIMARY KEY (id) );


CREATE TABLE forms ( id SERIAL NOT NULL, tenant_id INTEGER, type_id INTEGER, data JSON, PRIMARY KEY (id), FOREIGN KEY(tenant_id) REFERENCES tenants (id), FOREIGN KEY(type_id) REFERENCES form_types (id) );


CREATE INDEX ix_forms_type_id ON forms (type_id); CREATE INDEX ix_forms_tenant_id ON forms (tenant_id); INSERT INTO tenants (name) VALUES ('mike'), ('bob'); INSERT INTO form_types (name) VALUES ('type 1'), ('type 2'); INSERT INTO forms (tenant_id, type_id, data) VALUES (1, 1, '{"string": "unicorns", "int": 1}'), (1, 1, '{"string": "pythons", "int": 2}'), (1, 1, '{"string": "pythons", "int": 8}'), (1, 1, '{"string": "penguins"}');


CREATE OR REPLACE VIEW foo AS SELECT forms.id AS forms_id, json_string(forms.data, 'string') AS "data.string", json_int(forms.data, 'int') AS "data.int" FROM forms WHERE forms.tenant_id = 1 AND forms.type_id = 1;


CREATE INDEX "forms_string" ON forms (json_string(data, 'string')) WHERE tenant_id = 1 AND type_id = 1; CREATE INDEX "forms_int" ON forms (json_int(data, 'int')) WHERE tenant_id = 1 AND type_id = 1;


EXPLAIN ANALYZE VERBOSE SELECT "data.string" from foo;

