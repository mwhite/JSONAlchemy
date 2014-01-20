-- Adapted from https://gist.github.com/tobyhede/2715918
CREATE EXTENSION IF NOT EXISTS plv8;

CREATE OR REPLACE FUNCTION
date_part_immutable(text, anyelement) RETURNS DOUBLE PRECISION 
    AS 'SELECT date_part($1, $2)'
    LANGUAGE SQL
    IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION
json_string(data json, key text) RETURNS TEXT AS $$

    var ret = data,
        keys = key.split('.'),
        len = keys.length;

    for (var i = 0; i < len; ++i) {
        if (ret) {
            ret = ret[keys[i]]
        };
    }
    if (typeof ret === "undefined") {
        ret = null;
    } else if (ret) {
        ret = ret.toString();
    }
     
    return ret;
     
$$ LANGUAGE plv8 IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION
json_int(data json, key text) RETURNS INT AS $$
    var ret = data,
        keys = key.split('.'),
        len = keys.length;

    for (var i = 0; i < len; ++i) {
        if (ret) {
            ret = ret[keys[i]]
        }
    }
    if (typeof ret === "undefined") {
        ret = null;
    } else {
        ret = parseInt(ret, 10);
        if (isNaN(ret)) {
            ret = null;
        }
    }
     
    return ret;
 
$$ LANGUAGE plv8 IMMUTABLE STRICT;
 
 
CREATE OR REPLACE FUNCTION
json_int_array(data json, key text) RETURNS INT[] AS $$
    var ret = data,
        keys = key.split('.'),
        len = keys.length;

    for (var i = 0; i < len; ++i) {
        if (ret) {
            ret = ret[keys[i]];
        }
    }
    if (typeof ret === "undefined") {
        ret = null;
    }
    return ret;
 
$$ LANGUAGE plv8 IMMUTABLE STRICT;
 
 
CREATE OR REPLACE FUNCTION
json_float(data json, key text) RETURNS FLOAT AS $$
    var ret = data,
        keys = key.split('.'),
        len = keys.length;

    for (var i = 0; i < len; ++i) {
        if (ret) {
            ret = ret[keys[i]];
        }
    }
    if (typeof ret === "undefined") {
        ret = null;
    } else {
        ret = parseFloat(ret);
        if (isNaN(ret)) {
            ret = null;
        }
    }
    return ret;
 
$$ LANGUAGE plv8 IMMUTABLE STRICT;


-- handles decimals stored as strings
CREATE OR REPLACE FUNCTION
json_decimal(data json, key text) RETURNS DECIMAL AS $$
    var ret = data,
        keys = key.split('.'),
        len = keys.length;

    for (var i = 0; i < len; ++i) {
        if (ret) {
            ret = ret[keys[i]];
        }
    }
    if (typeof ret === "undefined") {
        ret = null;
    }
    return ret;
 
$$ LANGUAGE plv8 IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION
json_bool(data json, key text) RETURNS BOOLEAN AS $$
    var ret = data,
        keys = key.split('.'),
        len = keys.length;

    for (var i = 0; i < len; ++i) {
        if (ret) {
            ret = ret[keys[i]];
        }
    }
    if (typeof ret === "undefined") {
        ret = null;
    }
     
    return ret;
 
$$ LANGUAGE plv8 IMMUTABLE STRICT;
 
 
CREATE OR REPLACE FUNCTION
json_datetime(data json, key text) RETURNS TIMESTAMP WITH TIME ZONE AS $$
    var ret = data,
        keys = key.split('.'),
        len = keys.length;

    for (var i = 0; i < len; ++i) {
        if (ret) {
            ret = ret[keys[i]];
        }
    }

    if (typeof ret === "undefined") {
        ret = null;
    } else {
        ret = new Date(ret)
        if (isNaN(ret.getTime())) {
            ret = null;
        }
    }
    return ret;
 
$$ LANGUAGE plv8 IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION
json_datetime_no_tz(data json, key text) RETURNS TIMESTAMP AS $$
    var ret = data,
        keys = key.split('.'),
        len = keys.length;

    for (var i = 0; i < len; ++i) {
        if (ret) {
            ret = ret[keys[i]];
        }
    }

    if (typeof ret === "undefined") {
        ret = null;
    } else {
        ret = new Date(ret)
        if (isNaN(ret.getTime())) {
            ret = null;
        }
    }
    return ret;
 
$$ LANGUAGE plv8 IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION
json_date(data json, key text) RETURNS DATE AS $$
    var ret = data,
        keys = key.split('.'),
        len = keys.length;

    for (var i = 0; i < len; ++i) {
        if (ret) {
            ret = ret[keys[i]];
        }
    }

    if (typeof ret === "undefined") {
        ret = null;
    } else {
        ret = new Date(ret)
        if (isNaN(ret.getTime())) {
            ret = null;
        }
    }
    return ret;
 
$$ LANGUAGE plv8 IMMUTABLE STRICT;
