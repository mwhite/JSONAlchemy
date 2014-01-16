CREATE EXTENSION IF NOT EXISTS plv8;
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE OR REPLACE FUNCTION
json_geopoint(data json, key text) RETURNS GEOMETRY AS $$
    var ret = data,
        keys = key.split('.'),
        len = keys.length;

    for (var i = 0; i < len; ++i) {
        if (ret) {
            ret = ret[keys[i]];
        }
    }

    if (typeof ret === "undefined" || typeof ret !== "string") {
        ret = null;
    } else {
        var pieces = ret.split(',');
        if (pieces.length !== 2) {
            ret = null;
        } else {
            ret = plv8.execute("SELECT ST_MakePoint(" + pieces[0] + "," + pieces[1] + ") AS result")
            ret = ret[0]["result"];
        }
    }
    return ret;

$$ LANGUAGE plv8 IMMUTABLE STRICT;
