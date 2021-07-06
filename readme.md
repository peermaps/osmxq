# osmxq

indexed quad format for open street map data

Build a database to lookup osm records by id where nearby features are clustered together in the
on-disk representation to greatly improve locality.

This database is not quite a spatial database itself, but should greatly assist in the batch
processing to populate a spatial database and denormalize records for rendering.

