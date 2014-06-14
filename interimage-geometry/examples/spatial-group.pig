SET pig.tmpfilecompression true
SET pig.tmpfilecompression.codec lzo

REGISTER s3n://interimage2/libs/jts-1.8.jar;
REGISTER s3n://interimage2/libs/interimage-geometry-0.1.jar;

IMPORT 's3n://interimage2/scripts/interimage-geometry-import.pig';

small_polygons = LOAD 's3n://interimage2/datasets/objects2.json' USING org.apache.pig.piggybank.storage.JsonLoader('geometry:bytearray, data:map[], properties:map[]');

big_polygons = LOAD 's3n://interimage2/datasets/objects.json' USING org.apache.pig.piggybank.storage.JsonLoader('geometry:bytearray, data:map[], properties:map[]');

grouped = II_SpatialGroup(big_polygons, small_polygons, 2);

relations = FOREACH grouped GENERATE big_polygons::geometry..big_polygons::properties, II_NumberOf(big_polygons::group,'small') AS number;

STORE relations INTO 's3n://interimage2/results/relations.json' USING org.apache.pig.piggybank.storage.JsonStorage();