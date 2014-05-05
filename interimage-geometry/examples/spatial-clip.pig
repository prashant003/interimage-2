SET pig.tmpfilecompression true
SET pig.tmpfilecompression.codec lzo

REGISTER s3n://interimage2/libs/jts-1.8.jar;

REGISTER s3n://interimage2/libs/interimage-common-0.1.jar;
REGISTER s3n://interimage2/libs/interimage-datamining-0.1.jar;
REGISTER s3n://interimage2/libs/interimage-geometry-0.1.jar;

IMPORT 's3n://interimage2/scripts/interimage-common-import.pig';
IMPORT 's3n://interimage2/scripts/interimage-datamining-import.pig';
IMPORT 's3n://interimage2/scripts/interimage-geometry-import.pig';

polygons = LOAD 's3n://interimage2/datasets/polygons.json' USING org.apache.pig.piggybank.storage.JsonLoader('geometry:bytearray, data:map[], properties:map[]');

filtered1 = FILTER polygons BY (NOT II_IsEmpty(geometry)) and II_IsValid(geometry);

filtered2 = FILTER filtered1 BY II_SpatialFilter(geometry, properties#'tile');

clipped = FOREACH filtered2 GENERATE II_ToText(II_SpatialClip(geometry, properties#'tile')) AS geometry, data, properties;

STORE clipped INTO 's3n://interimage2/results/clipped.json' USING org.apache.pig.piggybank.storage.JsonStorage();