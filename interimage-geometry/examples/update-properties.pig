
REGISTER s3n://interimage2/libs/jts-1.8.jar;
REGISTER s3n://interimage2/libs/interimage-geometry-0.1.jar;
REGISTER s3n://interimage2/libs/interimage-common-0.1.jar;

IMPORT 's3n://interimage2/scripts/interimage-geometry-import.pig';

polygons = LOAD 's3n://interimage2/datasets/objects.json' USING org.apache.pig.piggybank.storage.JsonLoader('geometry:chararray, data:map[], properties:map[]');

projected = FOREACH polygons GENERATE geometry, data, II_FieldToProps(II_Area(geometry), 'area', properties) AS properties;

STORE projected INTO 's3n://interimage2/results/intersections.json' USING org.apache.pig.piggybank.storage.JsonStorage();