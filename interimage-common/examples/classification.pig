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

filtered2 = FILTER filtered1 BY properties#'ratiayer_4' > 0.2988;

projected1 = FOREACH filtered2 GENERATE geometry, data, II_ToClassification('grass',II_Min(II_Membership('grass-mean-layer2',properties#'meanyer_2'), II_Membership('grass-mean-layer3',properties#'meanyer_3')),properties) as properties;

projected2 = FOREACH projected1 GENERATE geometry, data, II_ToClassification('trees',II_Min(II_Membership('trees-mean-layer2',properties#'meanyer_2'), II_Membership('trees-mean-layer3',properties#'meanyer_3')),properties) as properties;

classified = FOREACH projected2 GENERATE geometry, data, II_Classify(properties) as properties;

STORE classified INTO 's3n://interimage2/results/classified.json' USING org.apache.pig.piggybank.storage.JsonStorage();