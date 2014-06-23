SET pig.tmpfilecompression true
SET pig.tmpfilecompression.codec lzo

REGISTER s3n://interimage2/libs/jts-1.13.jar;

REGISTER s3n://interimage2/libs/interimage-common-0.0.1-SNAPSHOT.jar;
REGISTER s3n://interimage2/libs/interimage-datamining-0.0.1-SNAPSHOT.jar;
--REGISTER s3n://interimage2/libs/interimage-data-0.0.1-SNAPSHOT.jar;
REGISTER s3n://interimage2/libs/interimage-geometry-0.0.1-SNAPSHOT.jar;

IMPORT 's3n://interimage2/scripts/interimage-common-import.pig';
IMPORT 's3n://interimage2/scripts/interimage-datamining-import.pig';
--IMPORT 's3n://interimage2/scripts/interimage-data-import.pig';
IMPORT 's3n://interimage2/scripts/interimage-geometry-import.pig';

segmentation = LOAD 's3n://interimage2/resources/shapes/segmentation.json' USING org.apache.pig.piggybank.storage.JsonLoader('geometry:bytearray, data:map[], properties:map[]');

f1 = FILTER segmentation BY (NOT II_IsEmpty(geometry)) AND II_IsValid(geometry);

p1 = FOREACH f1 GENERATE geometry, data, II_ToProps('false','iirep',II_ToProps('false','iinrep',properties)) as properties;

p2 = FOREACH p1 GENERATE II_CRSTransform(geometry, properties#'epsg', 'EPSG:3857') AS geometry, data, properties;

p3 = FOREACH p2 GENERATE geometry, data, II_ToProps(II_CalculateTiles(geometry),'tile',properties) AS properties;

replicated = FOREACH p3 GENERATE FLATTEN(II_Replicate(geometry, data, properties)) AS (geometry:bytearray, data:map[], properties:map[]);

grouped = GROUP replicated BY properties#'tile' PARALLEL 2;

neighbors = FOREACH grouped GENERATE FLATTEN(II_ReplicateNeighborhood(replicated)) AS (geometry:bytearray, data:map[], properties:map[]);

f2 = FILTER neighbors BY properties#'iirep' == 'false' AND properties#'iinrep' == 'false';

p3 = FOREACH f2 GENERATE II_ToText(geometry) as geometry, data, properties; 

STORE p3 INTO 's3n://interimage2/results/result.json' USING org.apache.pig.piggybank.storage.JsonStorage();