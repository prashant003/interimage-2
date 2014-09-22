SET pig.tmpfilecompression true
SET pig.tmpfilecompression.codec lzo

REGISTER s3n://interimage2/libs/jts-1.13.jar;

REGISTER s3n://interimage2/libs/imglib2-2.0.0-beta-25.jar;
REGISTER s3n://interimage2/libs/imglib2-algorithms-2.0.0-beta-25.jar
REGISTER s3n://interimage2/libs/imglib2-meta-2.0.0-beta-25.jar
REGISTER s3n://interimage2/libs/imglib2-ops-2.0.0-beta-25.jar

REGISTER s3n://interimage2/libs/jai_codec.jar;
REGISTER s3n://interimage2/libs/jai_core-1.1.3.jar;
REGISTER s3n://interimage2/libs/jai_imageio-1.1.jar;

REGISTER s3n://interimage2/libs/interimage-common-0.0.1-SNAPSHOT.jar;
REGISTER s3n://interimage2/libs/interimage-datamining-0.0.1-SNAPSHOT.jar;
REGISTER s3n://interimage2/libs/interimage-data-0.0.1-SNAPSHOT.jar;
REGISTER s3n://interimage2/libs/interimage-geometry-0.0.1-SNAPSHOT.jar;
REGISTER s3n://interimage2/libs/interimage-operators-0.0.1-SNAPSHOT.jar;

IMPORT 's3n://interimage2/resources/scripts/interimage-import.pig';

load_1 = LOAD 's3n://interimage2/resources/tiles/T69217015.json' USING org.apache.pig.piggybank.storage.JsonLoader('geometry:chararray, data:map[], properties:map[]');

f1 = FILTER load_1 BY (NOT II_IsEmpty(geometry)) AND II_IsValid(geometry);

--DEFINE II_Limiarization br.puc_rio.ele.lvc.interimage.operators.udf.Limiarization('https://s3.amazonaws.com/interimage2/resources/images/','uberlandia','','0,50,100,255','Dark,Gray,Bright','Brightness');

--p1 = FOREACH f1 GENERATE FLATTEN(II_Limiarization(geometry, data, properties)) AS (geometry:bytearray, data:map[], properties:map[]);

DEFINE II_MutualBaatzSegmentation br.puc_rio.ele.lvc.interimage.operators.udf.MutualBaatzSegmentation('https://s3.amazonaws.com/interimage2/resources/images/','uberlandia','','20','0.5','0.75','0,1,1,1');

p1 = FOREACH f1 GENERATE FLATTEN(II_MutualBaatzSegmentation(geometry, data, properties)) AS (geometry:bytearray, data:map[], properties:map[]);

--DEFINE II_ChessboardSegmentation br.puc_rio.ele.lvc.interimage.geometry.udf.ChessboardSegmentation('https://s3.amazonaws.com/interimage2/resources/images/','image','170.66667');

--p1 = FOREACH f1 GENERATE FLATTEN(II_ChessboardSegmentation(geometry, data, properties)) AS (geometry:bytearray, data:map[], properties:map[]);

p3 = FOREACH p1 GENERATE II_ToText(geometry) as geometry, data, properties; 

STORE p3 INTO 's3n://interimage2/results/result.json' USING org.apache.pig.piggybank.storage.JsonStorage();