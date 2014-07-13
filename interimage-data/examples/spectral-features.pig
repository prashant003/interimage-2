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

load_1 = LOAD 's3n://interimage2/resources/shapes/result.json' USING org.apache.pig.piggybank.storage.JsonLoader('geometry:chararray, data:map[], properties:map[]');

f1 = FILTER load_1 BY (NOT II_IsEmpty(geometry)) AND II_IsValid(geometry);

p1 = FOREACH f1 GENERATE geometry, data, II_ToProps(II_CalculateTiles(geometry),'tile',properties) AS properties;

DEFINE SpectralFeatures br.puc_rio.ele.lvc.interimage.data.udf.SpectralFeatures('https://s3.amazonaws.com/interimage2/resources/images/','mean1 = mean(image_layer1);mean2 = mean(image_layer2);mean3 = mean(image_layer3);mean4 = mean(image_layer4);max1 = maxPixelValue(image_layer1);min2 = minPixelValue(image_layer2);brightness = brightness(image);ratio3 = ratio(image_layer3);amplitude4 = amplitudeValue(image_layer4);bandadd23 = bandMeanAdd(image_layer2,image_layer3);banddiv24 = bandMeanDiv(image_layer2,image_layer4);bandmul34 = bandMeanMul(image_layer3,image_layer4);bandsub41 = bandMeanSub(image_layer4,image_layer1)','256.0');

g1 = II_SpectralFeatures(p1,2);

p2 = FOREACH g1 GENERATE II_ToText(geometry) as geometry, data, properties;

STORE p2 INTO 's3n://interimage2/results/result.json' USING org.apache.pig.piggybank.storage.JsonStorage();