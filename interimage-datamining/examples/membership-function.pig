/*Copyright 2014 Computer Vision Lab

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.*/

/**
A Pig script that computes the membership value of a membership function (fuzzy set) positioned at the given attribute value.
@author: Rodrigo Ferreira
*/

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

filtered = FILTER polygons BY II_SpatialFilter(geometry, properties#'tile');

projected = FOREACH filtered GENERATE II_Min(II_Membership('grass-mean-layer2',properties#'meanyer_2'),II_Membership('grass-mean-layer3',properties#'meanyer_3')) as membership;

STORE projected INTO 's3n://interimage2/results/projected.json' USING org.apache.pig.piggybank.storage.JsonStorage();