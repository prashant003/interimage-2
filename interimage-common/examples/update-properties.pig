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
A Pig script that computes a feature and adds it to the properties map.
@author: Rodrigo Ferreira
*/

SET pig.tmpfilecompression true
SET pig.tmpfilecompression.codec lzo

REGISTER s3n://interimage2/libs/jts-1.8.jar;
REGISTER s3n://interimage2/libs/interimage-geometry-0.1.jar;
REGISTER s3n://interimage2/libs/interimage-common-0.1.jar;

IMPORT 's3n://interimage2/scripts/interimage-geometry-import.pig';
IMPORT 's3n://interimage2/scripts/interimage-common-import.pig';

polygons = LOAD 's3n://interimage2/datasets/polygons.json' USING org.apache.pig.piggybank.storage.JsonLoader('geometry:bytearray, data:map[], properties:map[]');

projected = FOREACH polygons GENERATE geometry, data, II_FieldToProps(II_Area(geometry), 'area', properties) AS properties;

STORE projected INTO 's3n://interimage2/results/projected.json' USING org.apache.pig.piggybank.storage.JsonStorage();