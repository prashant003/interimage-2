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
A Pig script that spatially joins two datasets and selects the geometries that intersect each other.
@author: Rodrigo Ferreira
*/

REGISTER s3n://interimage2/libs/jts-1.8.jar;
REGISTER s3n://interimage2/libs/interimage-geometry-0.1.jar;

IMPORT 's3n://interimage2/scripts/interimage-geometry-import.pig';

small_polygons = LOAD 's3n://interimage2/datasets/objects2.json' USING org.apache.pig.piggybank.storage.JsonLoader('geometry:chararray, data:map[], properties:map[]');

big_polygons = LOAD 's3n://interimage2/datasets/objects.json' USING org.apache.pig.piggybank.storage.JsonLoader('geometry:chararray, data:map[], properties:map[]');

small_polygons_filtered = FILTER small_polygons BY II_SpatialFilter(geometry, properties#'tile');

big_polygons_filtered = FILTER big_polygons BY II_SpatialFilter(geometry, properties#'tile');

joined = II_SpatialJoin(small_polygons_filtered, big_polygons_filtered, 2);

intersections = FILTER joined BY II_Intersects(small_polygons_filtered::geometry, big_polygons_filtered::geometry);

STORE intersections INTO 's3n://interimage2/results/intersections.json' USING org.apache.pig.piggybank.storage.JsonStorage();