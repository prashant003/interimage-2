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
A Pig script that takes bus stations and parks data and computes
the intersections between buffers of 100 meters around the bus stations and
the parks larger than 25 squared meters.
@author: Rodrigo Ferreira
*/

SET pig.tmpfilecompression true
SET pig.tmpfilecompression.codec lzo

REGISTER s3n://interimage2/libs/jts-1.8.jar;
REGISTER s3n://interimage2/libs/interimage-geometry-0.1.jar;

IMPORT 's3n://interimage2/scripts/interimage-geometry-import.pig';

stations = LOAD 's3n://interimage2/datasets/stations.json' USING org.apache.pig.piggybank.storage.JsonLoader('geometry:chararray, properties:map[]');

stations_buffered = FOREACH stations GENERATE *, II_Buffer(geometry,100) AS buffered;

parks = LOAD 's3n://interimage2/datasets/parks.json' USING org.apache.pig.piggybank.storage.JsonLoader('geometry:chararray, properties:map[]');

parks_filtered = FILTER parks BY II_Area(geometry) > 25;

stations_parks = CROSS stations_buffered, parks_filtered;

intersections = FILTER stations_parks BY II_Intersects(stations_buffered::buffered,parks_filtered::geometry);

STORE intersections INTO 's3n://interimage2/results/intersections.json' USING org.apache.pig.piggybank.storage.JsonStorage();
