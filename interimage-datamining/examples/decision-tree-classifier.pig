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
A Pig script that classifies objects using a decision tree classifier.
@author: Victor Quirita
*/

SET pig.tmpfilecompression true
SET pig.tmpfilecompression.codec lzo

REGISTER s3n://interimage2victor/libs/weka.jar;
REGISTER s3n://interimage2victor/libs/interimage-pig-datamining.jar;
IMPORT 's3n://interimage2victor/scripts/interimage-pig-datamining-import.pig';
rmf s3n://interimage2victor/results;
data_train = LOAD 's3n://interimage2victor/datasets/iris_train.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') AS (sl_train:float,sw_train:float,pl_train:float,pw_train:float,name_train:chararray);
data_test = LOAD 's3n://interimage2victor/datasets/iris_test.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') AS (sl_test:float,sw_test:float,pl_test:float,pw_test:float,name_test:chararray);

data_train = GROUP data_train ALL;
data_train = FOREACH data_train GENERATE $1 AS train_set;
test_set = CROSS data_test, data_train;
test_set = FOREACH test_set GENERATE sl_test as sl, sw_test as sw, pl_test as pl, pw_test as pw, name_test as class_name, train_set as train_set;
DUMP test_set;

classes = FOREACH test_set GENERATE *, II_DecisionTreeClassifier(4,sl,sw,pl,pw,train_set) AS results;
DUMP classes;

STORE classes INTO 's3n://interimage2victor/results' USING PigStorage(); 