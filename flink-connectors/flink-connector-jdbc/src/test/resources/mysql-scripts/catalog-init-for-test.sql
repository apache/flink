/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
* The test for mysql 5.7.X & 8.0.X versions.
* The init script contains some types that are incompatible with 5.6.X or lower versions.
*/

-- Creates test user info and grants privileges.
CREATE USER 'mysql'@'%' IDENTIFIED BY 'mysql';
GRANT ALL ON *.* TO 'mysql'@'%';
FLUSH PRIVILEGES;

-- Create the `test` database.
DROP DATABASE IF EXISTS `test`;
CREATE DATABASE `test` CHARSET=utf8;

-- Uses `test` database.
use `test`;

-- Create test tables.
-- ----------------------------
-- Table structure for t_all_types
-- ----------------------------
DROP TABLE IF EXISTS `t_all_types`;
CREATE TABLE `t_all_types` (
  `pid` bigint(20) NOT NULL AUTO_INCREMENT,
  `col_bigint` bigint(20) DEFAULT NULL,
  `col_bigint_unsigned` bigint(20) unsigned DEFAULT NULL,
  `col_binary` binary(100) DEFAULT NULL,
  `col_bit` bit(1) DEFAULT NULL,
  `col_blob` blob,
  `col_char` char(10) DEFAULT NULL,
  `col_date` date DEFAULT NULL,
  `col_datetime` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `col_decimal` decimal(10,0) DEFAULT NULL,
  `col_decimal_unsigned` decimal(10,0) unsigned DEFAULT NULL,
  `col_double` double DEFAULT NULL,
  `col_double_unsigned` double unsigned DEFAULT NULL,
  `col_enum` enum('enum1','enum2','enum11') DEFAULT NULL,
  `col_float` float DEFAULT NULL,
  `col_float_unsigned` float unsigned DEFAULT NULL,
  `col_geometry` geometry DEFAULT NULL,
  `col_geometrycollection` geometrycollection DEFAULT NULL,
  `col_int` int(11) DEFAULT NULL,
  `col_int_unsigned` int(10) unsigned DEFAULT NULL,
  `col_integer` int(11) DEFAULT NULL,
  `col_integer_unsigned` int(10) unsigned DEFAULT NULL,
  `col_json` json DEFAULT NULL,
  `col_linestring` linestring DEFAULT NULL,
  `col_longblob` longblob,
  `col_longtext` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
  `col_mediumblob` mediumblob,
  `col_mediumint` mediumint(9) DEFAULT NULL,
  `col_mediumint_unsigned` mediumint(8) unsigned DEFAULT NULL,
  `col_mediumtext` mediumtext,
  `col_multilinestring` multilinestring DEFAULT NULL,
  `col_multipoint` multipoint DEFAULT NULL,
  `col_multipolygon` multipolygon DEFAULT NULL,
  `col_numeric` decimal(10,0) DEFAULT NULL,
  `col_numeric_unsigned` decimal(10,0) unsigned DEFAULT NULL,
  `col_polygon` polygon DEFAULT NULL,
  `col_point` point DEFAULT NULL,
  `col_real` double DEFAULT NULL,
  `col_real_unsigned` double unsigned DEFAULT NULL,
  `col_set` set('set_ele1','set_ele12') DEFAULT NULL,
  `col_smallint` smallint(6) DEFAULT NULL,
  `col_smallint_unsigned` smallint(5) unsigned DEFAULT NULL,
  `col_text` text,
  `col_time` time DEFAULT NULL,
  `col_timestamp` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `col_tinytext` tinytext,
  `col_tinyint` tinyint DEFAULT NULL,
  `col_tinyint_unsinged` tinyint(255) unsigned DEFAULT NULL,
  `col_tinyblob` tinyblob,
  `col_varchar` varchar(255) DEFAULT NULL,
  `col_year` year(4) DEFAULT NULL,
  `col_datetime_p3` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT 'Test for precision  value in default and user define precison',
  `col_time_p3` time(3) DEFAULT NULL COMMENT 'Test for precision  value in default and user define precison',
  `col_timestamp_p3` timestamp(3) NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT 'Test for precision  value in default and user define precison',
  `col_varbinary` varbinary(255) DEFAULT NULL,
  PRIMARY KEY (`pid`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of t_all_types
-- ----------------------------
INSERT INTO `t_all_types` VALUES (1, -1, 1, 0x31313100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000, b'1', '', 'hello', '2021-08-04', '2021-08-04 01:54:16', -1, 1, -1, 1, 'enum2', -9.1, 9.1, ST_GeomFromText('LINESTRING(121.342 31.5424, 121.346 31.2468, 121.453 31.4569)'), ST_GeomFromText('GEOMETRYCOLLECTION(POINT(10 10), POINT(30 30), LINESTRING(15 15, 20 20))'), -1, 1, -1, 1, '{\"k1\": \"v1\"}', ST_GeomFromText('LINESTRING(1 3, 12 5, 12 7)'), '', 'col_longtext', '', -1, 1, 'col_mediumtext', ST_GeomFromText('MULTILINESTRING((10 10, 20 20), (15 15, 30 15))'), ST_GeomFromText('MULTIPOINT(0 0, 20 20, 60 60)'), ST_GeomFromText('MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)), ((5 5, 7 5, 7 7, 5 7, 5 5)))'), -99, 99, ST_GeomFromText('POLYGON((121.474 31.2345, 121.472 31.2333, 121.471 31.2315, 121.472 31.2302, 121.473 31.2304, 121.476 31.232, 121.474 31.2345))'), ST_GeomFromText('POINT(6 7)'), -1, 1, 'set_ele1', -1, 1, 'col_text', '10:32:34', '2021-08-04 01:54:16', 'col_tinytext', -1, 1, '', 'col_varchar', 1999, '2021-08-04 01:54:16.463', '09:33:43.000', '2021-08-04 01:54:16.463', 0x3130313031303130);
INSERT INTO `t_all_types` VALUES (2, -1, 1, 0x526F6300000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000, b'1', 0x4D69636861656C, 'hello', '2021-08-04', '2021-08-04 01:53:19', -1, 1, -1, 1, 'enum2', -9.1, 9.1, ST_GeomFromText('LINESTRING(121.342 31.5424, 121.346 31.2468, 121.453 31.4569)'), ST_GeomFromText('GEOMETRYCOLLECTION(POINT(10 10), POINT(30 30), LINESTRING(15 15, 20 20))'), -1, 1, -1, 1, '{\"k1\": \"v1\"}', ST_GeomFromText('LINESTRING(1 3, 12 5, 12 7)'), 0x6C6F6E67626C6F62, 'col_longtext', 0x6D656469756D626C6F62, -1, 1, 'col_mediumtext', ST_GeomFromText('MULTILINESTRING((10 10, 20 20), (15 15, 30 15))'), ST_GeomFromText('MULTIPOINT(0 0, 20 20, 60 60)'), ST_GeomFromText('MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)), ((5 5, 7 5, 7 7, 5 7, 5 5)))'), -99, 99, ST_GeomFromText('POLYGON((121.474 31.2345, 121.472 31.2333, 121.471 31.2315, 121.472 31.2302, 121.473 31.2304, 121.476 31.232, 121.474 31.2345))'), ST_GeomFromText('POINT(6 7)'), -1, 1, 'set_ele1,set_ele12', -1, 1, 'col_text', '10:32:34', '2021-08-04 01:53:19', 'col_tinytext', -1, 1, 0x636F6C5F74696E79626C6F62, 'col_varchar', 1999, '2021-08-04 01:53:19.098', '09:33:43.000', '2021-08-04 01:53:19.098', 0x636F6C5F76617262696E617279);

-- Create test table t_all_types_sink.
DROP TABLE IF EXISTS `t_all_types_sink_without_year_type`;
CREATE TABLE `t_all_types_sink_without_year_type` select * from t_all_types where 1=2;
ALTER TABLE `t_all_types_sink_without_year_type` modify col_year date DEFAULT NULL;

DROP TABLE IF EXISTS `t_all_types_sink_with_year_type`;
CREATE TABLE `t_all_types_sink_with_year_type` select * from t_all_types where 1=2;

-- Create test table t_grouped_by_sink.
DROP TABLE IF EXISTS `t_grouped_by_sink`;
CREATE TABLE `t_grouped_by_sink` (
  `pid` bigint(20) NOT NULL AUTO_INCREMENT,
  `col_bigint` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`pid`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
