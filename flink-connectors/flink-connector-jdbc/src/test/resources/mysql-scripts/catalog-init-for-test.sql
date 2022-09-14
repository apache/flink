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
* The test for mysql 5.6.X & 5.7.X & 8.0.X versions.
* The init script contains some types that are incompatible with lower versions.
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
  `col_int` int(11) DEFAULT NULL,
  `col_int_unsigned` int(10) unsigned DEFAULT NULL,
  `col_integer` int(11) DEFAULT NULL,
  `col_integer_unsigned` int(10) unsigned DEFAULT NULL,
  `col_longblob` longblob,
  `col_longtext` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
  `col_mediumblob` mediumblob,
  `col_mediumint` mediumint(9) DEFAULT NULL,
  `col_mediumint_unsigned` mediumint(8) unsigned DEFAULT NULL,
  `col_mediumtext` mediumtext,
  `col_numeric` decimal(10,0) DEFAULT NULL,
  `col_numeric_unsigned` decimal(10,0) unsigned DEFAULT NULL,
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
  `col_datetime_p3` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT 'Test for precision  value in default and user define precision',
  `col_time_p3` time(3) DEFAULT NULL COMMENT 'Test for precision  value in default and user define precision',
  `col_timestamp_p3` timestamp(3) NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT 'Test for precision  value in default and user define precision',
  `col_varbinary` varbinary(255) DEFAULT NULL,
  PRIMARY KEY (`pid`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of t_all_types
-- ----------------------------
INSERT INTO `t_all_types` VALUES (1, -1, 1, null, b'1', null, 'hello', '2021-08-04', '2021-08-04 01:54:16', -1, 1, -1, 1, 'enum2', -9.1, 9.1, -1, 1, -1, 1, null, 'col_longtext', null, -1, 1, 'col_mediumtext', -99, 99, -1, 1, 'set_ele1', -1, 1, 'col_text', '10:32:34', '2021-08-04 01:54:16', 'col_tinytext', -1, 1, null, 'col_varchar', '2021-08-04 01:54:16.463', '09:33:43.000', '2021-08-04 01:54:16.463', null);
INSERT INTO `t_all_types` VALUES (2, -1, 1, null, b'1', null, 'hello', '2021-08-04', '2021-08-04 01:53:19', -1, 1, -1, 1, 'enum2', -9.1, 9.1, -1, 1, -1, 1, null, 'col_longtext', null, -1, 1, 'col_mediumtext', -99, 99, -1, 1, 'set_ele1,set_ele12', -1, 1, 'col_text', '10:32:34', '2021-08-04 01:53:19', 'col_tinytext', -1, 1, null, 'col_varchar', '2021-08-04 01:53:19.098', '09:33:43.000', '2021-08-04 01:53:19.098', null);

-- Create test table t_all_types_sink.
DROP TABLE IF EXISTS `t_all_types_sink`;
CREATE TABLE `t_all_types_sink` select * from t_all_types where 1=2;

-- Create test table t_grouped_by_sink.
DROP TABLE IF EXISTS `t_grouped_by_sink`;
CREATE TABLE `t_grouped_by_sink` (
  `pid` bigint(20) NOT NULL AUTO_INCREMENT,
  `col_bigint` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`pid`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- Create test table t_pk.
DROP TABLE IF EXISTS `t_pk`;
CREATE TABLE `t_pk` (
  `uid` bigint(20) NOT NULL AUTO_INCREMENT,
  `col_bigint` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`uid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- Create the `test2` database.
DROP DATABASE IF EXISTS `test2`;
CREATE DATABASE `test2` CHARSET=utf8;

-- Uses `test2` database.
use `test2`;

-- Create test table t_pk.
DROP TABLE IF EXISTS `t_pk`;
CREATE TABLE `t_pk` (
  `pid` int(11) NOT NULL AUTO_INCREMENT,
  `col_varchar` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`pid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
