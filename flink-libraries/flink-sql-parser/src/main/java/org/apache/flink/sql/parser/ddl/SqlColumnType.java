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

package org.apache.flink.sql.parser.ddl;

/**
 * All data types in DDL.
 */
public enum SqlColumnType {

	BOOLEAN,
	TINYINT,
	SMALLINT,
	INT,
	BIGINT,
	FLOAT,
	DOUBLE,
	DECIMAL,
	DATE,
	TIME,
	TIMESTAMP,
	VARCHAR,
	VARBINARY,
	ANY,
	UNSUPPORTED;

	public static SqlColumnType getType(String type) {
		if (type == null) {
			return UNSUPPORTED;
		}
		String lowerCaseType = type.toLowerCase();
		switch (lowerCaseType) {
			case "boolean":
				return BOOLEAN;
			case "tinyint":
				return TINYINT;
			case "smallint":
				return SMALLINT;
			case "int":
			case "integer":
				return INT;
			case "bigint":
				return BIGINT;
			case "real":
			case "float":
				return FLOAT;
			case "decimal":
				return DECIMAL;
			case "double":
				return DOUBLE;
			case "date":
				return DATE;
			case "time":
				return TIME;
			case "timestamp":
				return TIMESTAMP;
			case "varchar":
				return VARCHAR;
			case "varbinary":
				return VARBINARY;
			case "any":
				return ANY;
			default:
				return UNSUPPORTED;
		}
	}

}
