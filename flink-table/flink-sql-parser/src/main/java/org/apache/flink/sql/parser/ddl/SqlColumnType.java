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
 * All supported data types in DDL. Used for Create Table DDL validation.
 */
public enum SqlColumnType {
	BOOLEAN,
	TINYINT,
	SMALLINT,
	INT,
	INTEGER,
	BIGINT,
	REAL,
	FLOAT,
	DOUBLE,
	DECIMAL,
	DATE,
	TIME,
	TIMESTAMP,
	VARCHAR,
	VARBINARY,
	ANY,
	ARRAY,
	MAP,
	ROW,
	UNSUPPORTED;

	/** Returns the column type with the string representation. **/
	public static SqlColumnType getType(String type) {
		if (type == null) {
			return UNSUPPORTED;
		}
		try {
			return SqlColumnType.valueOf(type.toUpperCase());
		} catch (IllegalArgumentException var1) {
			return UNSUPPORTED;
		}
	}

	/** Returns true if this type is unsupported. **/
	public boolean isUnsupported() {
		return this.equals(UNSUPPORTED);
	}
}
