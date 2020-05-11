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

package org.apache.flink.sql.parser.hive.ddl;

import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.sql.parser.hive.impl.ParseException;
import org.apache.flink.table.catalog.config.CatalogConfig;

import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveDatabase.ALTER_DATABASE_OP;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveDatabase.DATABASE_LOCATION_URI;

/**
 * Util methods for Hive DDL Sql nodes.
 */
public class HiveDDLUtils {

	private static final Set<String> RESERVED_DB_PROPERTIES = new HashSet<>();

	static {
		RESERVED_DB_PROPERTIES.addAll(Arrays.asList(ALTER_DATABASE_OP, DATABASE_LOCATION_URI));
	}

	private HiveDDLUtils() {
	}

	public static SqlNodeList checkReservedDBProperties(SqlNodeList props) throws ParseException {
		return checkReservedProperties(RESERVED_DB_PROPERTIES, props, "Databases");
	}

	public static SqlNodeList ensureNonGeneric(SqlNodeList props) throws ParseException {
		for (SqlNode node : props) {
			if (node instanceof SqlTableOption && ((SqlTableOption) node).getKeyString().equalsIgnoreCase(CatalogConfig.IS_GENERIC)) {
				if (!((SqlTableOption) node).getValueString().equalsIgnoreCase("false")) {
					throw new ParseException("Creating generic object with Hive dialect is not allowed");
				}
			}
		}
		return props;
	}

	private static SqlNodeList checkReservedProperties(Set<String> reservedProperties, SqlNodeList properties,
			String metaType) throws ParseException {
		if (properties == null) {
			return null;
		}
		Set<String> match = new HashSet<>();
		for (SqlNode node : properties) {
			if (node instanceof SqlTableOption) {
				String key = ((SqlTableOption) node).getKeyString();
				if (reservedProperties.contains(key)) {
					match.add(key);
				}
			}
		}
		if (!match.isEmpty()) {
			throw new ParseException(String.format(
					"Properties %s are reserved and shouldn't be used for Hive %s", match, metaType));
		}
		return properties;
	}

	public static SqlTableOption toTableOption(String key, SqlNode value, SqlParserPos pos) {
		return new SqlTableOption(SqlLiteral.createCharString(key, pos), value, pos);
	}

	public static SqlTableOption toTableOption(String key, String value, SqlParserPos pos) {
		return new SqlTableOption(SqlLiteral.createCharString(key, pos), SqlLiteral.createCharString(value, pos), pos);
	}
}
