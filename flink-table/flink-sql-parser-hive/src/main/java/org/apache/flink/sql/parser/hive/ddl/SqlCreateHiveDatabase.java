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

import org.apache.flink.sql.parser.ddl.SqlCreateDatabase;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.sql.parser.hive.impl.ParseException;
import org.apache.flink.table.catalog.config.CatalogConfig;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * CREATE Database DDL for Hive dialect.
 */
public class SqlCreateHiveDatabase extends SqlCreateDatabase {

	public static final String DATABASE_LOCATION_URI = "database.location_uri";

	public SqlCreateHiveDatabase(SqlParserPos pos, SqlIdentifier databaseName, SqlNodeList propertyList,
			SqlCharStringLiteral comment, SqlCharStringLiteral location, boolean ifNotExists) throws ParseException {
		super(pos, databaseName, HiveDDLUtils.checkReservedDBProperties(propertyList), comment, ifNotExists);
		// mark it as a hive database
		HiveDDLUtils.ensureNonGeneric(propertyList);
		propertyList.add(HiveDDLUtils.toTableOption(CatalogConfig.IS_GENERIC, "false", pos));
		if (location != null) {
			propertyList.add(new SqlTableOption(
					SqlLiteral.createCharString(DATABASE_LOCATION_URI, location.getParserPosition()),
					location,
					location.getParserPosition()));
		}
	}
}
