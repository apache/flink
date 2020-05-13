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
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * CREATE Database DDL for Hive dialect.
 */
public class SqlCreateHiveDatabase extends SqlCreateDatabase {

	public static final String DATABASE_LOCATION_URI = "hive.database.location-uri";

	private SqlNodeList originPropList;
	private final SqlCharStringLiteral location;

	public SqlCreateHiveDatabase(SqlParserPos pos, SqlIdentifier databaseName, SqlNodeList propertyList,
			SqlCharStringLiteral comment, SqlCharStringLiteral location, boolean ifNotExists) throws ParseException {
		super(pos, databaseName, HiveDDLUtils.checkReservedDBProperties(propertyList), comment, ifNotExists);
		HiveDDLUtils.ensureNonGeneric(propertyList);
		originPropList = new SqlNodeList(propertyList.getList(), propertyList.getParserPosition());
		// mark it as a hive database
		propertyList.add(HiveDDLUtils.toTableOption(CatalogConfig.IS_GENERIC, "false", pos));
		if (location != null) {
			propertyList.add(new SqlTableOption(
					SqlLiteral.createCharString(DATABASE_LOCATION_URI, location.getParserPosition()),
					location,
					location.getParserPosition()));
		}
		this.location = location;
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		writer.keyword("CREATE DATABASE");
		if (isIfNotExists()) {
			writer.keyword("IF NOT EXISTS");
		}
		getDatabaseName().unparse(writer, leftPrec, rightPrec);

		if (getComment().isPresent()) {
			writer.newlineAndIndent();
			writer.keyword("COMMENT");
			getComment().get().unparse(writer, leftPrec, rightPrec);
		}

		if (location != null) {
			writer.newlineAndIndent();
			writer.keyword("LOCATION");
			location.unparse(writer, leftPrec, rightPrec);
		}

		if (originPropList.size() > 0) {
			writer.keyword("WITH DBPROPERTIES");
			SqlWriter.Frame withFrame = writer.startList("(", ")");
			for (SqlNode property : originPropList) {
				printIndent(writer);
				property.unparse(writer, leftPrec, rightPrec);
			}
			writer.newlineAndIndent();
			writer.endList(withFrame);
		}
	}
}
