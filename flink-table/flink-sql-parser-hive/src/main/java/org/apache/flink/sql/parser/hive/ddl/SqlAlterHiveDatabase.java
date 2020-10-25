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

import org.apache.flink.sql.parser.ddl.SqlAlterDatabase;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Abstract class for ALTER DDL of a Hive database.
 */
public abstract class SqlAlterHiveDatabase extends SqlAlterDatabase {

	public static final String ALTER_DATABASE_OP = "hive.alter.database.op";

	protected final SqlNodeList originPropList;

	public SqlAlterHiveDatabase(SqlParserPos pos, SqlIdentifier databaseName, SqlNodeList propertyList) {
		super(pos, databaseName, propertyList);
		originPropList = new SqlNodeList(propertyList.getList(), propertyList.getParserPosition());
		propertyList.add(HiveDDLUtils.toTableOption(ALTER_DATABASE_OP, getAlterOp().name(), pos));
	}

	protected abstract AlterHiveDatabaseOp getAlterOp();

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		writer.keyword("ALTER DATABASE");
		getDatabaseName().unparse(writer, leftPrec, rightPrec);
		writer.keyword("SET");
	}

	/**
	 * Type of ALTER DATABASE operation.
	 */
	public enum AlterHiveDatabaseOp {
		CHANGE_PROPS,
		CHANGE_LOCATION,
		CHANGE_OWNER
	}
}
