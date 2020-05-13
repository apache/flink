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

import org.apache.flink.sql.parser.hive.impl.ParseException;

import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * ALTER DDL to update columns of a Hive table.
 */
public abstract class SqlAlterHiveTableColumn extends SqlAlterHiveTable {

	public static final String ALTER_COL_CASCADE = "alter.column.cascade";

	protected final SqlNodeList partSpec;
	protected final boolean cascade;

	public SqlAlterHiveTableColumn(AlterTableOp op, SqlParserPos pos, SqlIdentifier tableName,
			SqlNodeList partSpec, SqlNodeList propertyList, boolean cascade) throws ParseException {
		super(op, pos, tableName, propertyList);
		if (cascade && partSpec != null) {
			throw new ParseException("Alter partition doesn't support cascade");
		}
		this.partSpec = partSpec;
		this.cascade = cascade;
		addProps(propertyList);
	}

	private void addProps(SqlNodeList props) {
		if (cascade) {
			props.add(HiveDDLUtils.toTableOption(ALTER_COL_CASCADE, "true", pos));
		}
	}

	@Override
	public SqlNodeList getPartitionSpec() {
		// can be partial spec in which case all partitions matching the spec will be updated
		return partSpec;
	}

	protected String toTypeString(SqlDataTypeSpec typeSpec) {
		return typeSpec.toString();
	}
}
