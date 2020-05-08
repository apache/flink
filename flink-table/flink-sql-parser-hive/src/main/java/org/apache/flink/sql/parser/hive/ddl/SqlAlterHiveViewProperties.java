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

import org.apache.flink.sql.parser.ddl.SqlAlterViewProperties;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * ALTER DDL to change properties of a Hive view.
 *
 * <p>ALTER VIEW [db_name.]view_name SET TBLPROPERTIES table_properties;
 */
public class SqlAlterHiveViewProperties extends SqlAlterViewProperties {

	public SqlAlterHiveViewProperties(SqlParserPos pos, SqlIdentifier tableName, SqlNodeList propertyList) {
		super(pos, tableName, propertyList);
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		writer.keyword("ALTER VIEW");
		viewIdentifier.unparse(writer, leftPrec, rightPrec);
		writer.keyword("SET TBLPROPERTIES");
		SqlWriter.Frame withFrame = writer.startList("(", ")");
		for (SqlNode property : getPropertyList()) {
			printIndent(writer);
			property.unparse(writer, leftPrec, rightPrec);
		}
		writer.newlineAndIndent();
		writer.endList(withFrame);
	}
}
