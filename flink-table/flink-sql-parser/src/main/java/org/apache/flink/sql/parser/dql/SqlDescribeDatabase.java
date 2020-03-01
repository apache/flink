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

package org.apache.flink.sql.parser.dql;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;

/**
 * DESCRIBE DATABASE [ EXTENDED] [ databaseName.] dataBasesName sql call.
 */
public class SqlDescribeDatabase extends SqlCall {

	public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("DESCRIBE DATABASE", SqlKind.OTHER);
	private final SqlIdentifier databaseName;
	private boolean isExtended = false;

	public SqlDescribeDatabase(SqlParserPos pos, SqlIdentifier databaseName, boolean isExtended) {
		super(pos);
		this.databaseName = databaseName;
		this.isExtended = isExtended;
	}

	@Override
	public SqlOperator getOperator() {
		return OPERATOR;
	}

	@Override
	public List<SqlNode> getOperandList() {
		return Collections.singletonList(databaseName);
	}

	public String getDatabaseName() {
		return databaseName.getSimple();
	}

	public boolean isExtended() {
		return isExtended;
	}

	public String[] fullDatabaseName() {
		return databaseName.names.toArray(new String[0]);
	}

	@Override
	public void unparse(
			SqlWriter writer,
			int leftPrec,
			int rightPrec) {
		writer.keyword("DESCRIBE DATABASE");
		if (isExtended) {
			writer.keyword("EXTENDED");
		}
		databaseName.unparse(writer, leftPrec, rightPrec);
	}
}
