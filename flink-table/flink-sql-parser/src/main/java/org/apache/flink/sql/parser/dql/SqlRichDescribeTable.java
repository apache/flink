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
 * DESCRIBE [ EXTENDED] [[catalogName.] dataBasesName].sqlIdentifier sql call.
 * Here we add Rich in className to distinguish from calcite's original SqlDescribeTable.
 */
public class SqlRichDescribeTable extends SqlCall {

	public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("DESCRIBE TABLE", SqlKind.DESCRIBE_TABLE);
	protected final SqlIdentifier tableNameIdentifier;
	private boolean isExtended = false;

	public SqlRichDescribeTable(SqlParserPos pos, SqlIdentifier tableNameIdentifier, boolean isExtended) {
		super(pos);
		this.tableNameIdentifier = tableNameIdentifier;
		this.isExtended = isExtended;
	}

	@Override
	public SqlOperator getOperator() {
		return OPERATOR;
	}

	@Override
	public List<SqlNode> getOperandList() {
		return Collections.singletonList(tableNameIdentifier);
	}

	public boolean isExtended() {
		return isExtended;
	}

	public String[] fullTableName() {
		return tableNameIdentifier.names.toArray(new String[0]);
	}

	@Override
	public void unparse(
			SqlWriter writer,
			int leftPrec,
			int rightPrec) {
		writer.keyword("DESCRIBE");
		if (isExtended) {
			writer.keyword("EXTENDED");
		}
		tableNameIdentifier.unparse(writer, leftPrec, rightPrec);
	}
}
