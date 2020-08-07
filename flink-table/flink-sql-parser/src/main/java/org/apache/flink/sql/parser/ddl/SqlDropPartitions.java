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

import org.apache.flink.sql.parser.SqlPartitionUtils;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * ALTER TABLE DDL to drop partitions of a table.
 */
public class SqlDropPartitions extends SqlAlterTable {

	private final boolean ifExists;
	private final List<SqlNodeList> partSpecs;

	public SqlDropPartitions(SqlParserPos pos, SqlIdentifier tableName, boolean ifExists,
			List<SqlNodeList> partSpecs) {
		super(pos, tableName);
		this.ifExists = ifExists;
		this.partSpecs = partSpecs;
	}

	public boolean ifExists() {
		return ifExists;
	}

	public List<SqlNodeList> getPartSpecs() {
		return partSpecs;
	}

	public LinkedHashMap<String, String> getPartitionKVs(int i) {
		return SqlPartitionUtils.getPartitionKVs(getPartSpecs().get(i));
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		super.unparse(writer, leftPrec, rightPrec);
		writer.newlineAndIndent();
		writer.keyword("DROP");
		if (ifExists) {
			writer.keyword("IF EXISTS");
		}
		int opLeftPrec = getOperator().getLeftPrec();
		int opRightPrec = getOperator().getRightPrec();
		for (SqlNodeList partSpec : partSpecs) {
			writer.newlineAndIndent();
			writer.keyword("PARTITION");
			partSpec.unparse(writer, opLeftPrec, opRightPrec);
		}
	}

	@Nonnull
	@Override
	public List<SqlNode> getOperandList() {
		List<SqlNode> operands = new ArrayList<>();
		operands.add(tableIdentifier);
		operands.addAll(partSpecs);
		return operands;
	}
}
