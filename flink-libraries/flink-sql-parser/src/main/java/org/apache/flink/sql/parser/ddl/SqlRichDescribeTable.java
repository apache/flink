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

import org.apache.calcite.sql.SqlDescribeTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

/**
 * DESCRIBE EXTENDED TABLE OR COLUMN.
 * Syntax:
 * 1. Describe table:
 *    DESCRIBE [EXTENDED|FORMATTED] table_name
 * 2. Describe column:
 *    DESCRIBE [EXTENDED|FORMATTED] table_name column_name
 *
 * <p>Examples:
 *
 * <p>t1: a: INT, b: DOUBLE, c: VARCHAR(32)
 *
 * <p>Original records:
 * +-----+-----+-------+
 * |  a  |  b  |  c    |
 * +-----+-----+-------+
 * |  1  | 2d  |"a1"   |
 * +-----+-----+-------+
 * |  1  | 3.1d|"a2222"|
 * +-----+-----+-------+
 * |  2  | null|  "a3" |
 * +-----+-----+-------+
 *
 * <p>Example1:
 *
 * <p>SQL:
 * DESCRIBE t1
 * -- which has same results with : DESCRIBE TABLE t1
 * +-------------+-------------+-------------+
 * | column_name | column_type | is_nullable |
 * +-------------+-------------+-------------+
 * |  a          | INTEGER     |     YES     |
 * +-------------+-------------+-------------+
 * |  b          | DOUBLE      |     YES     |
 * +-------------+-------------+-------------+
 * |  c          | VARCHAR     |     YES     |
 * +-------------+-------------+-------------+
 *
 * <p>DESCRIBE EXTENDED t1
 * -- which has same results with : DESCRIBE FORMATTED t1
 * +----------------------------+-------------+-------------+
 * | column_name                | column_type | is_nullable |
 * +----------------------------+-------------+-------------+
 * |  a                         | INTEGER     |     YES     |
 * +----------------------------+-------------+-------------+
 * |  b                         | DOUBLE      |     YES     |
 * +----------------------------+-------------+-------------+
 * |  c                         | VARCHAR     |     YES     |
 * +----------------------------+-------------+-------------+
 * |                            |             |             |
 * +----------------------------+-------------+-------------+
 * |# Detailed Table Information|             |             |
 * +----------------------------+-------------+-------------+
 * | table_name                 |  t1         |             |
 * +----------------------------+-------------+-------------+
 * | row_count                  |  NULL       |             |
 * +----------------------------+-------------+-------------+
 *
 * <p>DESCRIBE t1 a
 * -- which has same results with : DESCRIBE TABLE t1 a
 * +-------------+-------------+
 * |   info_name |  info_value |
 * +-------------+-------------+
 * | column_name |      a      |
 * +-------------+-------------+
 * | column_type |   INTEGER   |
 * +-------------+-------------+
 * | is_nullable |     YES     |
 * +-------------+-------------+
 *
 * <p>DESCRIBE EXTENDED t1 a
 * -- which has same results with : DESCRIBE FORMATTED TABLE t1 a
 * +-------------+-------------+
 * |   info_name |  info_value |
 * +-------------+-------------+
 * | column_name |      a      |
 * +-------------+-------------+
 * | column_type |   INTEGER   |
 * +-------------+-------------+
 * | is_nullable |     YES     |
 * +-------------+-------------+
 * |     ndv     |       2     |
 * +-------------+-------------+
 * | null_count  |       0     |
 * +-------------+-------------+
 * |   avg_len   |      4.0    |
 * +-------------+-------------+
 * |   max_len   |      4      |
 * +-------------+-------------+
 * |     max     |      2      |
 * +-------------+-------------+
 * |     min     |      1      |
 * +-------------+-------------+

 */
public class SqlRichDescribeTable extends SqlDescribeTable {

	public static final SqlSpecialOperator OPERATOR =
			new SqlSpecialOperator("DESCRIBE_TABLE", SqlKind.DESCRIBE_TABLE);

	private boolean isExtended;

	private boolean isFormatted;

	private SqlIdentifier table;

	private SqlIdentifier column;

	public SqlRichDescribeTable(SqlParserPos pos,
			SqlIdentifier table, SqlIdentifier column, boolean isExtended, boolean isFormatted) {
		super(pos, table, column);
		this.table = table;
		this.column = column;
		this.isExtended = isExtended;
		this.isFormatted = isFormatted;
	}

	@Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		writer.keyword("DESCRIBE");
		if (isExtended) {
			writer.keyword("EXTENDED");
		}
		if (isFormatted) {
			writer.keyword("FORMATTED");
		}
		writer.keyword("TABLE");
		table.unparse(writer, leftPrec, rightPrec);
		if (column != null) {
			column.unparse(writer, leftPrec, rightPrec);
		}
	}

	@Override public void setOperand(int i, SqlNode operand) {
		throw new UnsupportedOperationException();
	}

	@Override public SqlOperator getOperator() {
		return OPERATOR;
	}

	@Override public List<SqlNode> getOperandList() {
		return null;
	}

	@Override public SqlIdentifier getTable() {
		return this.table;
	}

	@Override public SqlIdentifier getColumn() {
		return this.column;
	}

	public boolean getIsExtended() {
		return isExtended;
	}

	public boolean getIsFormatted() {
		return isFormatted;
	}

}
