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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

import static java.util.Objects.requireNonNull;


/**
 * Watermark statement in CREATE TABLE DDL, e.g. {@code WATERMARK FOR ts AS ts - INTERVAL '5' SECOND}.
 */
public class SqlWatermark extends SqlCall {

	private static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("WATERMARK", SqlKind.OTHER);

	private final SqlIdentifier eventTimeColumnName;
	private final SqlNode watermarkStrategy;

	public SqlWatermark(SqlParserPos pos, SqlIdentifier eventTimeColumnName, SqlNode watermarkStrategy) {
		super(pos);
		this.eventTimeColumnName = requireNonNull(eventTimeColumnName);
		this.watermarkStrategy = requireNonNull(watermarkStrategy);
	}

	@Override
	public SqlOperator getOperator() {
		return OPERATOR;
	}

	@Override
	public List<SqlNode> getOperandList() {
		return ImmutableNullableList.of(eventTimeColumnName, watermarkStrategy);
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		writer.keyword("WATERMARK");
		writer.keyword("FOR");
		eventTimeColumnName.unparse(writer, leftPrec, rightPrec);
		writer.keyword("AS");
		watermarkStrategy.unparse(writer, leftPrec, rightPrec);
	}

	public SqlIdentifier getEventTimeColumnName() {
		return eventTimeColumnName;
	}

	public SqlNode getWatermarkStrategy() {
		return watermarkStrategy;
	}
}
