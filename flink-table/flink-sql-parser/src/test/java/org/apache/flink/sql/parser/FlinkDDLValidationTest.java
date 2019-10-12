/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.sql.parser;

import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.error.SqlValidateException;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.util.SourceStringReader;
import org.junit.Assert;
import org.junit.Test;

/**
 * Validation tests for ddl fields.
 */
public class FlinkDDLValidationTest {

	protected SqlParserImplFactory parserImplFactory() {
		return FlinkSqlParserImpl.FACTORY;
	}

	private SqlParser getSqlParser(String sql) {
		SourceStringReader source = new SourceStringReader(sql);
		return SqlParser.create(source,
			SqlParser.configBuilder()
				.setParserFactory(parserImplFactory())
				.setQuoting(Quoting.DOUBLE_QUOTE)
				.setUnquotedCasing(Casing.TO_UPPER)
				.setQuotedCasing(Casing.UNCHANGED)
				.build());
	}

	@Test
	public void testSqlCreateTable() throws SqlParseException, SqlValidateException {
		String sql = "CREATE TABLE tbl1";
		SqlCreateTable sqlCreateTable = (SqlCreateTable) getSqlParser(sql).parseStmt();

		sqlCreateTable.validate();
		// verify columnList and propertyList and partitionKeyList not null
		Assert.assertEquals(SqlNodeList.EMPTY, sqlCreateTable.getColumnList());
		Assert.assertEquals(SqlNodeList.EMPTY, sqlCreateTable.getPropertyList());
		Assert.assertEquals(SqlNodeList.EMPTY, sqlCreateTable.getPartitionKeyList());
	}

	@Test
	public void testSqlCreateView() throws SqlParseException, SqlValidateException {
		String sql = "CREATE VIEW v1 AS SELECT 1";
		SqlCreateView sqlCreateView = (SqlCreateView) getSqlParser(sql).parseStmt();

		sqlCreateView.validate();
		// verify fieldList not null
		Assert.assertEquals(SqlNodeList.EMPTY, sqlCreateView.getFieldList());
	}
}
