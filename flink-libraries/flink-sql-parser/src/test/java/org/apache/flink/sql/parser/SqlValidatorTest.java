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

package org.apache.flink.sql.parser;

import org.apache.flink.sql.parser.ddl.SqlNodeInfo;
import org.apache.flink.sql.parser.plan.FlinkPlannerImpl;
import org.apache.flink.sql.parser.plan.SqlParseException;

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 *
 */
public class SqlValidatorTest extends ParserTestBase {

	@Test
	public void testCreateAndInsert() throws SqlParseException {
		List<SqlNodeInfo> nodes = validateSqlContext(
			"create table sls_stream1(\n" +
				"  a bigint,\n" +
				"  b VARCHAR,\n" +
				"  PRIMARY KEY(a, b),\n" +
				"  WATERMARK wk FOR a AS withd(b, 1000)\n" +
				") with ( x = 'y', asd = 'dada');\n" +
				"create table rds_output(\n" +
				"  a VARCHAR,\n" +
				"  b bigint\n" +
				");\n" +
				"insert into rds_output\n" +
				"SELECT \n" +
				"  b,\n" +
				"  SUM(a)\n" +
				"FROM sls_stream1");
		assertTrue(nodes.stream().anyMatch(node -> node.getSqlNode() instanceof SqlInsert));
	}

	private List<SqlNodeInfo> validateSqlContext(String sqlContext) throws SqlParseException {
		FrameworkConfig frameworkConfig = Frameworks
			.newConfigBuilder()
			.defaultSchema(Frameworks.createRootSchema(true))
			.parserConfig(getParserConfig())
			.typeSystem(RelDataTypeSystem.DEFAULT)
			.build();

		FlinkPlannerImpl flinkPlannerImpl = new FlinkPlannerImpl(frameworkConfig);
		List<SqlNodeInfo> sqlNodeInfoList = flinkPlannerImpl.parseContext(sqlContext);
		flinkPlannerImpl.validate(sqlNodeInfoList);
		return sqlNodeInfoList;
	}

	@Test
	public void testCreateAndSelect() throws SqlParseException {
		List<SqlNodeInfo> nodes = validateSqlContext(
			"create table sls_stream1(\n" +
				"  a bigint,\n" +
				"  b VARCHAR,\n" +
				"  PRIMARY KEY(a, b),\n" +
				"  WATERMARK wk FOR a AS withd(b, 1000)\n" +
				") with ( x = 'y', asd = 'dada');\n" +
				"SELECT \n" +
				"  b,\n" +
				"  SUM(a)\n" +
				"FROM sls_stream1");
		assertTrue(nodes.stream().anyMatch(node -> node.getSqlNode() instanceof SqlSelect));
	}

	@Test
	public void testLimit() throws SqlParseException {
		List<SqlNodeInfo> nodes = validateSqlContext(
			"SELECT \n" +
				"  b,\n" +
				"  SUM(a)\n" +
				"FROM sls_stream1 LIMIT 100");
		assertTrue(nodes.stream().anyMatch(node -> node.getSqlNode() instanceof SqlOrderBy));
	}
}
