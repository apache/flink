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

package org.apache.flink.sql.parser.util;

import org.apache.flink.sql.parser.ddl.SqlNodeInfo;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.plan.FlinkPlannerImpl;
import org.apache.flink.sql.parser.plan.SqlParseException;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;

import java.util.List;

/**
 * A util that provides static methods to validate full sql context.
 */
public class SqlContextValidator {

	private static final SqlParser.Config PARSER_CONFIG = SqlParser.configBuilder()
		.setParserFactory(FlinkSqlParserImpl.FACTORY)
		.setQuoting(Quoting.BACK_TICK)
		.setQuotedCasing(Casing.UNCHANGED)
		.setUnquotedCasing(Casing.UNCHANGED)
		.setIdentifierMaxLength(256)
		.setLex(Lex.JAVA)
		.build();

	private static final SchemaPlus ROOT_SCHEMA = Frameworks.createRootSchema(true);

	private static final FrameworkConfig FRAMEWORK_CONFIG = Frameworks
		.newConfigBuilder()
		.defaultSchema(ROOT_SCHEMA)
		.parserConfig(PARSER_CONFIG)
		.typeSystem(RelDataTypeSystem.DEFAULT)
		.build();

	public static void validateSqlContext(String sqlContext) throws SqlParseException {
		FlinkPlannerImpl flinkPlannerImpl = new FlinkPlannerImpl(FRAMEWORK_CONFIG);
		List<SqlNodeInfo> sqlNodeInfoList = flinkPlannerImpl.parseContext(sqlContext);
		flinkPlannerImpl.validate(sqlNodeInfoList);
	}

	public static void main(String[] args) throws SqlParseException {

		validateSqlContext(
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
	}

}
