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

package org.apache.flink.table.client.cli;

import org.apache.flink.table.calcite.FlinkRelBuilder;
import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.calcite.FlinkTypeSystem;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.local.LocalExecutor;
import org.apache.flink.table.plan.cost.DataSetCostFactory;
import org.apache.flink.table.validate.FunctionCatalog;

import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.advise.SqlAdvisor;
import org.apache.calcite.sql.advise.SqlAdvisorValidator;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlMoniker;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;
import org.jline.utils.AttributedString;

import java.util.List;
import java.util.Properties;

/**
 * SQL Completer.
 */
public class SqlCompleter implements Completer {
	private SessionContext context;
	private Executor executor;
	private SqlParser.Config parserConfig = SqlParser
			.configBuilder()
			.setLex(Lex.JAVA)
			.setCaseSensitive(false)
			.build();
	private SqlOperatorTable operatorTable =
			FunctionCatalog.withBuiltIns().getSqlOperatorTable();
	private SchemaPlus rootSchema = CalciteSchema.createRootSchema(true, false).plus();
	private SqlAdvisor advisor;

	public SqlCompleter(SessionContext context, Executor executor) {
		this.context = context;
		this.executor = executor;

		if (executor instanceof LocalExecutor) {
			LocalExecutor localexecutor = (LocalExecutor) executor;
			for (String tableName: localexecutor.listTables(context)) {
				org.apache.calcite.schema.Table table = localexecutor.getTable(context, tableName);
				rootSchema.add(tableName, table);
			}
			FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder()
					.defaultSchema(rootSchema)
					.parserConfig(parserConfig)
					.costFactory(new DataSetCostFactory())
					.typeSystem(new FlinkTypeSystem())
					.operatorTable(operatorTable)
					.build();
			FlinkTypeFactory typeFactory = FlinkRelBuilder.create(frameworkConfig).getTypeFactory();

			Properties prop = new Properties();
			prop.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
					String.valueOf(parserConfig.caseSensitive()));
			CalciteCatalogReader catalogReader = new CalciteCatalogReader(
					CalciteSchema.from(rootSchema),
					CalciteSchema.from(rootSchema).path(null),
					typeFactory,
					new CalciteConnectionConfigImpl(prop)
					);

			SqlAdvisorValidator validator = new SqlAdvisorValidator(operatorTable, catalogReader, typeFactory, SqlConformance.DEFAULT);
			advisor = new SqlAdvisor(validator);
		}
	}

	public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates) {
		if (executor instanceof LocalExecutor) {
			try {
				final String[] replaced = {null};
				List<SqlMoniker> hints = advisor.getCompletionHints(line.line(), line.cursor(), replaced);
				for (SqlMoniker m: hints) {
					candidates.add(new Candidate(AttributedString.stripAnsi(m.toIdentifier().toString()), m.toIdentifier().toString() , null, null, null, null, true));
				}
			} catch (Exception e) {
				//ignore completer exception;
			}
		}
	}
}
