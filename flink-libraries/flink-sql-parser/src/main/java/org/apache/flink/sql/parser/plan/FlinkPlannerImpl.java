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

package org.apache.flink.sql.parser.plan;

import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.ddl.SqlNodeInfo;
import org.apache.flink.sql.parser.errorcode.ParserErrors;
import org.apache.flink.sql.parser.node.SqlToTreeConverter;
import org.apache.flink.sql.parser.plan.builder.BlinkRelBuilder;
import org.apache.flink.sql.parser.util.SqlInfo;
import org.apache.flink.sql.parser.util.SqlLists;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Blink Sql planner.
 */
public class FlinkPlannerImpl {

	private RelOptPlanner planner;
	private RelDataTypeFactory typeFactory;
	private SqlOperatorTable operatorTable;
	private List<RelTraitDef> traitDefs;
	private SqlRexConvertletTable convertletTable;
	private SchemaPlus defaultSchema;
	private SqlParser.Config parserConfig;

	private FlinkSqlValidator validator;
	private BlinkRelBuilder relBuilder;

	public FlinkPlannerImpl(FrameworkConfig frameworkConfig) {
		this.relBuilder = BlinkRelBuilder.create(frameworkConfig);

		this.planner = relBuilder.getPlanner();
		this.typeFactory = relBuilder.getTypeFactory();

		this.traitDefs = frameworkConfig.getTraitDefs();
		this.parserConfig = frameworkConfig.getParserConfig();
		this.convertletTable = frameworkConfig.getConvertletTable();
		this.defaultSchema = frameworkConfig.getDefaultSchema();

		SqlOperatorTable builtinOperatorTable = frameworkConfig.getOperatorTable();
		CalciteCatalogReader catalogReader = this.createCatalogReader();
		this.operatorTable = ChainedSqlOperatorTable.of(builtinOperatorTable, catalogReader);

		this.validator = new FlinkSqlValidator(this.operatorTable, catalogReader, this.typeFactory);
		this.validator.setIdentifierExpansion(true);
	}

	public RelDataTypeFactory getTypeFactory() {
		return this.typeFactory;
	}

	public List<SqlNodeInfo> parseContext(String sqlContext) throws SqlParseException {
		this.ready();

		List<SqlInfo> sqlList = SqlLists.getSQLList(sqlContext);
		for (SqlInfo sqlInfo : sqlList) {
			int startLine = sqlInfo.getLine();
			StringBuilder sqlBuilder = new StringBuilder();
			for (int i = 0; i < startLine - 1; i++) {
				sqlBuilder.append('\n');
			}
			String sql = sqlBuilder.append(sqlInfo.getSqlContent()).toString();
			sqlInfo.setSqlContent(sql);
		}

		List<SqlNodeInfo> sqlNodeInfoList = new ArrayList<>();
		Map<String, SqlParserPos> sinkTables = new HashMap<>();
		Set<String> tableNames = new HashSet<>();
		for (SqlInfo sqlInfo : sqlList) {
			if (StringUtils.isBlank(sqlInfo.getSqlContent())) {
				continue;
			}
			SqlNodeInfo sqlNodeInfo = new SqlNodeInfo();
			SqlParser sqlParser = SqlParser.create(sqlInfo.getSqlContent(), parserConfig);
			try {
				SqlNode sqlNode = sqlParser.parseStmt();
				if (sqlNode instanceof SqlCreateView) {
					String subQuerySql = getViewSubQuerySql(sqlInfo, (SqlCreateView) sqlNode);
					((SqlCreateView) sqlNode).setSubQuerySql(subQuerySql);
				} else if (sqlNode instanceof SqlInsert) {
					SqlIdentifier targetTable = (SqlIdentifier) ((SqlInsert) sqlNode).getTargetTable();
					sinkTables.put(targetTable.toString(), targetTable.getParserPosition());
				} else if (sqlNode instanceof SqlCreateTable) {
					String tableName = ((SqlCreateTable) sqlNode).getTableName().toString();
					tableNames.add(tableName);
				}
				sqlNodeInfo.setOriginSql(sqlInfo.getSqlContent());
				sqlNodeInfo.setSqlNode(sqlNode);
				sqlNodeInfoList.add(sqlNodeInfo);
			} catch (org.apache.calcite.sql.parser.SqlParseException e) {
				//String message = StringUtils.substringBeforeLast(e.getMessage().split("\n")[0], "at line");
				//todo log
				//need to cut message?
				//String message = e.getMessage().split("\n")[0];

				throw new SqlParseException(
					e.getPos() == null ? new SqlParserPos(-1, -1) : e.getPos(),
					ParserErrors.INST.parParseContextError(e.getMessage()),
					e);
			}
		}

		//validate sink table names
		for (Map.Entry<String, SqlParserPos> sinkTable : sinkTables.entrySet()) {
			if (!tableNames.contains(sinkTable.getKey())) {
				throw new SqlParseException(sinkTable.getValue(),
					ParserErrors.INST.parParseContextError(
						"Undefined target table [" + sinkTable.getKey() + "]"));
			}
		}
		for (SqlNodeInfo sqlNodeInfo : sqlNodeInfoList) {
			if (sqlNodeInfo.getSqlNode() instanceof SqlCreateTable) {
				SqlCreateTable sqlCreateTable = (SqlCreateTable) sqlNodeInfo.getSqlNode();
				if (sqlCreateTable.getTableType() != null) {
					continue;
				}
				String tableName = sqlCreateTable.getTableName().toString();
				if (sinkTables.containsKey(tableName)) {
					sqlCreateTable.setTableType("SINK");
				} else {
					sqlCreateTable.setTableType("SOURCE");
				}
			}
		}
		return sqlNodeInfoList;
	}

	private String getViewSubQuerySql(SqlInfo sqlInfo, SqlCreateView sqlNode) {
		int lineNum = sqlNode.getQuery().getParserPosition().getLineNum();
		int columnNum = sqlNode.getQuery().getParserPosition().getColumnNum();
		String sql = sqlInfo.getSqlContent();

		BufferedReader br = new BufferedReader(new InputStreamReader(
			new ByteArrayInputStream(sql.getBytes(Charset.forName("utf8"))),
			Charset.forName("utf8")));
		String line;
		int i = 1;
		StringBuilder stringBuilder = new StringBuilder();
		try {
			while ((line = br.readLine()) != null) {
				//System.out.println(line);
				if (i < lineNum) {
					stringBuilder.append("\n");
				} else if (i == lineNum) {
					for (int j = 1; j < columnNum; j++) {
						stringBuilder.append(" ");
					}
					stringBuilder.append(line.substring(columnNum - 1)).append('\n');
				} else {
					stringBuilder.append(line).append('\n');
				}
				i++;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return stringBuilder.toString();
	}

	public RelDataType getRelDataType(SqlNode sqlnode) {
		return this.validator.getValidatedNodeType(sqlnode);
	}

	public boolean validate(List<SqlNodeInfo> sqlNodeInfoList) throws SqlParseException {
		for (SqlNodeInfo nodeInfo : sqlNodeInfoList) {
			SqlNode sqlNode = nodeInfo.getSqlNode();
			if (sqlNode instanceof SqlCreateTable) {
				((SqlCreateTable) sqlNode).validate();
			} else if (sqlNode instanceof SqlCreateFunction) {
				((SqlCreateFunction) sqlNode).validate();
			} else if (sqlNode instanceof SqlCreateView) {
				((SqlCreateView) sqlNode).validate();
			}
		}
		return true;
	}

	public String getJSONPlan(String sql) throws SqlParseException {
		List<SqlNodeInfo> nodeInfos = parseContext(sql);
		SqlToTreeConverter sql2Tree = new SqlToTreeConverter(this.validator);

		// convert to tree
		for (SqlNodeInfo info : nodeInfos) {
			SqlNode node = info.getSqlNode();
			sql2Tree.convertSql(node);
		}

		return sql2Tree.getJSON();
	}

	public RelRoot sqlToRel(SqlNode sqlNode) {
		assert sqlNode != null;

		RexBuilder rexBuilder = this.createRexBuilder();

		RelOptCluster cluster = RelOptCluster.create(this.planner, rexBuilder);
		SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(
			new ViewExpanderImpl(), this.validator, this.createCatalogReader(),
			cluster, this.convertletTable, SqlToRelConverter.Config.DEFAULT);

		RelRoot tempRoot = sqlToRelConverter.convertQuery(sqlNode, false, true);
		tempRoot = tempRoot.withRel(sqlToRelConverter.flattenTypes(tempRoot.project(), true));
		tempRoot = tempRoot.withRel(RelDecorrelator.decorrelateQuery(tempRoot.project()));

		return tempRoot;
	}

	public RelOptPlanner getPlanner() {
		return this.relBuilder.getPlanner();
	}

	class ViewExpanderImpl implements RelOptTable.ViewExpander {

		public RelRoot expandView(
			RelDataType rowType,
			String queryString,
			List<String> schemaPath,
			List<String> list1) {
			SqlParser sqlParser = SqlParser.create(queryString, parserConfig);
			try {
				SqlNode sqlNode = sqlParser.parseQuery();
				CalciteCatalogReader catalogReader = createCatalogReader().withSchemaPath(schemaPath);

				SqlNode validatedSqlNode = validator.validate(sqlNode);

				RexBuilder rexBuilder = createRexBuilder();
				RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

				SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(
					new ViewExpanderImpl(), validator, catalogReader, cluster,
					convertletTable, SqlToRelConverter.Config.DEFAULT);

				// unknown ? TODO
//				root = sqlToRelConverter.convertQuery(validatedSqlNode, true, false);
//				root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true));
//				root = root.withRel(RelDecorrelator.decorrelateQuery(root.rel));
//
//				return root;

				return null;
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	private RexBuilder createRexBuilder() {
		return new RexBuilder(this.typeFactory);
	}

	private CalciteCatalogReader createCatalogReader() {
		SchemaPlus rootSchema = FlinkPlannerImpl.rootSchema(this.defaultSchema);
		Properties prop = new Properties();
		prop.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
			String.valueOf(parserConfig.caseSensitive()));
		CalciteConnectionConfig connectionConfig = new CalciteConnectionConfigImpl(prop);
		return new CalciteCatalogReader(
			CalciteSchema.from(rootSchema),
			CalciteSchema.from(this.defaultSchema).path(null),
			this.typeFactory,
			connectionConfig);
	}

	private static SchemaPlus rootSchema(SchemaPlus schema) {
		if (schema.getParentSchema() == null) {
			return schema;
		} else {
			return rootSchema(schema.getParentSchema());
		}
	}

	private void ready() {
		if (this.traitDefs != null) {
			this.planner.clearRelTraitDefs();
			for (RelTraitDef traitDef : this.traitDefs) {
				this.planner.addRelTraitDef(traitDef);
			}
		}
	}

	public FlinkSqlValidator getValidator() {
		return validator;
	}
}
