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

package org.apache.flink.table.client.utils;

import org.apache.flink.sql.parser.ddl.SqlAnalyzeTable;
import org.apache.flink.sql.parser.ddl.SqlColumnType;
import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlNodeInfo;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.plan.FlinkPlannerImpl;
import org.apache.flink.sql.parser.plan.SqlParseException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.BatchTableEnvironment;
import org.apache.flink.table.api.Column;
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.functions.UserDefinedFunction;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.DecimalType;
import org.apache.flink.table.api.types.GenericType;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ReadableWritableCatalog;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.errorcode.TableErrors;
import org.apache.flink.table.plan.stats.AnalyzeStatistic;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.runtime.functions.python.PythonUDFUtil;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.util.StringUtils;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlProperty;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Util to transform a sql context (a sequence of sql statements) into a flink job.
 */
public class SqlJobUtil {
	private static final Logger LOG = LoggerFactory.getLogger(SqlJobUtil.class);

	/**
	 * Register an external table to table environment.
	 * @param tableEnv     table environment
	 * @param sqlNodeInfo  external table defined in ddl.
	 */
	public static void registerExternalTable(TableEnvironment tableEnv, SqlNodeInfo sqlNodeInfo) {
		final SqlCreateTable sqlCreateTable = (SqlCreateTable) sqlNodeInfo.getSqlNode();
		final String tableName = sqlCreateTable.getTableName().toString();

		// set with properties
		SqlNodeList propertyList = sqlCreateTable.getPropertyList();
		Map<String, String> properties = new HashMap<>();
		for (SqlNode sqlNode : propertyList) {
			SqlProperty sqlProperty = (SqlProperty) sqlNode;
			properties.put(
					sqlProperty.getKeyString(), sqlProperty.getValueString());
		}
		String tableType = properties.get("type");
		if (StringUtils.isNullOrWhitespaceOnly(tableType)) {
			throw new SqlExecutionException("The type of CREATE TABLE is null");
		}

		TableSchema tableSchema = SqlJobUtil.getTableSchema(tableEnv, sqlCreateTable);

		List<RexNode> exprList = SqlJobUtil.getComputedColumns(tableEnv, sqlCreateTable);
		Map<String, RexNode> computedColumns = null;
		if (exprList != null) {
			computedColumns = new HashMap<>();
			for (int i = 0; i < tableSchema.getColumns().length; i++) {
				computedColumns.put(tableSchema.getColumnName(i), exprList.get(i));
			}
		}

		String rowtimeField = null;
		long offset = -1;
		if (sqlCreateTable.getWatermark() != null) {
			try {
				rowtimeField = sqlCreateTable.getWatermark().getColumnName().toString();
				offset = sqlCreateTable.getWatermark().getWatermarkOffset();
			} catch (SqlParseException e) {
				throw new SqlExecutionException(e.getMessage(), e);
			}
		}

		boolean isStreaming = true;
		if (tableEnv instanceof BatchTableEnvironment) {
			isStreaming = false;
		}

		long now = System.currentTimeMillis();
		CatalogTable catalogTable = new CatalogTable(
				tableType,
				tableSchema,
				properties,
				SqlJobUtil.createBlinkTableSchema(sqlCreateTable),
				null,
				"SQL Client Table",  // TODO We don't have a table comment from DDL
				null,
				false,
				computedColumns,
				rowtimeField,
				offset,
				now,
				now,
				isStreaming);

		ReadableWritableCatalog catalog = tableEnv.getDefaultCatalog();
		// TODO: need to consider if a default db doesn't exist
		catalog.createTable(
			new ObjectPath(tableEnv.getDefaultDatabaseName(), tableName),
			catalogTable,
			false);
	}

	/**
	 * Configuration for sql parser.
	 */
	private static final SqlParser.Config PARSER_CONFIG = SqlParser.configBuilder()
		.setParserFactory(FlinkSqlParserImpl.FACTORY)
		.setQuoting(Quoting.BACK_TICK)
		.setQuotedCasing(Casing.UNCHANGED)
		.setUnquotedCasing(Casing.UNCHANGED)
		.setConformance(SqlConformanceEnum.DEFAULT)
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

	/**
	 * Parses a sql context as a list of {@link SqlNodeInfo}.
	 *
	 * @throws SqlParseException if there is any syntactic error
	 */
	public static List<SqlNodeInfo> parseSqlContext(String sqlContext) throws SqlParseException {
		FlinkPlannerImpl planner = new FlinkPlannerImpl(FRAMEWORK_CONFIG);
		List<SqlNodeInfo> sqlNodeList = planner.parseContext(sqlContext);
		planner.validate(sqlNodeList);
		return sqlNodeList;
	}

	/**
	 * Registers functions to the tableEnvironment.
	 *
	 * @param tableEnv        the {@link TableEnvironment} of the sql job
	 * @param sqlNodeInfoList the parsed result of a sql context
	 * @return true or false
	 */
	public static boolean registerFunctions(
		TableEnvironment tableEnv,
		List<SqlNodeInfo> sqlNodeInfoList,
		String userPyLibs) {

		Map<String, String> pyUdfNameClass = new HashMap<>();
		for (SqlNodeInfo sqlNodeInfo : sqlNodeInfoList) {
			if (sqlNodeInfo.getSqlNode() instanceof SqlCreateFunction) {
				SqlCreateFunction sqlCreateFunction = (SqlCreateFunction) sqlNodeInfo.getSqlNode();
				String functionName = sqlCreateFunction.getFunctionName().toString();

				boolean isPyUdf = sqlCreateFunction.getClassName().startsWith(PythonUDFUtil.PYUDF_PREFIX);
				if (isPyUdf) {
					String className = sqlCreateFunction.getClassName()
											.substring(PythonUDFUtil.PYUDF_PREFIX.length());
					pyUdfNameClass.put(functionName, className);
					continue;
				}

				// Register in catalog
				// TODO: [BLINK-18570607] re-enable register external functions in SqlJobUtil
//				tableEnv.registerExternalFunction(
//					null, functionName, sqlCreateFunction.getClassName(), false);
				throw new UnsupportedOperationException(
					"catalogs haven't support registering functions yet");
			}
		}

		if (pyUdfNameClass.size() > 0) {
			ArrayList<String> pyFiles = new ArrayList<>(Arrays.asList(userPyLibs.split(",")));
			Map<String, UserDefinedFunction> pyUDFs =
				PythonUDFUtil.registerPyUdfsToTableEnvironment(tableEnv, pyFiles, pyUdfNameClass);
			// TODO How to handle the python function?
		}
		return true;
	}

	public static RichTableSchema createBlinkTableSchema(SqlCreateTable sqlCreateTable) {
		if (!"SOURCE".equals(sqlCreateTable.getTableType())) {
			if (sqlCreateTable.getWatermark() != null) {
				throw new IllegalArgumentException(
					TableErrors.INST.sqlTableTypeNotSupportWaterMark(
						sqlCreateTable.getTableName().toString(),
						sqlCreateTable.getTableType()));
			}
			if (sqlCreateTable.containsComputedColumn()) {
				throw new IllegalArgumentException(
					TableErrors.INST.sqlTableTypeNotSupportComputedCol(
						sqlCreateTable.getTableName().toString(),
						sqlCreateTable.getTableType()));
			}
		}

		//set columnList
		SqlNodeList columnList = sqlCreateTable.getColumnList();
		int columnCount = columnList.size();
		String[] columnNames = new String[columnCount];
		boolean[] nullables = new boolean[columnCount];
		InternalType[] columnTypes = new InternalType[columnCount];
		List<String> headerFields = new ArrayList<>();
		RichTableSchema schema;

		if (!sqlCreateTable.containsComputedColumn()) {
			// all column is SqlTableColumn
			for (int i = 0; i < columnCount; i++) {
				SqlTableColumn columnNode = (SqlTableColumn) columnList.get(i);
				String columnName = columnNode.getName().getSimple();
				columnNames[i] = columnName;
				try {
					columnTypes[i] = getInternalType(columnNode.getType());
				} catch (IllegalArgumentException e) {
					throw new IllegalArgumentException(
						TableErrors.INST.sqlUnSupportedColumnType(
							sqlCreateTable.getTableName().toString(),
							columnName,
							columnNode.getType().getTypeName().getSimple()));
				}
				nullables[i] = columnNode.getType().getNullable() == null ? true : columnNode.getType().getNullable();
				if (columnNode.isHeader()) {
					headerFields.add(columnName);
				}
			}
			schema = new RichTableSchema(columnNames, columnTypes, nullables);
		} else {
			// some columns are computed column
			List<String> originNameList = new ArrayList<>();
			List<InternalType> originTypeList = new ArrayList<>();
			List<Boolean> originNullableList = new ArrayList<>();
			for (int i = 0; i < columnCount; i++) {
				SqlNode node = columnList.get(i);
				if (node instanceof SqlTableColumn) {
					SqlTableColumn columnNode = (SqlTableColumn) columnList.get(i);
					String columnName = columnNode.getName().getSimple();
					try {
						InternalType columnType = getInternalType(columnNode.getType());
						originTypeList.add(columnType);
					} catch (IllegalArgumentException e) {
						throw new IllegalArgumentException(
							TableErrors.INST.sqlUnSupportedColumnType(
								sqlCreateTable.getTableName().toString(),
								columnName,
								columnNode.getType().getTypeName().getSimple()));
					}
					originNameList.add(columnName);
					originNullableList.add(columnNode.getType().getNullable() == null ? true : columnNode.getType().getNullable());
					if (columnNode.isHeader()) {
						headerFields.add(columnName);
					}
				}
			}
			String[] originColumnNames = originNameList.toArray(new String[originNameList.size()]);
			InternalType[] originColumnTypes = originTypeList.toArray(new InternalType[originTypeList.size()]);
			boolean[] originNullables = new boolean[originNullableList.size()];
			for (int i = 0; i < originNullables.length; i++) {
				originNullables[i] = originNullableList.get(i);
			}
			schema = new RichTableSchema(originColumnNames, originColumnTypes, originNullables);
		}

		schema.setHeaderFields(headerFields);

		//set primary key
		if (sqlCreateTable.getPrimaryKeyList() != null) {
			String[] primaryKeys = new String[sqlCreateTable.getPrimaryKeyList().size()];
			for (int i = 0; i < primaryKeys.length; i++) {
				primaryKeys[i] = sqlCreateTable.getPrimaryKeyList().get(i).toString();
			}
			schema.setPrimaryKey(primaryKeys);
		}

		//set unique key
		List<SqlNodeList> uniqueKeyList = sqlCreateTable.getUniqueKeysList();
		if (uniqueKeyList != null) {
			List<List<String>> ukList = new ArrayList<>();
			for (SqlNodeList uniqueKeys: uniqueKeyList) {
				List<String> uk = new ArrayList<>();
				for (int i = 0; i < uniqueKeys.size(); i++) {
					uk.add(uniqueKeys.get(i).toString());
				}
				ukList.add(uk);
			}
			schema.setUniqueKeys(ukList);
		}

		//set index
		List<SqlCreateTable.IndexWrapper> indexKeyList = sqlCreateTable.getIndexKeysList();
		if (indexKeyList != null) {
			List<RichTableSchema.Index> indexes = new ArrayList<>();
			for (SqlCreateTable.IndexWrapper idx : indexKeyList) {
				List<String> keyList = new ArrayList<>();
				for (int i = 0; i < idx.indexKeys.size(); i++) {
					keyList.add(idx.indexKeys.get(i).toString());
				}
				indexes.add(new RichTableSchema.Index(idx.unique, keyList));
			}
			schema.setIndexes(indexes);
		}
		return schema;
	}

	/**
	 * Analyze table statistics.
	 *
	 * @param tableEnv        the {@link TableEnvironment} of the sql job
	 * @param sqlNodeInfoList the parsed result of a sql context
	 */
	public static void analyzeTableStats(TableEnvironment tableEnv, List<SqlNodeInfo> sqlNodeInfoList) {
		for (SqlNodeInfo sqlNodeInfo : sqlNodeInfoList) {
			if (sqlNodeInfo.getSqlNode() instanceof SqlAnalyzeTable) {
				SqlAnalyzeTable sqlAnalyzeTable = (SqlAnalyzeTable) sqlNodeInfo.getSqlNode();
				String[] tablePath = sqlAnalyzeTable.getTableName().names.toArray(new String[] {});
				String[] columnNames = getColumnsToAnalyze(sqlAnalyzeTable);
				TableStats tableStats = AnalyzeStatistic.generateTableStats(tableEnv, tablePath, columnNames);
				tableEnv.alterTableStats(tablePath, tableStats);
			}
		}
	}

	private static String[] getColumnsToAnalyze(SqlAnalyzeTable analyzeTable) {
		if (!analyzeTable.isWithColumns()) {
			return new String[] {};
		}
		SqlNodeList columnList = analyzeTable.getColumnList();
		int columnCount = columnList.size();
		// analyze all columns or specified columns.
		if (columnCount == 0) {
			return new String[] {"*"};
		}
		String[] columnNames = new String[columnCount];
		for (int i = 0; i < columnCount; i++) {
			SqlIdentifier column = (SqlIdentifier) columnList.get(i);
			columnNames[i] = column.getSimple();
		}
		return columnNames;
	}

	/**
	 * Maps a sql column type to a flink {@link InternalType}.
	 *
	 * @param type the sql column type
	 * @return the corresponding flink type
	 */
	public static InternalType getInternalType(SqlDataTypeSpec type) {
		switch (SqlColumnType.getType(type.getTypeName().getSimple())) {
			case BOOLEAN:
				return DataTypes.BOOLEAN;
			case TINYINT:
				return DataTypes.BYTE;
			case SMALLINT:
				return DataTypes.SHORT;
			case INT:
				return DataTypes.INT;
			case BIGINT:
				return DataTypes.LONG;
			case FLOAT:
				return DataTypes.FLOAT;
			case DECIMAL:
				if (type.getPrecision() >= 0) {
					return DecimalType.of(type.getPrecision(), type.getScale());
				}
				return DecimalType.USER_DEFAULT;
			case DOUBLE:
				return DataTypes.DOUBLE;
			case DATE:
				return DataTypes.DATE;
			case TIME:
				return DataTypes.TIME;
			case TIMESTAMP:
				return DataTypes.TIMESTAMP;
			case VARCHAR:
				return DataTypes.STRING;
			case VARBINARY:
				return DataTypes.BYTE_ARRAY;
			case ANY:
				return new GenericType<>(Object.class);
			default:
				LOG.warn("Unsupported sql column type: {}", type);
				throw new IllegalArgumentException("Unsupported sql column type " + type + " !");
		}
	}

	public static List<RexNode> getComputedColumns(
			TableEnvironment tEnv, SqlCreateTable sqlCreateTable) {

		RichTableSchema richTableSchema = createBlinkTableSchema(sqlCreateTable);
		if (!sqlCreateTable.containsComputedColumn()) {
			return null;
		}
		TableSchema originalSchema = new TableSchema(
				richTableSchema.getColumnNames(),
				richTableSchema.getColumnTypes(),
				richTableSchema.getNullables());

		String name = tEnv.createUniqueTableName();
		tEnv.registerTableSource(
				name, new MockTableSource(name, originalSchema));
		String viewSql = "select " + sqlCreateTable.getColumnSqlString() + " from " + name;
		Table viewTable = tEnv.sqlQuery(viewSql);

		LogicalProject project = (LogicalProject) viewTable.getRelNode();
		List<RexNode> exprs = project.getProjects();
		return exprs;
	}

	public static TableSchema getTableSchema(TableEnvironment tEnv, SqlCreateTable sqlCreateTable) {
		RichTableSchema richTableSchema = createBlinkTableSchema(sqlCreateTable);
		String rowtimeField = null;
		if (sqlCreateTable.getWatermark() != null) {
			rowtimeField = sqlCreateTable.getWatermark().getColumnName().toString();
		}

		TableSchema.Builder builder = TableSchema.builder();
		if (sqlCreateTable.containsComputedColumn()) {
			TableSchema originalSchema = new TableSchema(
					richTableSchema.getColumnNames(),
					richTableSchema.getColumnTypes(),
					richTableSchema.getNullables());

			String name = tEnv.createUniqueTableName();
			tEnv.registerTableSource(
					name, new MockTableSource(name, originalSchema));
			String viewSql = "select " + sqlCreateTable.getColumnSqlString() + " from " + name;
			Table viewTable = tEnv.sqlQuery(viewSql);

			TableSchema schemaWithComputedColumn = viewTable.getSchema();

			for (int i = 0; i < schemaWithComputedColumn.getColumns().length; i++) {
				Column col = schemaWithComputedColumn.getColumn(i);
				if (col.name().equals(rowtimeField)) {
					builder.field(
							rowtimeField,
							DataTypes.ROWTIME_INDICATOR,
							false);
				} else {
					builder.field(
							col.name(),
							col.internalType(),
							col.isNullable());
				}
			}
		} else {
			for (int i = 0; i < richTableSchema.getColumnNames().length; i++) {
				if (richTableSchema.getColumnNames()[i].equals(rowtimeField)) {
					builder.field(rowtimeField, DataTypes.ROWTIME_INDICATOR, false);
				} else {
					builder.field(
							richTableSchema.getColumnNames()[i],
							richTableSchema.getColumnTypes()[i],
							richTableSchema.getNullables()[i]);
				}
			}
		}

		builder.primaryKey(richTableSchema.getPrimaryKeys().stream().toArray(String[]::new));
		for (List<String> uniqueKey: richTableSchema.getUniqueKeys()) {
			builder.uniqueIndex(uniqueKey.stream().toArray(String[]::new));
		}
		return builder.build();
	}

	private static class MockTableSource
			implements BatchTableSource<BaseRow>, StreamTableSource<BaseRow> {

		private String name;
		private TableSchema schema;

		public MockTableSource(String name, TableSchema tableSchema) {
			this.name = name;
			this.schema = tableSchema;
		}

		@Override
		public DataStream<BaseRow> getBoundedStream(StreamExecutionEnvironment streamEnv) {
			return null;
		}

		@Override
		public DataType getReturnType() {
			return DataTypes.createRowType(schema.getTypes(), schema.getColumnNames());
		}

		@Override
		public TableSchema getTableSchema() {
			return schema;
		}

		@Override
		public String explainSource() {
			return name;
		}

		@Override
		public TableStats getTableStats() {
			return null;
		}

		@Override
		public DataStream<BaseRow> getDataStream(StreamExecutionEnvironment execEnv) {
			return null;
		}
	}
}
