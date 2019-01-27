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

import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlNodeInfo;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.plan.FlinkPlannerImpl;
import org.apache.flink.sql.parser.plan.SqlParseException;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlProperty;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A class providing static methods to deal with full sql context text.
 */
public class SqlContextUtils {

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

	public static String getSqlTreeJson(String sql) throws SqlParseException {
		FlinkPlannerImpl flinkPlannerImpl = new FlinkPlannerImpl(FRAMEWORK_CONFIG);
		return flinkPlannerImpl.getJSONPlan(sql);
	}

	public static List<String> extractConnectorTypes(String sqlContext) throws SqlParseException {
		List<String> connectorTypes = Lists.newArrayList();
		FlinkPlannerImpl flinkPlannerImpl = new FlinkPlannerImpl(FRAMEWORK_CONFIG);
		List<SqlNodeInfo> sqlNodeInfoList = flinkPlannerImpl.parseContext(sqlContext);
		for (SqlNodeInfo sqlNodeInfo : sqlNodeInfoList) {
			if (sqlNodeInfo.getSqlNode() instanceof SqlCreateTable) {
				SqlCreateTable sqlCreateTable = (SqlCreateTable) sqlNodeInfo.getSqlNode();
				SqlNodeList propertyList = sqlCreateTable.getPropertyList();
				if (propertyList != null) {
					for (SqlNode sqlNode : sqlCreateTable.getPropertyList()) {
						String key = ((SqlProperty) sqlNode).getKeyString();
						if ("type".equals(key)) {
							String type = ((SqlProperty) sqlNode).getValueString().toLowerCase();
							if (!connectorTypes.contains(type)) {
								connectorTypes.add(type);
							}
						}
					}
				}
			}
		}
		return connectorTypes;
	}

	public static void extractTables(
		String sqlContext,
		List<TableInfo> sourceTables,
		List<TableInfo> sinkTables) throws SqlParseException {
		FlinkPlannerImpl flinkPlannerImpl = new FlinkPlannerImpl(FRAMEWORK_CONFIG);
		List<SqlNodeInfo> sqlNodeInfoList = flinkPlannerImpl.parseContext(sqlContext);

		//validate
		flinkPlannerImpl.validate(sqlNodeInfoList);

		//extract table info
		for (SqlNodeInfo sqlNodeInfo : sqlNodeInfoList) {
			if (sqlNodeInfo.getSqlNode() instanceof SqlCreateTable) {
				SqlCreateTable sqlCreateTable = (SqlCreateTable) sqlNodeInfo.getSqlNode();
				switch (sqlCreateTable.getTableType()) {
					case "SOURCE":
						sourceTables.add(extractTableInfo(sqlCreateTable));
						break;
					case "SINK":
						sinkTables.add(extractTableInfo(sqlCreateTable));
						break;
					default:
				}
			}
		}
	}

	private static TableInfo extractTableInfo(SqlCreateTable sqlCreateTable) {
		TableInfo tableInfo = new TableInfo();
		tableInfo.tableName = sqlCreateTable.getTableName().getSimple();
		tableInfo.tableType = sqlCreateTable.getTableType();
		//properties
		if (sqlCreateTable.getPropertyList() != null) {
			Map<String, Object> properties = new HashMap<>();
			for (SqlNode sqlNode : sqlCreateTable.getPropertyList()) {
				SqlProperty sqlProperty = (SqlProperty) sqlNode;
				properties.put(sqlProperty.getKeyString().toLowerCase(), sqlProperty.getValueString());
			}
			tableInfo.properties = properties;
			tableInfo.storageType = (String) properties.get("type");
		}
		//columnList
		tableInfo.columnInfoList = new ArrayList<>();
		for (SqlNode node : sqlCreateTable.getColumnList().getList()) {
			if (node != null && node instanceof SqlTableColumn) {
				SqlTableColumn column = (SqlTableColumn) node;
				ColumnInfo columnInfo = new ColumnInfo();
				columnInfo.columnName = column.getName().getSimple();
				columnInfo.columnType = column.getType().getTypeName().getSimple();
				tableInfo.columnInfoList.add(columnInfo);
			}
		}
		return tableInfo;
	}

	/**
	 * A wrapper of table properties.
	 */
	public static class TableInfo {
		private String tableName;
		private String tableType;
		private List<ColumnInfo> columnInfoList;
		private String storageType;
		private Map<String, Object> properties;

		public String getTableName() {
			return tableName;
		}

		public List<ColumnInfo> getColumnInfoList() {
			return columnInfoList;
		}

		public String getTableType() {
			return tableType;
		}

		public void setTableType(String tableType) {
			this.tableType = tableType;
		}

		public String getStorageType() {
			return storageType;
		}

		public void setStorageType(String storageType) {
			this.storageType = storageType;
		}

		public Map<String, Object> getProperties() {
			return properties;
		}

		public void setProperties(Map<String, Object> properties) {
			this.properties = properties;
		}
	}

	/**
	 * A wrapper of a single column.
	 */
	public static class ColumnInfo {
		private String columnName;
		private String columnType;

		public ColumnInfo(String columnName, String columnType) {
			this.columnName = columnName;
			this.columnType = columnType;
		}

		public ColumnInfo() {
		}

		public String getColumnName() {
			return columnName;
		}

		public String getColumnType() {
			return columnType;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			ColumnInfo that = (ColumnInfo) o;

			return columnName.equals(that.columnName) && columnType.equals(that.columnType);
		}

		@Override
		public int hashCode() {
			int result = columnName != null ? columnName.hashCode() : 0;
			result = 31 * result + (columnType != null ? columnType.hashCode() : 0);
			return result;
		}
	}

}
