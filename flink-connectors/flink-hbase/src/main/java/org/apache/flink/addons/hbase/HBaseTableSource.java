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

package org.apache.flink.addons.hbase;

import org.apache.flink.addons.hbase.parser.FlatRowParser;
import org.apache.flink.addons.hbase.parser.NestedRowParser;
import org.apache.flink.addons.hbase.parser.RowParser;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.ProjectableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.Map;

import static org.apache.flink.addons.hbase.HBaseValidator.COLUMNFAMILY_QUALIFIER_DELIMITER_PATTERN;
import static org.apache.flink.addons.hbase.HBaseValidator.CONNECTOR_QUALIFIER_DELIMITER;

/**
 * Creates a TableSource to scan an HBase table.
 *
 * <p>The table name and required HBase configuration is passed during {@link HBaseTableSource} construction.
 * Use {@link #addColumn(String, String, Class)} to specify the family, qualifier, and type of columns to scan.
 *
 * <p>The TableSource returns {@link Row} with nested Rows for each column family.
 *
 * <p>The HBaseTableSource is used as shown in the example below.
 *
 * <pre>
 * {@code
 * HBaseTableSource hSrc = new HBaseTableSource(conf, "hTable");
 * hSrc.addColumn("fam1", "col1", byte[].class);
 * hSrc.addColumn("fam1", "col2", Integer.class);
 * hSrc.addColumn("fam2", "col1", String.class);
 *
 * tableEnv.registerTableSource("hTable", hSrc);
 * Table res = tableEnv.sqlQuery("SELECT t.fam2.col1, SUM(t.fam1.col2) FROM hTable AS t GROUP BY t.fam2.col1");
 * }
 * </pre>
 */
public class HBaseTableSource implements BatchTableSource<Row>, ProjectableTableSource<Row>, StreamTableSource<Row>, LookupableTableSource<Row> {

	private final Configuration conf;
	private final String tableName;
	private final HBaseTableSchema hBaseSchema;
	private TableSchema tableSchema;
	private DescriptorProperties descriptorProperties;

	/**
	 * The HBase configuration and the name of the table to read.
	 *
	 * @param conf      hbase configuration
	 * @param tableName the tableName
	 */
	public HBaseTableSource(Configuration conf, String tableName) {
		this.conf = conf;
		this.tableName = Preconditions.checkNotNull(tableName, "Table  name");
		this.hBaseSchema = new HBaseTableSchema();
	}

	private HBaseTableSource(Configuration conf, String tableName, TableSchema tableSchema) {
		this.conf = conf;
		this.tableName = Preconditions.checkNotNull(tableName, "Table  name");
		this.hBaseSchema = new HBaseTableSchema();
		this.tableSchema = tableSchema;
	}

	public HBaseTableSource(
		Configuration conf, String tableName, TableSchema tableSchema, DescriptorProperties descriptorProperties) {
		this(conf, tableName, tableSchema);
		this.descriptorProperties = descriptorProperties;
	}

	/**
	 * Adds a column defined by family, qualifier, and type to the table schema.
	 *
	 * @param family    the family name
	 * @param qualifier the qualifier name
	 * @param clazz     the data type of the qualifier
	 */
	public void addColumn(String family, String qualifier, Class<?> clazz) {
		this.hBaseSchema.addColumn(family, qualifier, clazz);
	}

	/**
	 * Specifies the charset to parse Strings to HBase byte[] keys and String values.
	 *
	 * @param charset Name of the charset to use.
	 */
	public void setCharset(String charset) {
		this.hBaseSchema.setCharset(charset);
	}

	@Override
	public TypeInformation<Row> getReturnType() {
		return new RowTypeInfo(getFieldTypes(), getFieldNames());
	}

	@Override
	public TableSchema getTableSchema() {
		if (this.tableSchema == null) {
			return new TableSchema(getFieldNames(), getFieldTypes());
		} else {
			return this.tableSchema;
		}
	}

	private String[] getFieldNames() {
		return hBaseSchema.getFamilyNames();
	}

	private TypeInformation[] getFieldTypes() {
		String[] famNames = hBaseSchema.getFamilyNames();
		TypeInformation<?>[] fieldTypes = new TypeInformation[hBaseSchema.getFamilyNames().length];
		int i = 0;
		for (String family : famNames) {
			fieldTypes[i] = new RowTypeInfo(hBaseSchema.getQualifierTypes(family), hBaseSchema.getQualifierNames(family));
			i++;
		}
		return fieldTypes;
	}

	@Override
	public DataSet<Row> getDataSet(ExecutionEnvironment execEnv) {
		return execEnv.createInput(new HBaseRowInputFormat(conf, tableName, hBaseSchema), getReturnType()).name(explainSource());
	}

	@Override
	public HBaseTableSource projectFields(int[] fields) {
		String[] famNames = hBaseSchema.getFamilyNames();
		HBaseTableSource newTableSource = new HBaseTableSource(this.conf, tableName, getTableSchema().copy());
		// Extract the family from the given fields
		for (int field : fields) {
			String family = famNames[field];
			Map<String, TypeInformation<?>> familyInfo = hBaseSchema.getFamilyInfo(family);
			for (Map.Entry<String, TypeInformation<?>> entry : familyInfo.entrySet()) {
				// create the newSchema
				String qualifier = entry.getKey();
				newTableSource.addColumn(family, qualifier, entry.getValue().getTypeClass());
			}
		}
		return newTableSource;
	}

	@Override
	public String explainSource() {
		return TableConnectorUtils.generateRuntimeName(this.getClass(), getFieldNames());
	}

	@Override
	public TableFunction<Row> getLookupFunction(String[] lookupKeys) {
		Preconditions.checkArgument(null != lookupKeys && lookupKeys.length == 1,
			"HBase table can only be retrieved by rowKey for now.");
		try {
			boolean isTableSchemaNested = checkTableSchemaNested();
			initHBaseTableSchema(isTableSchemaNested);
			return new HBaseLookupFunction(this.conf, this.tableName, createRowParser(isTableSchemaNested));
		} catch (IOException e) {
			throw new RuntimeException("encounter an IOException when initialize the HBase143LookupFunction.", e);
		}
	}

	@Override
	public AsyncTableFunction<Row> getAsyncLookupFunction(String[] lookupKeys) {
		throw new UnsupportedOperationException("HBase table can not convert to DataStream currently.");
	}

	@Override
	public boolean isAsyncEnabled() {
		return false;
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
		throw new UnsupportedOperationException("HBase table can not convert to DataStream currently.");
	}

	private void initHBaseTableSchema(boolean isTableSchemaNested) {

		String[] columnNames = this.tableSchema.getFieldNames();
		TypeInformation[] columnTypes = this.tableSchema.getFieldTypes();
		if (isTableSchemaNested) {
			initNestedHBaseTableSchema(columnNames, columnTypes);
		} else {
			initFlatHBaseTableSchema(columnNames, columnTypes);
		}
	}

	private void initNestedHBaseTableSchema(String[] columnNames, TypeInformation[] columnTypes) {
		for (int idx = 0; idx < columnNames.length; idx++) {
			if (columnTypes[idx] instanceof RowTypeInfo) {
				RowTypeInfo rowTypeInfo = (RowTypeInfo) columnTypes[idx];
				TypeInformation<?>[] qualifierTypes = rowTypeInfo.getFieldTypes();
				String[] qualifierNames = rowTypeInfo.getFieldNames();
				for (int qix = 0; qix < qualifierNames.length; qix++) {
					this.hBaseSchema.addColumn(columnNames[idx], qualifierNames[qix], qualifierTypes[qix]);
				}
			}
		}
	}

	private void initFlatHBaseTableSchema(String[] columnNames, TypeInformation[] columnTypes) {
		String delimiter = descriptorProperties.getOptionalString(CONNECTOR_QUALIFIER_DELIMITER).orElse(
			COLUMNFAMILY_QUALIFIER_DELIMITER_PATTERN);
		for (int idx = 0; idx < columnNames.length; idx++) {
			if (columnNames[idx].contains(delimiter)) {
				String[] cfQ = columnNames[idx].split(delimiter);
				Preconditions.checkArgument(2 == cfQ.length,
					"invalid column name'" + columnNames[idx] + "' for HBase qualifier name pattern: `columnFamily.qualifier`!");
				TypeInformation columnType = columnTypes[idx];
				//NOTICE: hBaseTableSchema should keep the order of columns.
				this.hBaseSchema.addColumn(cfQ[0], cfQ[1], columnType);
			}
		}
	}

	private boolean checkTableSchemaNested() {
		String[] columnNames = this.tableSchema.getFieldNames();
		DataType[] columnTypes = this.tableSchema.getFieldDataTypes();

		boolean hasRowTypeInfo = false;

		for (int idx = 0; idx < columnNames.length; idx++) {
			if (TypeConversions.fromDataTypeToLegacyInfo(columnTypes[idx]) instanceof RowTypeInfo) {
				hasRowTypeInfo = true;
				break;
			}
		}
		return hasRowTypeInfo;
	}

	private RowParser<Result> createRowParser(boolean isTableSchemaNested) {
		return isTableSchemaNested
			? new NestedRowParser(tableSchema, hBaseSchema)
			: new FlatRowParser(tableSchema, hBaseSchema, descriptorProperties);
	}

	@VisibleForTesting
	HBaseTableSchema getHBaseTableSchema() {
		return this.hBaseSchema;
	}
}
