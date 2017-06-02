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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.ProjectableTableSource;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.conf.Configuration;

import java.util.Map;

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
 * Table res = tableEnv.sql("SELECT t.fam2.col1, SUM(t.fam1.col2) FROM hTable AS t GROUP BY t.fam2.col1");
 * }
 * </pre>
 *
 */
public class HBaseTableSource implements BatchTableSource<Row>, ProjectableTableSource<Row> {

	private Configuration conf;
	private String tableName;
	private HBaseTableSchema schema;

	/**
	 * The HBase configuration and the name of the table to read.
	 *
	 * @param conf      hbase configuration
	 * @param tableName the tableName
	 */
	public HBaseTableSource(Configuration conf, String tableName) {
		this.conf = conf;
		this.tableName = Preconditions.checkNotNull(tableName, "Table  name");
		this.schema = new HBaseTableSchema();
	}

	/**
	 * Adds a column defined by family, qualifier, and type to the table schema.
	 *
	 * @param family    the family name
	 * @param qualifier the qualifier name
	 * @param clazz     the data type of the qualifier
	 */
	public void addColumn(String family, String qualifier, Class<?> clazz) {
		this.schema.addColumn(family, qualifier, clazz);
	}

	/**
	 * Specifies the charset to parse Strings to HBase byte[] keys and String values.
	 *
	 * @param charset Name of the charset to use.
	 */
	public void setCharset(String charset) {
		this.schema.setCharset(charset);
	}

	@Override
	public TypeInformation<Row> getReturnType() {
		String[] famNames = schema.getFamilyNames();
		TypeInformation<?>[] typeInfos = new TypeInformation[famNames.length];
		int i = 0;
		for (String family : famNames) {
			typeInfos[i] = new RowTypeInfo(schema.getQualifierTypes(family), schema.getQualifierNames(family));
			i++;
		}
		return new RowTypeInfo(typeInfos, famNames);
	}

	@Override
	public DataSet<Row> getDataSet(ExecutionEnvironment execEnv) {
		return execEnv.createInput(new HBaseRowInputFormat(conf, tableName, schema), getReturnType());
	}

	@Override
	public HBaseTableSource projectFields(int[] fields) {
		String[] famNames = schema.getFamilyNames();
		HBaseTableSource newTableSource = new HBaseTableSource(this.conf, tableName);
		// Extract the family from the given fields
		for (int field : fields) {
			String family = famNames[field];
			Map<String, TypeInformation<?>> familyInfo = schema.getFamilyInfo(family);
			for (String qualifier : familyInfo.keySet()) {
				// create the newSchema
				newTableSource.addColumn(family, qualifier, familyInfo.get(qualifier).getTypeClass());
			}
		}
		return newTableSource;
	}

	@Override
	public String explainSource() {
		return "";
	}
}
