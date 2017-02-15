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
 * Creates a table source that helps to scan data from an hbase table
 * The table name is passed during {@link HBaseTableSource} construction along with the required hbase configurations.
 * Use {@link #addColumn(String, String, Class)} to specify the family name, qualifier and the type of the qualifier that needs to be scanned
 * This supports nested schema, for eg: if we have a column family 'Person' and two qualifiers 'Name' (type String) and 'Unique_Id' (Type Integer),
 * then we represent this schema as Row<Person : Row<Name: String, Unique_Id : Integer>>
 */
public class HBaseTableSource implements BatchTableSource<Row>, ProjectableTableSource<Row> {

	private Configuration conf;
	private String tableName;
	private HBaseTableSchema schema;

	/**
	 * The hbase configuration and the table name that acts as the hbase table source
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
	 * Allows specifying the family and qualifier name along with the data type of the qualifier for an HBase table
	 *
	 * @param family    the family name
	 * @param qualifier the qualifier name
	 * @param clazz     the data type of the qualifier
	 */
	public void addColumn(String family, String qualifier, Class<?> clazz) {
		this.schema.addColumn(family, qualifier, clazz);
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
		RowTypeInfo rowInfo = new RowTypeInfo(typeInfos, famNames);
		return rowInfo;
	}

	@Override
	public DataSet<Row> getDataSet(ExecutionEnvironment execEnv) {
		return execEnv.createInput(new HBaseTableSourceInputFormat(conf, tableName, schema), getReturnType());
	}

	@Override
	public ProjectableTableSource<Row> projectFields(int[] fields) {
		String[] famNames = schema.getFamilyNames();
		HBaseTableSource newTableSource = new HBaseTableSource(this.conf, tableName);
		// Extract the family from the given fields
		for(int field : fields) {
			String family = famNames[field];
			Map<String, TypeInformation<?>> familyInfo = schema.getFamilyInfo(family);
			for(String qualifier : familyInfo.keySet()) {
				// create the newSchema
				newTableSource.addColumn(family, qualifier, familyInfo.get(qualifier).getTypeClass());
			}
		}
		return newTableSource;
	}
}
