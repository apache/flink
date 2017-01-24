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
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Creates a table source that helps to scan data from an hbase table
 *
 * Note : the colNames are specified along with a familyName and they are seperated by a ':'
 * For eg, cf1:q1 - where cf1 is the familyName and q1 is the qualifier name
 */
// TODO : Implement ProjectableTableSource?
public class HBaseTableSource implements BatchTableSource<Row> {

	private Configuration conf;
	private String tableName;
	private HBaseTableSchema schema;
	private String[] famNames;

	public HBaseTableSource(Configuration conf, String tableName, HBaseTableSchema schema) {
		this.conf = conf;
		this.tableName = Preconditions.checkNotNull(tableName, "Table  name");
		this.schema = Preconditions.checkNotNull(schema, "Schema");
		Map<String, List<Pair>> familyMap = schema.getFamilyMap();
		famNames = familyMap.keySet().toArray(new String[familyMap.size()]);
	}

	@Override
	public TypeInformation<Row> getReturnType() {
		// split the fieldNames
		Map<String, List<Pair>> famMap = schema.getFamilyMap();

		List<String> qualNames = new ArrayList<String>();
		List<TypeInformation<?>> colTypeInfo = new ArrayList<TypeInformation<?>>();
		TypeInformation<?>[] typeInfos = new TypeInformation[famMap.size()];
		int i = 0;
		for (String family : famMap.keySet()) {
			List<Pair> colDetails = famMap.get(family);
			for (Pair<String, TypeInformation<?>> pair : colDetails) {
				qualNames.add(pair.getFirst());
				colTypeInfo.add(pair.getSecond());
			}
			typeInfos[i] = new RowTypeInfo(colTypeInfo.toArray(new TypeInformation[colTypeInfo.size()]), qualNames.toArray(new String[qualNames.size()]));
			i++;
			colTypeInfo.clear();
			qualNames.clear();
		}
		RowTypeInfo rowInfo = new RowTypeInfo(typeInfos, famNames);
		return rowInfo;
	}

	@Override
	public DataSet<Row> getDataSet(ExecutionEnvironment execEnv) {
		return execEnv.createInput(new HBaseTableSourceInputFormat(conf, tableName, schema), getReturnType());
	}
}
