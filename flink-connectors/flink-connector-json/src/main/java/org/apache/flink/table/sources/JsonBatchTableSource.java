/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.sources;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

/**
 * A [[BatchTableSource]] for simple Json files.
 */
public class JsonBatchTableSource implements BatchTableSource<Row>, ProjectableTableSource<Row>{

	// The path to the Json file.
	private final String path;
	// The names of the table fields.
	private final String[] fieldNames;
	// The types of the table fields.
	private final TypeInformation[] fieldTypes;
	// The fields which will be read and returned by the table source.
	private final int[] selectedFields;

	/**
	 * A [[BatchTableSource]] for simple JSON files with a
	 * (logically) unlimited number of fields.
	 *
	 * @param path The path to the json file.
	 * @param fieldNames The names of the table fields.
	 * @param fieldTypes The types of the table fields.
	 */
	public JsonBatchTableSource(String path, String[] fieldNames, TypeInformation[] fieldTypes) {
		this(path, fieldNames, fieldTypes, null);
	}

	public JsonBatchTableSource(String path, String[] fieldNames, TypeInformation[] fieldTypes, int[] selectedFields) {
		Preconditions.checkNotNull(path, "Path must not be null.");
		this.path = path;
		this.fieldNames = fieldNames;
		this.fieldTypes = fieldTypes;
		if (null == selectedFields) {
			selectedFields = new int[fieldNames.length];
			for (int i = 0; i < fieldNames.length; i++) {
				selectedFields[i] = i;
			}
		}
		this.selectedFields = selectedFields;
	}

	@Override
	public DataSet<Row> getDataSet(ExecutionEnvironment execEnv) {
		JsonRowInputFormat inputFormat = new JsonRowInputFormat(path, fieldNames, fieldTypes);
		if (selectedFields != null) {
			inputFormat.selectFields(selectedFields);
		}
		return execEnv.createInput(inputFormat).name(explainSource());
	}

	@Override
	public TableSource<Row> projectFields(int[] fields) {
		// create a copy of the JsonFileBatchTableSource with new selected fields.
		// If None, all fields are returned.
		if (null == fields) {
			fields = this.selectedFields;
		}
		return new JsonBatchTableSource(path, fieldNames, fieldTypes, fields);
	}

	@Override
	public TypeInformation<Row> getReturnType() {
		String[] selectedFieldNames = new String[this.selectedFields.length];
		TypeInformation[] selectedFieldTypes = new TypeInformation[this.selectedFields.length];
		for (int i = 0; i < this.selectedFields.length; i++) {
			selectedFieldNames[i] = fieldNames[selectedFields[i]];
			selectedFieldTypes[i] = fieldTypes[selectedFields[i]];
		}
		return new RowTypeInfo(selectedFieldTypes , selectedFieldNames);
	}

	@Override
	public TableSchema getTableSchema() {
		return new TableSchema(fieldNames, fieldTypes);
	}

	@Override
	public String explainSource() {
		return String.format("JsonFileBatchTableSource( read fields: %s)", fieldNames.toString());
	}
}
