/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.common.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Generates output schema given:
 * 1) data schema
 * 2) output column names
 * 3) output column types
 * 4) keep column names
 * [result columns] = ([keep columns] subtract [output columns]) + [output columns]
 * If some of the output column names are the same as input column names, then they
 * together with the kept column names have their order kept.
 */
public class OutputColsHelper implements Serializable {
	private transient String[] dataColNames;
	private transient TypeInformation[] dataColTypes;
	private transient String[] outputColNames;
	private transient TypeInformation[] outputColTypes;
	private transient String[] keepColNames;
	private transient Boolean flag;

	private int resultLength;
	private int[] keepCols;
	private int[] keepColsIndexToResultIndex;
	private int[] outputIndexToResultIndex;

	public OutputColsHelper(TableSchema dataSchema, String outputColName, TypeInformation outputColType) {
		this(dataSchema, outputColName, outputColType, null);
	}

	public OutputColsHelper(TableSchema dataSchema, String outputColName, TypeInformation outputColType,
							String[] keepColNames) {
		this(dataSchema, new String[] {outputColName}, new TypeInformation[] {outputColType}, keepColNames);
	}

	public OutputColsHelper(TableSchema dataSchema, String[] outputColNames, TypeInformation[] outputColTypes) {
		this(dataSchema, outputColNames, outputColTypes, null);
	}

	public OutputColsHelper(TableSchema dataSchema, String[] outputColNames, TypeInformation[] outputColTypes,
							String[] keepColNames) {
		this.dataColNames = dataSchema.getFieldNames();
		this.dataColTypes = dataSchema.getFieldTypes();
		this.outputColNames = outputColNames;
		this.outputColTypes = outputColTypes;
		this.keepColNames = (keepColNames == null ? this.dataColNames : keepColNames);
		this.flag = true;
		initialize();
	}

	public String[] getKeepColNames() {
		checkEnv();
		String[] keepColNames = new String[keepCols.length];
		for (int i = 0; i < keepCols.length; i++) {
			keepColNames[i] = dataColNames[keepCols[i]];
		}
		return keepColNames;
	}

	public TypeInformation[] getKeepColTypes() {
		checkEnv();
		TypeInformation[] keepColTypes = new TypeInformation[keepCols.length];
		for (int i = 0; i < keepCols.length; i++) {
			keepColTypes[i] = dataColTypes[keepCols[i]];
		}
		return keepColTypes;
	}

	public int[] getKeepColIndices() {
		return keepCols;
	}

	public String[] getResultColNames() {
		checkEnv();
		String[] resultColNames = new String[resultLength];
		for (int i = 0; i < keepCols.length; i++) {
			resultColNames[keepColsIndexToResultIndex[i]] = dataColNames[keepCols[i]];
		}
		for (int i = 0; i < outputIndexToResultIndex.length; i++) {
			resultColNames[outputIndexToResultIndex[i]] = outputColNames[i];
		}
		return resultColNames;
	}

	public TypeInformation[] getResultColTypes() {
		checkEnv();
		TypeInformation[] resultColTypes = new TypeInformation[resultLength];
		for (int i = 0; i < keepCols.length; i++) {
			resultColTypes[keepColsIndexToResultIndex[i]] = dataColTypes[keepCols[i]];
		}
		for (int i = 0; i < outputIndexToResultIndex.length; i++) {
			resultColTypes[outputIndexToResultIndex[i]] = outputColTypes[i];
		}
		return resultColTypes;
	}

	public TableSchema getResultSchema() {
		checkEnv();
		return new TableSchema(getResultColNames(), getResultColTypes());
	}

	/**
	 * Get the outputting row, which is a combination of kept input data and prediction result.
	 *
	 * @param data   prediction data
	 * @param output prediction output
	 * @return The outputting row, which is a combination of kept input data and prediction result.
	 */
	public Row getResultRow(Row data, Row output) {
		int numOutputs = outputIndexToResultIndex.length;
		if (output.getArity() != numOutputs) {
			throw new RuntimeException("Invalid output size");
		}
		Row result = new Row(resultLength);
		for (int i = 0; i < keepCols.length; i++) {
			result.setField(keepColsIndexToResultIndex[i], data.getField(keepCols[i]));
		}
		for (int i = 0; i < numOutputs; i++) {
			result.setField(outputIndexToResultIndex[i], output.getField(i));
		}
		return result;
	}

	public Row getResultRowSingle(Row data, Object output) {
		int numOutputs = outputIndexToResultIndex.length;
		if (1 != numOutputs) {
			throw new RuntimeException("Invalid output size");
		}
		Row result = new Row(resultLength);
		for (int i = 0; i < keepCols.length; i++) {
			result.setField(keepColsIndexToResultIndex[i], data.getField(keepCols[i]));
		}
		result.setField(outputIndexToResultIndex[0], output);
		return result;
	}

	private void initialize() {
		Set <String> toKeepCols = new HashSet <>(Arrays.asList(keepColNames));
		Set <String> outputCols = new HashSet <>(Arrays.asList(outputColNames));
		Set <String> inputCols = new HashSet <>(Arrays.asList(dataColNames));
		Set <String> inOutCols = intersect(inputCols, outputCols);
		if (inOutCols.size() == 0) {
			List <Integer> keepColIndices = new ArrayList <>(toKeepCols.size());
			for (int i = 0; i < dataColNames.length; i++) {
				if (toKeepCols.contains(dataColNames[i])) {
					keepColIndices.add(i);
				}
			}
			this.resultLength = keepColIndices.size() + outputColNames.length;
			this.keepCols = new int[keepColIndices.size()];
			this.keepColsIndexToResultIndex = new int[keepColIndices.size()];
			for (int i = 0; i < keepColIndices.size(); i++) {
				this.keepCols[i] = keepColIndices.get(i);
				this.keepColsIndexToResultIndex[i] = i;
			}
			this.outputIndexToResultIndex = new int[outputColNames.length];
			for (int i = 0; i < outputColNames.length; i++) {
				this.outputIndexToResultIndex[i] = this.keepCols.length + i;
			}
		} else {
			subtract(toKeepCols, outputCols);
			List <Integer> keepColIndices = new ArrayList <>(toKeepCols.size());
			for (int i = 0; i < dataColNames.length; i++) {
				if (toKeepCols.contains(dataColNames[i])) {
					keepColIndices.add(i);
				}
			}
			this.keepCols = new int[keepColIndices.size()];
			for (int i = 0; i < keepColIndices.size(); i++) {
				this.keepCols[i] = keepColIndices.get(i);
			}

			this.resultLength = this.keepCols.length + outputColNames.length;
			this.outputIndexToResultIndex = new int[outputColNames.length];
			this.keepColsIndexToResultIndex = new int[this.keepCols.length];

			int idx = 0;
			Map <String, Integer> colNameToColIdx = new HashMap <>();
			for (int i = 0; i < this.dataColNames.length; i++) {
				if (toKeepCols.contains(dataColNames[i]) || outputCols.contains(dataColNames[i])) {
					colNameToColIdx.put(dataColNames[i], idx++);
				}
			}

			for (int i = 0; i < keepCols.length; i++) {
				this.keepColsIndexToResultIndex[i] = colNameToColIdx.get(dataColNames[keepCols[i]]);
			}
			for (int i = 0; i < outputColNames.length; i++) {
				if (colNameToColIdx.containsKey(outputColNames[i])) {
					this.outputIndexToResultIndex[i] = colNameToColIdx.get(outputColNames[i]);
				} else {
					this.outputIndexToResultIndex[i] = idx++;
				}
			}
			if (idx != resultLength) {
				throw new RuntimeException("impl err.");
			}
		}
	}

	private static void subtract(Set <String> a, Set <String> b) {
		b.forEach(a::remove);
	}

	private static Set <String> intersect(Set <String> a, Set <String> b) {
		Set <String> ret = new HashSet <>();
		a.forEach(s -> {
			if (b.contains(s)) {
				ret.add(s);
			}
		});
		return ret;
	}

	private void checkEnv() {
		if (this.flag == null) {
			throw new RuntimeException("Method intended to be called in client program " +
				"is called in the operator code.");
		}
	}
}
