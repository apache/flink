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

import org.apache.commons.lang3.ArrayUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

/**
 * Utils for merging input data with output data.
 *
 * <p>Input:
 * 1) Schema of input data being predicted or transformed.
 * 2) Output column names of the prediction/transformation operator.
 * 3) Output column types of the prediction/transformation operator.
 * 4) Reserved column names, which is a subset of input data's column names that we want to preserve.
 *
 * <p>Output:
 * 1）The result data schema. The result data is a combination of the preserved columns and the operator's
 * output columns.
 *
 * <p>Several rules are followed:
 * <ul>
 * <li>If reserved columns are not given, then all columns of input data is reserved.
 * <li>The reserved columns are arranged ahead of the operator's output columns in the final output.
 * <li>If some of the reserved column names overlap with those of operator's output columns, then the operator's
 * output columns override the conflicting reserved columns.
 * <li>The reserved columns in the result table preserve their orders as in the input table.
 * </ul>
 *
 * <p>For example, if we have input data schema of ["id":INT, "f1":FLOAT, "f2":DOUBLE], and the operator outputs
 * a column "label" with type STRING, and we want to preserve the column "id", then we get the result
 * schema of ["id":INT, "label":STRING].
 *
 * <p>end user should not directly interact with this helper class. instead it will be indirectly used via concrete algorithms.
 */
public class OutputColsHelper implements Serializable {
	private String[] inputColNames;
	private TypeInformation<?>[] inputColTypes;
	private String[] outputColNames;
	private TypeInformation<?>[] outputColTypes;

	/**
	 * Column indices in the input data that would be forward to the result.
	 */
	private int[] reservedCols;

	/**
	 * The positions of reserved columns in the result.
	 */
	private int[] reservedColsPosInResult;

	/**
	 * The positions of output columns in the result.
	 */
	private int[] outputColsPosInResult;

	public OutputColsHelper(TableSchema inputSchema, String outputColName, TypeInformation<?> outputColType) {
		this(inputSchema, outputColName, outputColType, inputSchema.getFieldNames());
	}

	public OutputColsHelper(TableSchema inputSchema, String outputColName, TypeInformation<?> outputColType,
							String[] reservedColNames) {
		this(inputSchema, new String[]{outputColName}, new TypeInformation<?>[]{outputColType}, reservedColNames);
	}

	public OutputColsHelper(TableSchema inputSchema, String[] outputColNames, TypeInformation<?>[] outputColTypes) {
		this(inputSchema, outputColNames, outputColTypes, inputSchema.getFieldNames());
	}

	/**
	 * The constructor.
	 *
	 * @param inputSchema      Schema of input data being predicted or transformed.
	 * @param outputColNames   Output column names of the prediction/transformation operator.
	 * @param outputColTypes   Output column types of the prediction/transformation operator.
	 * @param reservedColNames Reserved column names, which is a subset of input data's column names that we want to preserve.
	 */
	public OutputColsHelper(TableSchema inputSchema, String[] outputColNames, TypeInformation<?>[] outputColTypes,
							String[] reservedColNames) {
		this.inputColNames = inputSchema.getFieldNames();
		this.inputColTypes = inputSchema.getFieldTypes();
		this.outputColNames = outputColNames;
		this.outputColTypes = outputColTypes;

		HashSet<String> toReservedCols = new HashSet<>(
			Arrays.asList(reservedColNames == null ? this.inputColNames : reservedColNames)
		);
		//the indices of the columns which need to be reserved.
		ArrayList<Integer> reservedColIndices = new ArrayList<>(toReservedCols.size());
		ArrayList<Integer> reservedColToResultIndex = new ArrayList<>(toReservedCols.size());
		outputColsPosInResult = new int[outputColNames.length];
		Arrays.fill(outputColsPosInResult, -1);
		int index = 0;
		for (int i = 0; i < inputColNames.length; i++) {
			int key = ArrayUtils.indexOf(outputColNames, inputColNames[i]);
			if (key >= 0) {
				outputColsPosInResult[key] = index++;
				continue;
			}
			//add these interested column.
			if (toReservedCols.contains(inputColNames[i])) {
				reservedColIndices.add(i);
				reservedColToResultIndex.add(index++);
			}
		}
		for (int i = 0; i < outputColsPosInResult.length; i++) {
			if (outputColsPosInResult[i] == -1) {
				outputColsPosInResult[i] = index++;
			}
		}
		//write reversed column information in array.
		this.reservedCols = new int[reservedColIndices.size()];
		this.reservedColsPosInResult = new int[reservedColIndices.size()];
		for (int i = 0; i < this.reservedCols.length; i++) {
			this.reservedCols[i] = reservedColIndices.get(i);
			this.reservedColsPosInResult[i] = reservedColToResultIndex.get(i);
		}
	}

	/**
	 * Get the reserved columns' names.
	 *
	 * @return the reserved colNames.
	 */
	public String[] getReservedColumns() {
		String[] passThroughColNames = new String[reservedCols.length];
		for (int i = 0; i < reservedCols.length; i++) {
			passThroughColNames[i] = inputColNames[reservedCols[i]];
		}
		return passThroughColNames;
	}

	/**
	 * Get the result table schema. The result data is a combination of the preserved columns and the operator's
	 * output columns.
	 *
	 * @return The result table schema.
	 */
	public TableSchema getResultSchema() {
		int resultLength = reservedCols.length + outputColNames.length;
		String[] resultColNames = new String[resultLength];
		TypeInformation<?>[] resultColTypes = new TypeInformation[resultLength];
		for (int i = 0; i < reservedCols.length; i++) {
			resultColNames[reservedColsPosInResult[i]] = inputColNames[reservedCols[i]];
			resultColTypes[reservedColsPosInResult[i]] = inputColTypes[reservedCols[i]];
		}
		for (int i = 0; i < outputColsPosInResult.length; i++) {
			resultColNames[outputColsPosInResult[i]] = outputColNames[i];
			resultColTypes[outputColsPosInResult[i]] = outputColTypes[i];
		}
		return new TableSchema(resultColNames, resultColTypes);
	}

	/**
	 * Merge the input row and the output row.
	 *
	 * @param input  The input row being predicted or transformed.
	 * @param output The output row of the prediction/transformation operator.
	 * @return The result row, which is a combination of preserved columns and the operator's
	 * output columns.
	 */
	public Row getResultRow(Row input, Row output) {
		int numOutputs = outputColsPosInResult.length;
		if (output.getArity() != numOutputs) {
			throw new IllegalArgumentException("Invalid output size");
		}
		int resultLength = reservedCols.length + outputColNames.length;
		Row result = new Row(resultLength);
		for (int i = 0; i < reservedCols.length; i++) {
			result.setField(reservedColsPosInResult[i], input.getField(reservedCols[i]));
		}
		for (int i = 0; i < numOutputs; i++) {
			result.setField(outputColsPosInResult[i], output.getField(i));
		}
		return result;
	}
}
