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
 * Util for generating output schema.
 *
 * <p>Need following information:
 * 1) data schema
 * 2) output column names
 * 3) output column types
 * 4) reserved column names
 *
 * <p>The following roles are followed:
 * 1)If reserved columns is null, then reserve all columns from the origin input dataSet.
 * 2)If some of the reserved column names are the same as output column names, then they are
 * replaced by the output at value and type, but with the reserved column names have their order kept.
 * 3)[result columns] = ([reserved columns] subtract [output columns]) + [output columns]
 *
 */
public class OutputColsHelper implements Serializable {
	private transient String[] dataColNames;
	private transient TypeInformation[] dataColTypes;
	private transient String[] outputColNames;
	private transient TypeInformation[] outputColTypes;

	private int resultLength;
	private int[] reservedColIndices;
	private int[] reservedToResultIndices;
	private int[] outputToResultIndices;

	public OutputColsHelper(TableSchema dataSchema, String outputColName, TypeInformation outputColType) {
		this(dataSchema, outputColName, outputColType, null);
	}

	public OutputColsHelper(TableSchema dataSchema, String outputColName, TypeInformation outputColType,
							String[] reservedColNames) {
		this(dataSchema, new String[] {outputColName}, new TypeInformation[] {outputColType}, reservedColNames);
	}

	public OutputColsHelper(TableSchema dataSchema, String[] outputColNames, TypeInformation[] outputColTypes) {
		this(dataSchema, outputColNames, outputColTypes, null);
	}

	public OutputColsHelper(TableSchema dataSchema, String[] outputColNames, TypeInformation[] outputColTypes,
							String[] reservedColNames) {
		this.dataColNames = dataSchema.getFieldNames();
		this.dataColTypes = dataSchema.getFieldTypes();
		this.outputColNames = outputColNames;
		this.outputColTypes = outputColTypes;

		HashSet <String> toReservedCols = new HashSet <>(
			Arrays.asList(
				reservedColNames == null ? this.dataColNames : reservedColNames
			)
		);

		ArrayList <Integer> reservedColIndices = new ArrayList <>(toReservedCols.size());
		ArrayList <Integer> reservedColToResultIndex = new ArrayList <>(toReservedCols.size());
		outputToResultIndices = new int[outputColNames.length];
		Arrays.fill(outputToResultIndices, -1);
		int index = 0;
		for (int i = 0; i < dataColNames.length; i++) {
			int key = ArrayUtils.indexOf(outputColNames, dataColNames[i]);
			if (key >= 0) {
				outputToResultIndices[key] = index++;
				continue;
			}
			if (toReservedCols.contains(dataColNames[i])) {
				reservedColIndices.add(i);
				reservedColToResultIndex.add(index++);
			}
		}
		for (int i = 0; i < outputToResultIndices.length; i++) {
			if (outputToResultIndices[i] == -1) {
				outputToResultIndices[i] = index++;
			}
		}
		this.resultLength = index;
		this.reservedColIndices = new int[reservedColIndices.size()];
		this.reservedToResultIndices = new int[reservedColIndices.size()];
		for (int i = 0; i < this.reservedColIndices.length; i++) {
			this.reservedColIndices[i] = reservedColIndices.get(i);
			this.reservedToResultIndices[i] = reservedColToResultIndex.get(i);
		}
	}

	/**
	 * Get the reserved colNames, the result is [reserve columns] subtract [output columns].
	 *
	 * @return the reserved colNames.
	 */
	public String[] getReservedColNames() {
		String[] reservedColNames = new String[reservedColIndices.length];
		for (int i = 0; i < reservedColIndices.length; i++) {
			reservedColNames[i] = dataColNames[reservedColIndices[i]];
		}
		return reservedColNames;
	}

	/**
	 * Get the final table schema. [result columns] = ([reserve columns] subtract [output columns]) + [output columns]
	 *
	 * @return the table schema.
	 */
	public TableSchema getResultSchema() {
		String[] resultColNames = new String[resultLength];
		TypeInformation[] resultColTypes = new TypeInformation[resultLength];
		for (int i = 0; i < reservedColIndices.length; i++) {
			resultColNames[reservedToResultIndices[i]] = dataColNames[reservedColIndices[i]];
			resultColTypes[reservedToResultIndices[i]] = dataColTypes[reservedColIndices[i]];
		}
		for (int i = 0; i < outputToResultIndices.length; i++) {
			resultColNames[outputToResultIndices[i]] = outputColNames[i];
			resultColTypes[outputToResultIndices[i]] = outputColTypes[i];
		}
		return new TableSchema(resultColNames, resultColTypes);
	}

	/**
	 * Get the outputting row, which is a combination of input data and output data.
	 *
	 * @param data   input data
	 * @param output output data
	 * @return The outputting row, which is a combination of reserved input data and prediction result.
	 */
	public Row getResultRow(Row data, Row output) {
		int numOutputs = outputToResultIndices.length;
		if (output.getArity() != numOutputs) {
			throw new IllegalArgumentException("Invalid output size");
		}
		Row result = new Row(resultLength);
		for (int i = 0; i < reservedColIndices.length; i++) {
			result.setField(reservedToResultIndices[i], data.getField(reservedColIndices[i]));
		}
		for (int i = 0; i < numOutputs; i++) {
			result.setField(outputToResultIndices[i], output.getField(i));
		}
		return result;
	}

	/**
	 * Get the outputting row, which is a combination of input data and output data.
	 * The difference with getResultRow is that it's single column output.
	 *
	 * @param data   input data
	 * @param output output data
	 * @return
	 */
	public Row getResultRowSingle(Row data, Object output) {
		int numOutputs = outputToResultIndices.length;
		if (1 != numOutputs) {
			throw new IllegalArgumentException("Invalid output size");
		}
		Row result = new Row(resultLength);
		for (int i = 0; i < reservedColIndices.length; i++) {
			result.setField(reservedToResultIndices[i], data.getField(reservedColIndices[i]));
		}
		result.setField(outputToResultIndices[0], output);
		return result;
	}
}
