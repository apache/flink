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

package org.apache.flink.table.runtime.batch.io;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;

import java.util.Arrays;

/**
 * InputFormat to read csv files into {@link Row}.
 */
public class RFCRowCsvInputFormat extends RFCCsvInputFormat<Row> implements ResultTypeQueryable<Row> {

	private static final long serialVersionUID = 1L;

	private int arity;
	private TypeInformation[] fieldTypeInfos;
	private int[] fieldPosMap;

	public RFCRowCsvInputFormat(Path filePath, TypeInformation[] fieldTypeInfos, String lineDelimiter, char fieldDelimiter, int[] selectedFields) {

		super(filePath);
		this.arity = fieldTypeInfos.length;
		if (arity == 0) {
			throw new IllegalArgumentException("At least one field must be specified");
		}
		if (arity != selectedFields.length) {
			throw new IllegalArgumentException("Number of field types and selected fields must be the same");
		}

		this.fieldTypeInfos = fieldTypeInfos;
		this.fieldPosMap = toFieldPosMap(selectedFields);

		boolean[] fieldsMask = toFieldMask(selectedFields);

		setRecordDelimiter(lineDelimiter);
		setFieldDelimiter(fieldDelimiter);
		setOnlySupportedFieldsTypes(fieldsMask, fieldTypeInfos);
	}

	public RFCRowCsvInputFormat(Path filePath, TypeInformation[] fieldTypes, String lineDelimiter, char fieldDelimiter) {
		this(filePath, fieldTypes, lineDelimiter, fieldDelimiter, sequentialScanOrder(fieldTypes.length));
	}

	public RFCRowCsvInputFormat(Path filePath, TypeInformation[] fieldTypes, int[] selectedFields) {
		this(filePath, fieldTypes, DEFAULT_RECORD_DELIMITER, DEFAULT_FIELD_DELIMITER, selectedFields);
	}

	public RFCRowCsvInputFormat(Path filePath, TypeInformation[] fieldTypes) {
		this(filePath, fieldTypes, DEFAULT_RECORD_DELIMITER, DEFAULT_FIELD_DELIMITER, sequentialScanOrder(fieldTypes.length));
	}


	/**
	 * Select all field indices in sequential order from a row/record to support full projection.
	 * @param arity total number of fields in a row/record
	 * @return projected field indices in sequential order
	 */
	private static int[] sequentialScanOrder(int arity) {
		int[] sequentialOrder = new int[arity];
		for (int i = 0; i < arity; i++) {
			sequentialOrder[i] = i;
		}
		return sequentialOrder;
	}

	/**
	 * Create a boolean mask from field indices selected for projection.
	 * the size of boolean mask depends upon the max index value of selected fields
	 * @param selectedFields selected fields indices for projection
	 * @return projection mask
	 */
	private static boolean[] toFieldMask(int[] selectedFields) {
		int maxField = 0;
		for (int selectedField : selectedFields) {
			maxField = Math.max(maxField, selectedField);
		}
		boolean[] mask = new boolean[maxField + 1];
		Arrays.fill(mask, false);

		for (int selectedField : selectedFields) {
			mask[selectedField] = true;
		}
		return mask;
	}

	/**
	 * Create a mapping for projection when selected field indices are not in sequential order.
	 * @param selectedFields selected fields indices for projection
	 * @return mapping for selected fields
	 */
	private static int[] toFieldPosMap(int[] selectedFields) {
		int[] fieldIdxs = Arrays.copyOf(selectedFields, selectedFields.length);
		Arrays.sort(fieldIdxs);

		int[] fieldPosMap = new int[selectedFields.length];
		for (int i = 0; i < selectedFields.length; i++) {
			int pos = Arrays.binarySearch(fieldIdxs, selectedFields[i]);
			fieldPosMap[pos] = i;
		}

		return fieldPosMap;
	}

	@Override
	protected Row fillRecord(Row reuse, Object[] parsedValues) {
		Row reuseRow;
		if (reuse == null) {
			reuseRow = new Row(arity);
		} else {
			reuseRow = reuse;
		}
		for (int i = 0; i < parsedValues.length; i++) {
			reuseRow.setField(i, parsedValues[fieldPosMap[i]]);
		}
		return reuseRow;
	}

	@Override
	public TypeInformation<Row> getProducedType() {
		return new RowTypeInfo(this.fieldTypeInfos);
	}
}
