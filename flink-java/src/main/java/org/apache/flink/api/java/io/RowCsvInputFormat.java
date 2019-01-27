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

package org.apache.flink.api.java.io;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;

/**
 * Input format that reads csv into {@link Row}.
 */
@PublicEvolving
public class RowCsvInputFormat extends AbstractRowCsvInputFormat<Row> {

	private static final long serialVersionUID = 1L;

	public RowCsvInputFormat(Path filePath, TypeInformation[] fieldTypeInfos, String lineDelimiter, String fieldDelimiter, int[] selectedFields, boolean emptyColumnAsNull, long limit) {
		super(filePath, fieldTypeInfos, lineDelimiter, fieldDelimiter, selectedFields, emptyColumnAsNull, limit);
	}

	public RowCsvInputFormat(Path filePath, TypeInformation[] fieldTypeInfos, String lineDelimiter, String fieldDelimiter, int[] selectedFields, boolean emptyColumnAsNull) {
		this(filePath, fieldTypeInfos, lineDelimiter, fieldDelimiter, selectedFields, emptyColumnAsNull, Long.MAX_VALUE);
	}

	public RowCsvInputFormat(Path filePath, TypeInformation[] fieldTypes, String lineDelimiter, String fieldDelimiter, int[] selectedFields) {
		this(filePath, fieldTypes, lineDelimiter, fieldDelimiter, selectedFields, false, Long.MAX_VALUE);
	}

	public RowCsvInputFormat(Path filePath, TypeInformation[] fieldTypes, String lineDelimiter, String fieldDelimiter) {
		this(filePath, fieldTypes, lineDelimiter, fieldDelimiter, sequentialScanOrder(fieldTypes.length));
	}

	public RowCsvInputFormat(Path filePath, TypeInformation[] fieldTypes, int[] selectedFields) {
		this(filePath, fieldTypes, DEFAULT_LINE_DELIMITER, DEFAULT_FIELD_DELIMITER, selectedFields);
	}

	public RowCsvInputFormat(Path filePath, TypeInformation[] fieldTypes, boolean emptyColumnAsNull) {
		this(filePath, fieldTypes, DEFAULT_LINE_DELIMITER, DEFAULT_FIELD_DELIMITER, sequentialScanOrder(fieldTypes
			.length), emptyColumnAsNull, Long.MAX_VALUE);
	}

	public RowCsvInputFormat(Path filePath, TypeInformation[] fieldTypes) {
		this(filePath, fieldTypes, false);
	}

	private static int[] sequentialScanOrder(int arity) {
		int[] sequentialOrder = new int[arity];
		for (int i = 0; i < arity; i++) {
			sequentialOrder[i] = i;
		}
		return sequentialOrder;
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
			reuseRow.setField(i, parsedValues[i]);
		}
		return reuseRow;
	}

	@Override
	public TypeInformation<Row> getProducedType() {
		return new RowTypeInfo(this.fieldTypeInfos);
	}
}
