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
import org.apache.flink.api.common.io.ParseException;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;
import org.apache.flink.types.parser.FieldParser;

@PublicEvolving
public class RowCsvInputFormat extends CsvInputFormat<Row> {

	private static final long serialVersionUID = 1L;

	private int arity;
	private boolean emptyColumnAsNull;

	public RowCsvInputFormat(Path filePath, RowTypeInfo rowTypeInfo, String lineDelimiter, String fieldDelimiter, boolean[] includedFieldsMask, boolean emptyColumnAsNull) {
		super(filePath);
		if (rowTypeInfo.getArity() == 0) {
			throw new IllegalArgumentException("Row arity must be greater than 0.");
		}
		this.arity = rowTypeInfo.getArity();

		boolean[] fieldsMask;
		if (includedFieldsMask != null) {
			fieldsMask = includedFieldsMask;
		} else {
			fieldsMask = createDefaultMask(arity);
		}
		this.emptyColumnAsNull = emptyColumnAsNull;
		setDelimiter(lineDelimiter);
		setFieldDelimiter(fieldDelimiter);
		setFieldsGeneric(fieldsMask, extractTypeClasses(rowTypeInfo));
	}

	public RowCsvInputFormat(Path filePath, RowTypeInfo rowTypeInfo, String lineDelimiter, String fieldDelimiter, int[] includedFieldsMask) {
		this(filePath, rowTypeInfo, lineDelimiter, fieldDelimiter, toBoolMask(includedFieldsMask), false);
	}

	public RowCsvInputFormat(Path filePath, RowTypeInfo rowTypeInfo, String lineDelimiter, String fieldDelimiter) {
		this(filePath, rowTypeInfo, lineDelimiter, fieldDelimiter, null, false);
	}

	public RowCsvInputFormat(Path filePath, RowTypeInfo rowTypeInfo, boolean[] includedFieldsMask) {
		this(filePath, rowTypeInfo, DEFAULT_LINE_DELIMITER, DEFAULT_FIELD_DELIMITER, includedFieldsMask, false);
	}

	public RowCsvInputFormat(Path filePath, RowTypeInfo rowTypeInfo, int[] includedFieldsMask) {
		this(filePath, rowTypeInfo, DEFAULT_LINE_DELIMITER, DEFAULT_FIELD_DELIMITER, includedFieldsMask);
	}

	public RowCsvInputFormat(Path filePath, RowTypeInfo rowTypeInfo, boolean emptyColumnAsNull) {
		this(filePath, rowTypeInfo, DEFAULT_LINE_DELIMITER, DEFAULT_FIELD_DELIMITER, null, emptyColumnAsNull);
	}

	public RowCsvInputFormat(Path filePath, RowTypeInfo rowTypeInfo) {
		this(filePath, rowTypeInfo, DEFAULT_LINE_DELIMITER, DEFAULT_FIELD_DELIMITER, null);
	}

	private static Class<?>[] extractTypeClasses(RowTypeInfo rowTypeInfo) {
		Class<?>[] classes = new Class<?>[rowTypeInfo.getArity()];
		for (int i = 0; i < rowTypeInfo.getArity(); i++) {
			classes[i] = rowTypeInfo.getTypeAt(i).getTypeClass();
		}
		return classes;
	}

	private static boolean[] toBoolMask(int[] includedFieldsMask) {
		if (includedFieldsMask == null) {
			return null;
		} else {
			return toBooleanMask(includedFieldsMask);
		}
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
	protected boolean parseRecord(Object[] holders, byte[] bytes, int offset, int numBytes) throws ParseException {
		byte[] fieldDelimiter = this.getFieldDelimiter();
		boolean[] fieldIncluded = this.fieldIncluded;

		int startPos = offset;
		int limit = offset + numBytes;

		int field = 0;
		int output = 0;
		while (field < fieldIncluded.length) {

			// check valid start position
			if (startPos >= limit) {
				if (isLenient()) {
					return false;
				} else {
					throw new ParseException("Row too short: " + new String(bytes, offset, numBytes));
				}
			}

			if (fieldIncluded[field]) {
				// parse field
				FieldParser<Object> parser = (FieldParser<Object>) this.getFieldParsers()[output];
				int latestValidPos = startPos;
				startPos = parser.resetErrorStateAndParse(
					bytes,
					startPos,
					limit,
					fieldDelimiter,
					holders[output]);

				if (!isLenient() && (parser.getErrorState() != FieldParser.ParseErrorState.NONE)) {
					// the error state EMPTY_COLUMN is ignored
					if (parser.getErrorState() != FieldParser.ParseErrorState.EMPTY_COLUMN) {
						throw new ParseException(String.format("Parsing error for column %1$s of row '%2$s' originated by %3$s: %4$s.",
							field, new String(bytes, offset, numBytes), parser.getClass().getSimpleName(), parser.getErrorState()));
					}
				}
				holders[output] = parser.getLastResult();

				// check parse result:
				// the result is null if it is invalid
				// or empty with emptyColumnAsNull enabled
				if (startPos < 0 ||
					(emptyColumnAsNull && (parser.getErrorState().equals(FieldParser.ParseErrorState.EMPTY_COLUMN)))) {
					holders[output] = null;
					startPos = skipFields(bytes, latestValidPos, limit, fieldDelimiter);
				}
				output++;
			} else {
				// skip field
				startPos = skipFields(bytes, startPos, limit, fieldDelimiter);
			}

			// check if something went wrong
			if (startPos < 0) {
				throw new ParseException(String.format("Unexpected parser position for column %1$s of row '%2$s'",
					field, new String(bytes, offset, numBytes)));
			}

			field++;
		}
		return true;
	}
}
