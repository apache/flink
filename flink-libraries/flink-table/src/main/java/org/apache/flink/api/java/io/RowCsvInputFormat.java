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
 * WITHRow WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.api.java.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.ParseException;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.table.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.parser.FieldParser;
import org.apache.flink.types.parser.FieldParser.ParseErrorState;

@Internal
public class RowCsvInputFormat extends CsvInputFormat<Row> {

	private static final long serialVersionUID = 1L;

	private int arity;

	public RowCsvInputFormat(Path filePath, RowTypeInfo rowTypeInfo) {
		this(filePath, DEFAULT_LINE_DELIMITER, DEFAULT_FIELD_DELIMITER, rowTypeInfo);
	}

	public RowCsvInputFormat(Path filePath, String lineDelimiter, String fieldDelimiter, RowTypeInfo rowTypeInfo) {
		this(filePath, lineDelimiter, fieldDelimiter, rowTypeInfo, createDefaultMask(rowTypeInfo.getArity()));
	}

	public RowCsvInputFormat(Path filePath, RowTypeInfo rowTypeInfo, int[] includedFieldsMask) {
		this(filePath, DEFAULT_LINE_DELIMITER, DEFAULT_FIELD_DELIMITER, rowTypeInfo, includedFieldsMask);
	}

	public RowCsvInputFormat(Path filePath, String lineDelimiter, String fieldDelimiter, RowTypeInfo rowTypeInfo,
			int[] includedFieldsMask) {
		this(filePath, lineDelimiter, fieldDelimiter, rowTypeInfo, (includedFieldsMask == null) ? createDefaultMask(rowTypeInfo.getArity())
				: toBooleanMask(includedFieldsMask));
	}

	public RowCsvInputFormat(Path filePath, RowTypeInfo rowTypeInfo, boolean[] includedFieldsMask) {
		this(filePath, DEFAULT_LINE_DELIMITER, DEFAULT_FIELD_DELIMITER, rowTypeInfo, includedFieldsMask);
	}

	public RowCsvInputFormat(Path filePath, String lineDelimiter, String fieldDelimiter, RowTypeInfo rowTypeInfo,
			boolean[] includedFieldsMask) {
		super(filePath);
		if (rowTypeInfo.getArity() == 0) {
			throw new IllegalArgumentException("Row arity must be greater than 0.");
		}

		if (includedFieldsMask == null) {
			includedFieldsMask = createDefaultMask(rowTypeInfo.getArity());
		}

		this.arity = rowTypeInfo.getArity();

		setDelimiter(lineDelimiter);
		setFieldDelimiter(fieldDelimiter);

		Class<?>[] classes = new Class<?>[rowTypeInfo.getArity()];

		for (int i = 0; i < rowTypeInfo.getArity(); i++) {
			classes[i] = rowTypeInfo.getTypeAt(i).getTypeClass();
		}

		setFieldsGeneric(includedFieldsMask, classes);
	}


	@Override
	public Row fillRecord(Row reuse, Object[] parsedValues) {
		if (reuse == null) {
			reuse = new Row(arity);
		}
		for (int i = 0; i < parsedValues.length; i++) {
			reuse.setField(i, parsedValues[i]);
		}
		return reuse;
	}

	@Override
	protected boolean parseRecord(Object[] holders, byte[] bytes, int offset, int numBytes) throws ParseException {
		boolean[] fieldIncluded = this.fieldIncluded;

		int startPos = offset;
		final int limit = offset + numBytes;

		for (int field = 0, output = 0; field < fieldIncluded.length; field++) {

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
				@SuppressWarnings("unchecked")
				FieldParser<Object> parser = (FieldParser<Object>) this.getFieldParsers()[output];
				int latestValidPos = startPos;
				startPos = parser.resetErrorStateAndParse(bytes, startPos, limit, this.getFieldDelimiter(), holders[output]);
				if (!isLenient() && parser.getErrorState() != ParseErrorState.NONE) {
					// Row is able to handle null values
					if (parser.getErrorState() != ParseErrorState.EMPTY_STRING) {
						throw new ParseException(
								String.format("Parsing error for column %s of row '%s' originated by %s: %s.", field,
										new String(bytes, offset, numBytes),
										parser.getClass().getSimpleName(), parser.getErrorState()));
					}
				}
				holders[output] = parser.getLastResult();

				// check parse result
				if (startPos < 0) {
					holders[output] = null;
					startPos = skipFields(bytes, latestValidPos, limit, this.getFieldDelimiter());
				}
				output++;
			} else {
				// skip field
				startPos = skipFields(bytes, startPos, limit, this.getFieldDelimiter());
			}
		}
		return true;
	}

}
