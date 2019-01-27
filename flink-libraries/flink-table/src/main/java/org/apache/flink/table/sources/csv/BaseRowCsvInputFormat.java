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

package org.apache.flink.table.sources.csv;

import org.apache.flink.api.common.typeinfo.BigDecimalTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.AbstractRowCsvInputFormat;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.TypeConverters;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.runtime.functions.BuildInScalarFunctions;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;
import org.apache.flink.types.parser.BigDecParser;
import org.apache.flink.types.parser.FieldParser;
import org.apache.flink.types.parser.SqlDateParser;
import org.apache.flink.types.parser.SqlTimeParser;
import org.apache.flink.types.parser.SqlTimestampParser;
import org.apache.flink.types.parser.StringParser;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.TimeZone;

/**
 * Input format that reads csv into {@link BaseRow}.
 */
public class BaseRowCsvInputFormat extends AbstractRowCsvInputFormat<BaseRow> {

	private static final long serialVersionUID = 1L;

	private transient GenericRow reuseRow;

	public BaseRowCsvInputFormat(
			Path filePath, InternalType[] fieldTypes, String lineDelimiter,
			String fieldDelimiter, int[] selectedFields, boolean emptyColumnAsNull) {
		this(filePath, fieldTypes, lineDelimiter, fieldDelimiter, selectedFields,
				emptyColumnAsNull, Long.MAX_VALUE);
	}

	public BaseRowCsvInputFormat(
			Path filePath, InternalType[] fieldTypes, String lineDelimiter,
			String fieldDelimiter, int[] selectedFields, boolean emptyColumnAsNull, long limit) {
		super(filePath, TypeConverters.createExternalTypeInfoFromDataTypes(fieldTypes), lineDelimiter, fieldDelimiter,
				selectedFields, emptyColumnAsNull, limit);
	}

	@Override
	public void open(FileInputSplit split) throws IOException {
		super.open(split);
		FieldParser<?>[] fieldParsers = getFieldParsers();
		for (int i = 0; i < fieldParsers.length; i++) {
			if (fieldParsers[i] instanceof StringParser) {
				StringParser parser = (StringParser) fieldParsers[i];
				BinaryStringParser newParser = new BinaryStringParser();
				if (parser.isQuotedStringParsing()) {
					newParser.enableQuotedStringParsing(parser.getQuoteCharacter());
				}
				fieldParsers[i] = newParser;
			} else if (fieldParsers[i] instanceof SqlDateParser) {
				fieldParsers[i] = new DateParser();
			} else if (fieldParsers[i] instanceof SqlTimeParser) {
				fieldParsers[i] = new TimeParser();
			} else if (fieldParsers[i] instanceof SqlTimestampParser) {
				fieldParsers[i] = new TimestampParser(getTimezone());
			} else if (fieldParsers[i] instanceof BigDecParser) {
				fieldParsers[i] = new DecimalParser((BigDecimalTypeInfo) fieldTypeInfos[i]);
			}
			this.parsedValues[i] = fieldParsers[i].createValue();
		}
	}

	@Override
	protected BaseRow fillRecord(BaseRow ignore, Object[] parsedValues) {
		if (reuseRow == null) {
			reuseRow = new GenericRow(arity);
		}
		for (int i = 0; i < parsedValues.length; i++) {
			reuseRow.update(i, parsedValues[i]);
		}
		return reuseRow;
	}

	@Override
	public TypeInformation<BaseRow> getProducedType() {
		return new BaseRowTypeInfo(this.fieldTypeInfos);
	}

	/**
	 * parser for BinaryString.
	 */
	public static class BinaryStringParser extends FieldParser<BinaryString> {

		private boolean quotedStringParsing = false;
		private byte quoteCharacter;
		// RFC 4180
		private String singleQuoteStr;
		private String doubleQuoteStr;
		private static final byte BACKSLASH = 92;

		private BinaryString result;

		public void enableQuotedStringParsing(byte quoteCharacter) {
			this.quotedStringParsing = true;
			this.quoteCharacter = quoteCharacter;
			byte[] doubleQuoteChars = {quoteCharacter, quoteCharacter};
			doubleQuoteStr = new String(doubleQuoteChars);
			byte[] singleQuoteChar = {quoteCharacter};
			singleQuoteStr = new String(singleQuoteChar);
		}

		@Override
		public int parseField(byte[] bytes, int startPos, int limit, byte[] delimiter, BinaryString reusable) {

			int i = startPos;

			final int delimLimit = limit - delimiter.length + 1;

			if (quotedStringParsing && bytes[i] == quoteCharacter) {
				// quoted string parsing enabled and first character is a quote
				i++;

				// search for ending quote character, continue when it is escaped
				boolean replace = false;
				// search for ending quote character, continue when it is escaped
				while (i < limit && (bytes[i] != quoteCharacter
						|| bytes[i - 1] == BACKSLASH
						|| (i + 1 < limit && bytes[i + 1] == quoteCharacter))) {
					if (bytes[i - 1] != BACKSLASH
							&& bytes[i] == quoteCharacter
							&& i + 1 < limit
							&& bytes[i + 1] == quoteCharacter) {
						// RFC 4180
						replace = true;
						i++;
					}
					i++;
				}

				if (i == limit) {
					setErrorState(ParseErrorState.UNTERMINATED_QUOTED_STRING);
					return -1;
				} else {
					i++;
					try {
						// check for proper termination
						if (i == limit) {
							// either by end of line
							if (replace || getCharset().equals(StandardCharsets.UTF_8)) {
								String res = new String(bytes, startPos + 1, i - startPos - 2, getCharset());
								// RFC 4180
								this.result = BinaryString.fromBytes(
										res.replace(doubleQuoteStr, singleQuoteStr).getBytes("utf-8"));
							} else {
								this.result = BinaryString.fromBytes(bytes, startPos + 1, i - startPos - 2);
							}
							return limit;
						} else if (i < delimLimit && delimiterNext(bytes, i, delimiter)) {
							// or following field delimiter
							if (replace && getCharset().equals(StandardCharsets.UTF_8)) {
								String res = new String(bytes, startPos + 1, i - startPos - 2, getCharset());
								// RFC 4180
								this.result = BinaryString.fromBytes(
										res.replace(doubleQuoteStr, singleQuoteStr).getBytes("utf-8"));
							} else {
								this.result = BinaryString.fromBytes(bytes, startPos + 1, i - startPos - 2);
							}
							return i + delimiter.length;
						} else {
							// no proper termination
							setErrorState(ParseErrorState.UNQUOTED_CHARS_AFTER_QUOTED_STRING);
							return -1;
						}
					} catch (UnsupportedEncodingException e) {
						setErrorState(ParseErrorState.CHARSET_INVALID);
						return -1;
					}
				}
			} else {

				// look for delimiter
				while (i < delimLimit && !delimiterNext(bytes, i, delimiter)) {
					i++;
				}

				if (i >= delimLimit) {
					// no delimiter found. Take the full string
					if (limit == startPos) {
						setErrorState(ParseErrorState.EMPTY_COLUMN); // mark empty column
					}
					this.result = BinaryString.fromBytes(bytes, startPos, limit - startPos);
					return limit;
				} else {
					// delimiter found.
					if (i == startPos) {
						setErrorState(ParseErrorState.EMPTY_COLUMN); // mark empty column
					}
					this.result = BinaryString.fromBytes(bytes, startPos, i - startPos);
					return i + delimiter.length;
				}
			}
		}

		@Override
		public BinaryString createValue() {
			return BinaryString.EMPTY_UTF8;
		}

		@Override
		public BinaryString getLastResult() {
			return this.result;
		}
	}

	/**
	 * Date parser.
	 */
	public static class DateParser extends FieldParser<Integer> {

		final SqlDateParser parser = new SqlDateParser();

		@Override
		public int parseField(byte[] bytes, int startPos, int limit, byte[] delimiter, Integer reusable) {
			return parser.parseField(bytes, startPos, limit, delimiter, null);
		}

		@Override
		public Integer createValue() {
			return 0;
		}

		@Override
		public Integer getLastResult() {
			return BuildInScalarFunctions.toInt(parser.getLastResult());
		}
	}

	/**
	 * TimeParser.
	 */
	public static class TimeParser extends FieldParser<Integer> {

		final SqlTimeParser parser = new SqlTimeParser();

		@Override
		public int parseField(byte[] bytes, int startPos, int limit, byte[] delimiter, Integer reusable) {
			return parser.parseField(bytes, startPos, limit, delimiter, null);
		}

		@Override
		public Integer createValue() {
			return 0;
		}

		@Override
		public Integer getLastResult() {
			return BuildInScalarFunctions.toInt(parser.getLastResult());
		}
	}

	/**
	 * TimestampParser.
	 */
	public static class TimestampParser extends FieldParser<Long> {

		final SqlTimestampParser parser = new SqlTimestampParser();
		public TimestampParser(TimeZone timezone) {
			parser.setTimeZone(timezone);
		}

		@Override
		public int parseField(byte[] bytes, int startPos, int limit, byte[] delimiter, Long reusable) {
			return parser.parseField(bytes, startPos, limit, delimiter, null);
		}

		@Override
		public Long createValue() {
			return 0L;
		}

		@Override
		public Long getLastResult() {
			return BuildInScalarFunctions.toLong(parser.getLastResult());
		}
	}

	/**
	 * DecimalParser.
	 */
	public static class DecimalParser extends FieldParser<Decimal> {

		final BigDecParser parser = new BigDecParser();
		final BigDecimalTypeInfo dt;

		public DecimalParser(BigDecimalTypeInfo dt) {
			this.dt = dt;
		}

		@Override
		protected int parseField(byte[] bytes, int startPos, int limit, byte[] delim, Decimal reuse) {
			return parser.parseField(bytes, startPos, limit, delim, null);
		}

		@Override
		public Decimal getLastResult() {
			return Decimal.fromBigDecimal(parser.getLastResult(), dt.precision(), dt.scale());
		}

		@Override
		public Decimal createValue() {
			return Decimal.zero(dt.precision(), dt.scale());
		}
	}
}
