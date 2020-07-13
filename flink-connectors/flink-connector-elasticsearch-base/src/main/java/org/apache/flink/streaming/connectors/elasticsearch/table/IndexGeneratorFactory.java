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

package org.apache.flink.streaming.connectors.elasticsearch.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import javax.annotation.Nonnull;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Factory of {@link IndexGenerator}.
 *
 * <p>Flink supports both static index and dynamic index.
 *
 * <p>If you want to have a static index, this option value should be a plain string, e.g. 'myusers',
 * all the records will be consistently written into "myusers" index.
 *
 * <p>If you want to have a dynamic index, you can use '{field_name}' to reference a field value in the
 * record to dynamically generate a target index. You can also use '{field_name|date_format_string}' to
 * convert a field value of TIMESTAMP/DATE/TIME type into the format specified by date_format_string. The
 * date_format_string is compatible with {@link java.text.SimpleDateFormat}. For example, if the option
 * value is 'myusers_{log_ts|yyyy-MM-dd}', then a record with log_ts field value 2020-03-27 12:25:55 will
 * be written into "myusers_2020-03-27" index.
 */
@Internal
final class IndexGeneratorFactory {

	private IndexGeneratorFactory() {}

	public static IndexGenerator createIndexGenerator(String index, TableSchema schema) {
		final IndexHelper indexHelper = new IndexHelper();
		if (indexHelper.checkIsDynamicIndex(index)) {
			return createRuntimeIndexGenerator(index, schema.getFieldNames(), schema.getFieldDataTypes(), indexHelper);
		} else {
			return new StaticIndexGenerator(index);
		}
	}

	interface DynamicFormatter extends Serializable {
		String format(@Nonnull Object fieldValue, DateTimeFormatter formatter);
	}

	private static IndexGenerator createRuntimeIndexGenerator(
			String index,
			String[] fieldNames,
			DataType[] fieldTypes,
			IndexHelper indexHelper) {
		final String dynamicIndexPatternStr = indexHelper.extractDynamicIndexPatternStr(index);
		final String indexPrefix = index.substring(0, index.indexOf(dynamicIndexPatternStr));
		final String indexSuffix = index.substring(indexPrefix.length() + dynamicIndexPatternStr.length());

		final boolean isDynamicIndexWithFormat = indexHelper.checkIsDynamicIndexWithFormat(index);
		final int indexFieldPos = indexHelper.extractIndexFieldPos(index, fieldNames, isDynamicIndexWithFormat);
		final LogicalType indexFieldType = fieldTypes[indexFieldPos].getLogicalType();
		final LogicalTypeRoot indexFieldLogicalTypeRoot = indexFieldType.getTypeRoot();

		// validate index field type
		indexHelper.validateIndexFieldType(indexFieldLogicalTypeRoot);

		// time extract dynamic index pattern
		final RowData.FieldGetter fieldGetter = RowData.createFieldGetter(indexFieldType, indexFieldPos);

		if (isDynamicIndexWithFormat) {
			final String dateTimeFormat = indexHelper.extractDateFormat(index, indexFieldLogicalTypeRoot);
			DynamicFormatter formatFunction = createFormatFunction(
				indexFieldType,
				indexFieldLogicalTypeRoot);

			return new AbstractTimeIndexGenerator(index, dateTimeFormat) {
				@Override
				public String generate(RowData row) {
					Object fieldOrNull = fieldGetter.getFieldOrNull(row);
					final String formattedField;
					// TODO we can possibly optimize it to use the nullability of the field
					if (fieldOrNull != null) {
						formattedField = formatFunction.format(fieldOrNull, dateTimeFormatter);
					} else {
						formattedField = "null";
					}
					return indexPrefix.concat(formattedField).concat(indexSuffix);
				}
			};
		}
		// general dynamic index pattern
		return new IndexGeneratorBase(index) {
			@Override
			public String generate(RowData row) {
				Object indexField = fieldGetter.getFieldOrNull(row);
				return indexPrefix.concat(indexField == null ? "null" : indexField.toString()).concat(indexSuffix);
			}
		};
	}

	private static DynamicFormatter createFormatFunction(
			LogicalType indexFieldType,
			LogicalTypeRoot indexFieldLogicalTypeRoot) {
		switch (indexFieldLogicalTypeRoot) {
			case DATE:
				return (value, dateTimeFormatter) -> {
					Integer indexField = (Integer) value;
					return LocalDate.ofEpochDay(indexField).format(dateTimeFormatter);
				};
			case TIME_WITHOUT_TIME_ZONE:
				return (value, dateTimeFormatter) -> {
					Integer indexField = (Integer) value;
					return LocalTime.ofNanoOfDay(indexField * 1_000_000L)
						.format(dateTimeFormatter);
				};
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				return (value, dateTimeFormatter) -> {
					TimestampData indexField = (TimestampData) value;
					return indexField.toLocalDateTime().format(dateTimeFormatter);
				};
			case TIMESTAMP_WITH_TIME_ZONE:
				throw new UnsupportedOperationException("TIMESTAMP_WITH_TIME_ZONE is not supported yet");
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				return (value, dateTimeFormatter) -> {
					TimestampData indexField = (TimestampData) value;
					return indexField.toInstant().atZone(ZoneOffset.UTC).format(dateTimeFormatter);
				};
			default:
				throw new TableException(String.format(
					"Unsupported type '%s' found in Elasticsearch dynamic index field, " +
						"time-related pattern only support types are: DATE,TIME,TIMESTAMP.",
					indexFieldType));
		}
	}

	/**
	 * Helper class for {@link IndexGeneratorFactory}, this helper can use to validate index field type
	 * ans parse index format from pattern.
	 */
	private static class IndexHelper {
		private static final Pattern dynamicIndexPattern = Pattern.compile("\\{[^\\{\\}]+\\}?");
		private static final Pattern dynamicIndexTimeExtractPattern = Pattern.compile(".*\\{.+\\|.*\\}.*");
		private static final List<LogicalTypeRoot> supportedTypes = new ArrayList<>();
		private static final Map<LogicalTypeRoot, String> defaultFormats = new HashMap<>();

		static {
			//time related types
			supportedTypes.add(LogicalTypeRoot.DATE);
			supportedTypes.add(LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE);
			supportedTypes.add(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE);
			supportedTypes.add(LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE);
			supportedTypes.add(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
			//general types
			supportedTypes.add(LogicalTypeRoot.VARCHAR);
			supportedTypes.add(LogicalTypeRoot.CHAR);
			supportedTypes.add(LogicalTypeRoot.TINYINT);
			supportedTypes.add(LogicalTypeRoot.INTEGER);
			supportedTypes.add(LogicalTypeRoot.BIGINT);
		}

		static {

			defaultFormats.put(LogicalTypeRoot.DATE, "yyyy_MM_dd");
			defaultFormats.put(LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE, "HH_mm_ss");
			defaultFormats.put(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE, "yyyy_MM_dd_HH_mm_ss");
			defaultFormats.put(LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE, "yyyy_MM_dd_HH_mm_ss");
			defaultFormats.put(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE, "yyyy_MM_dd_HH_mm_ssX");
		}

		/**
		 * Validate the index field Type.
		 */
		void validateIndexFieldType(LogicalTypeRoot logicalType) {
			if (!supportedTypes.contains(logicalType)) {
				throw new IllegalArgumentException(String.format("Unsupported type %s of index field, " +
					"Supported types are: %s", logicalType, supportedTypes));
			}
		}

		/**
		 * Get the default date format.
		 */
		String getDefaultFormat(LogicalTypeRoot logicalType) {
			return defaultFormats.get(logicalType);
		}

		/**
		 * Check general dynamic index is enabled or not by index pattern.
		 */
		boolean checkIsDynamicIndex(String index) {
			final Matcher matcher = dynamicIndexPattern.matcher(index);
			int count = 0;
			while (matcher.find()) {
				count++;
			}
			if (count > 1) {
				throw new TableException(String.format("Chaining dynamic index pattern %s is not supported," +
					" only support single dynamic index pattern.", index));
			}
			return count == 1;
		}

		/**
		 * Check time extract dynamic index is enabled or not by index pattern.
		 */
		boolean checkIsDynamicIndexWithFormat(String index) {
			return dynamicIndexTimeExtractPattern.matcher(index).matches();
		}

		/**
		 * Extract dynamic index pattern string from index pattern string.
		 */
		String extractDynamicIndexPatternStr(String index) {
			int start = index.indexOf("{");
			int end = index.lastIndexOf("}");
			return index.substring(start, end + 1);
		}

		/**
		 * Extract index field position in a fieldNames, return the field position.
		 */
		int extractIndexFieldPos(String index, String[] fieldNames, boolean isDynamicIndexWithFormat) {
			List<String> fieldList = Arrays.asList(fieldNames);
			String indexFieldName;
			if (isDynamicIndexWithFormat) {
				indexFieldName = index.substring(index.indexOf("{") + 1, index.indexOf("|"));
			} else {
				indexFieldName = index.substring(index.indexOf("{") + 1, index.indexOf("}"));
			}
			if (!fieldList.contains(indexFieldName)) {
				throw new TableException(String.format("Unknown field '%s' in index pattern '%s', please check the field name.",
					indexFieldName, index));
			}
			return fieldList.indexOf(indexFieldName);
		}

		/**
		 * Extract dateTime format by the date format that extracted from index pattern string.
		 */
		private String extractDateFormat(String index, LogicalTypeRoot logicalType) {
			String format =  index.substring(index.indexOf("|") + 1, index.indexOf("}"));
			if ("".equals(format)) {
				format = getDefaultFormat(logicalType);
			}
			return format;
		}
	}
}
