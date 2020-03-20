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

package org.apache.flink.streaming.connectors.elasticsearch;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableException;
import org.apache.flink.types.Row;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * User can use {@link IndexFormatter} to format index value from index pattern.
 *
 * <p>If the index is a dynamic index, the index pattern include general pattern like
 * 'connector.index'='my-index-{item}' and time extract pattern like 'connector.index'='my-index-{log_ts|yyyy-MM-dd}'.
 *
 * <p>For general pattern:
 * 'item' the index field comes from any type column.
 *
 * <p>For time extract pattern:
 * 'log_ts' is the index field comes from a varchar/timestamp column.
 * 'yyyy-MM-dd' is the date format follows the {@link SimpleDateFormat} syntax.
 * '{log_ts|yyyy-MM-dd}' is the time extract pattern for index in a dynamic index pattern.
 */
@Internal
public class IndexFormatter implements Serializable {

	private static final Pattern dynamicIndexPattern = Pattern.compile(".*\\{.+\\}.*");
	private static final Pattern dynamicIndexTimeExtractPattern = Pattern.compile(".*\\{.+\\|.*\\}.*");
	private static final String DEFAULT_FORMAT = "yyyy-MM-dd";

	private String index;
	private boolean dynamicIndexEnabled;
	private boolean dynamicIndexTimeExtractEnabled;
	private String dynamicIndexPatternStr;

	private int indexFieldPos;
	private TypeInformation indexFieldType;
	private SimpleDateFormat dateFormat;

	IndexFormatter(String index, String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		this.index = index;
		this.dynamicIndexEnabled = checkDynamicIndexEnabled();
		this.dynamicIndexPatternStr = extractDynamicIndexPatternStr();
		this.dynamicIndexTimeExtractEnabled = checkDynamicIndexTimeExtractEnabled();
		this.indexFieldPos = extractIndexFieldPos(fieldNames);
		this.indexFieldType = extractIndexFieldType(fieldTypes);
		this.dateFormat = extractDateFormat();
	}

	/**
	 * Builder for {@link IndexFormatter}.
	 */
	public static class Builder {
		private String index;
		private String[] fieldNames;
		private TypeInformation<?>[] fieldTypes;

		public Builder index(String index) {
			this.index = index;
			return this;
		}

		public Builder fieldNames(String[] fieldNames) {
			this.fieldNames = fieldNames;
			return this;
		}

		public Builder fieldTypes(TypeInformation<?>[] fieldTypes) {
			this.fieldTypes = fieldTypes;
			return this;
		}

		public IndexFormatter build() {
			return new IndexFormatter(index, fieldNames, fieldTypes);
		}
	}

	/**
	 * Creates a new builder for {@link IndexFormatter}.
	 */
	public static Builder builder() {
		return new Builder();
	}

	public boolean isDynamicIndexEnabled() {
		return dynamicIndexEnabled;
	}

	/**
	 * Return dynamic index if dynamic index is enabled, else return the static index.
	 */
	public String getFormattedIndex(Row row) {
		if (!isDynamicIndexEnabled()) {
			return index;
		}
		Object indexFiled = row.getField(indexFieldPos);
		String indexFiledValueStr = indexFiled.toString();

		if (dynamicIndexTimeExtractEnabled) {
			if (indexFieldType == Types.LONG) {
				indexFiledValueStr = dateFormat.format(new Date((Long) indexFiled));
			} else if (indexFieldType == Types.SQL_TIMESTAMP) {
				indexFiledValueStr = dateFormat.format((Timestamp) indexFiled);
			} else if (indexFieldType == Types.SQL_DATE) {
				indexFiledValueStr = dateFormat.format((Date) indexFiled);
			} else {
				throw new TableException(String.format("Unsupported type '%s' found in Elasticsearch dynamic index column:, " +
					"extract time-related pattern only support type 'LONG'、'SQL_TIMESTAMP' and 'SQL_DATE'.", indexFieldType));
			}
		}
		return StringUtils.replace(index, dynamicIndexPatternStr, indexFiledValueStr);
	}

	/**
	 * Check general dynamic index is enabled or not by index pattern.
	 */
	private boolean checkDynamicIndexEnabled() {
		return dynamicIndexPattern.matcher(index).matches();
	}

	/**
	 * Check time extract dynamic index is enabled or not by index pattern.
	 */
	private boolean checkDynamicIndexTimeExtractEnabled() {
		return dynamicIndexTimeExtractPattern.matcher(index).matches();
	}

	/**
	 * Extract dynamic index pattern string from index pattern string.
	 */
	private String extractDynamicIndexPatternStr() {
		String dynamicIndexPatternStr = null;
		if (dynamicIndexEnabled) {
			int start = StringUtils.indexOf(index, "{");
			int end = StringUtils.lastIndexOf(index, "}");
			if (end == index.length() - 1) {
				dynamicIndexPatternStr =  StringUtils.substring(index, start);
			} else {
				dynamicIndexPatternStr =  StringUtils.substring(index, start, end + 1);
			}
		}
		return dynamicIndexPatternStr;
	}

	/**
	 * Extract index field position in a fieldNames, return the field position.
	 */
	private int extractIndexFieldPos(String[] fieldNames) {
		int pos = 0;
		if (dynamicIndexEnabled) {
			List<String> fieldList = Arrays.asList(fieldNames);
			String indexFieldName;
			if (dynamicIndexTimeExtractEnabled) {
				indexFieldName = StringUtils.substringBetween(index, "{", "|");
			} else {
				indexFieldName = StringUtils.substringBetween(index, "{", "}");
			}
			if (!fieldList.contains(indexFieldName)) {
				throw new TableException(String.format("Unknown column '%s' in index pattern '%s', please check the column name.",
					indexFieldName, index));
			}
			pos = fieldList.indexOf(indexFieldName);
		}
		return pos;
	}

	/**
	 * Extract index field type.
	 */
	private TypeInformation<?> extractIndexFieldType(TypeInformation<?>[] fieldTypes) {
		return fieldTypes[this.indexFieldPos];
	}

	/**
	 * Extract {@link SimpleDateFormat} by the date format that extracted from index pattern string.
	 */
	private SimpleDateFormat extractDateFormat() {
		String format = DEFAULT_FORMAT;
		if (dynamicIndexTimeExtractEnabled) {
			format = StringUtils.substringBetween(index, "|", "}");
		}
		return new SimpleDateFormat(format);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof IndexFormatter)) {
			return false;
		}
		IndexFormatter that = (IndexFormatter) o;
		return dynamicIndexEnabled == that.dynamicIndexEnabled &&
			dynamicIndexTimeExtractEnabled == that.dynamicIndexTimeExtractEnabled &&
			indexFieldPos == that.indexFieldPos &&
			Objects.equals(index, that.index) &&
			Objects.equals(dynamicIndexPatternStr, that.dynamicIndexPatternStr) &&
			Objects.equals(indexFieldType, that.indexFieldType) &&
			Objects.equals(dateFormat, that.dateFormat);
	}

	@Override
	public int hashCode() {
		return Objects.hash(index, dynamicIndexEnabled, dynamicIndexTimeExtractEnabled, dynamicIndexPatternStr, indexFieldPos, indexFieldType, dateFormat);
	}
}
