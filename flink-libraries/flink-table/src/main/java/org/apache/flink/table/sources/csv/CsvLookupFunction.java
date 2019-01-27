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

import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.functions.FunctionContext;
import org.apache.flink.table.api.functions.TableFunction;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.RowType;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.dataformat.TypeGetterSetters;
import org.apache.flink.table.sources.IndexKey;
import org.apache.flink.table.typeutils.AbstractRowSerializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * LookupFunction for Csv format.
 */
public class CsvLookupFunction extends TableFunction<BaseRow> {
	private final String path;
	private final InternalType[] fieldTypes;
	private final RowType rowType;
	private final TimeZone timezone;
	private final List<Integer> sourceKeys = new ArrayList<>();
	private final List<Integer> targetKeys = new ArrayList<>();
	private final List<InternalType> keyTypes = new ArrayList<>();
	private final int[] selectedFields;
	private final boolean emptyColumnAsNull;
	private final boolean nestedFileEnumerate;

	private final boolean uniqueIndex;
	private final Map<Object, List<BaseRow>> one2manyDataMap = new HashMap<>();
	private final Map<Object, BaseRow> one2oneDataMap = new HashMap<>();

	private String fieldDelim = CsvInputFormat.DEFAULT_FIELD_DELIMITER;
	private String lineDelim = CsvInputFormat.DEFAULT_LINE_DELIMITER;
	private String charsetName = "UTF-8";
	private Character quoteCharacter = null;
	private Boolean ignoreFirstLine = false;
	private String ignoreComments = null;
	private Boolean lenient = false;

	public CsvLookupFunction(
			String path,
			RowType rowType,
			IndexKey checkedIndex,
			boolean emptyColumnAsNull,
			TimeZone timezone,
			boolean nestedFileEnumerate) {
		this.path = path;
		this.rowType = rowType;
		this.fieldTypes = rowType.getFieldInternalTypes();
		this.uniqueIndex = checkedIndex.isUnique();
		List<Integer> indexCols = checkedIndex.getDefinedColumns();
		for (int i = 0; i < indexCols.size(); i++) {
			sourceKeys.add(i);
			int targetIdx = indexCols.get(i);
			assert targetIdx != -1;
			targetKeys.add(targetIdx);
			keyTypes.add(fieldTypes[targetIdx]);
		}
		selectedFields = new int[fieldTypes.length];
		for (int i = 0; i < selectedFields.length; i++) {
			selectedFields[i] = i;
		}
		this.emptyColumnAsNull = emptyColumnAsNull;
		this.timezone = (timezone == null) ? TimeZone.getTimeZone("UTC") : timezone;
		this.nestedFileEnumerate = nestedFileEnumerate;
	}

	public void setFieldDelim(String fieldDelim) {
		this.fieldDelim = fieldDelim;
	}

	public void setLineDelim(String lineDelim) {
		this.lineDelim = lineDelim;
	}

	public void setCharsetName(String charsetName) {
		this.charsetName = charsetName;
	}

	public void setQuoteCharacter(Character quoteCharacter) {
		this.quoteCharacter = quoteCharacter;
	}

	public void setIgnoreFirstLine(Boolean ignoreFirstLine) {
		this.ignoreFirstLine = ignoreFirstLine;
	}

	public void setIgnoreComments(String ignoreComments) {
		this.ignoreComments = ignoreComments;
	}

	public void setLenient(Boolean lenient) {
		this.lenient = lenient;
	}

	@Override
	public void open(FunctionContext context) throws Exception {
		super.open(context);
		AbstractRowSerializer<BaseRow> rowSerializer = (AbstractRowSerializer<BaseRow>) DataTypes.createInternalSerializer(rowType);
		BaseRowCsvInputFormat inputFormat = new BaseRowCsvInputFormat(
			new Path(path), fieldTypes, lineDelim, fieldDelim,
			selectedFields, emptyColumnAsNull);
		inputFormat.setTimezone(timezone);
		inputFormat.setSkipFirstLineAsHeader(ignoreFirstLine);
		inputFormat.setLenient(lenient);
		if (charsetName != null) {
			inputFormat.setCharset(charsetName);
		}
		if (quoteCharacter != null) {
			inputFormat.enableQuotedStringParsing(quoteCharacter);
		}
		if (ignoreComments != null) {
			inputFormat.setCommentPrefix(ignoreComments);
		}

		inputFormat.setNestedFileEnumeration(nestedFileEnumerate);

		FileInputSplit[] inputSplits = inputFormat.createInputSplits(1);
		for (FileInputSplit split : inputSplits) {
			inputFormat.open(split);
			GenericRow row = new GenericRow(rowType.getArity());
			while (true) {
				BaseRow r = inputFormat.nextRecord(row);
				if (r == null) {
					break;
				} else {
					Object key = getTargetKey(r);
					if (uniqueIndex) {
						// TODO exception when duplicate data on uk ?
						one2oneDataMap.put(key, rowSerializer.copy(r));
					} else {
						if (one2manyDataMap.containsKey(key)) {
							one2manyDataMap.get(key).add(rowSerializer.copy(r));
						} else {
							List<BaseRow> rows = new ArrayList<>();
							rows.add(rowSerializer.copy(r));
							one2manyDataMap.put(key, rows);
						}
					}
				}
			}
			inputFormat.close();
		}
	}

	public void eval(Object... values) {
		Object srcKey = getSourceKey(GenericRow.of(values));
		if (uniqueIndex) {
			if (one2oneDataMap.containsKey(srcKey)) {
				collect(one2oneDataMap.get(srcKey));
			}
		} else {
			if (one2manyDataMap.containsKey(srcKey)) {
				for (BaseRow row1 : one2manyDataMap.get(srcKey)) {
					collect(row1);
				}
			}
		}
	}

	private Object getSourceKey(BaseRow source) {
		return getKey(source, sourceKeys);
	}

	private Object getTargetKey(BaseRow target) {
		return getKey(target, targetKeys);
	}

	private Object getKey(BaseRow input, List<Integer> keys) {
		if (keys.size() == 1) {
			int keyIdx = keys.get(0);
			if (!input.isNullAt(keyIdx)) {
				return TypeGetterSetters.get(input, keyIdx, keyTypes.get(0));
			}
			return null;
		} else {
			GenericRow key = new GenericRow(keys.size());
			for (int i = 0; i < keys.size(); i++) {
				int keyIdx = keys.get(i);
				Object field = null;
				if (!input.isNullAt(keyIdx)) {
					field = TypeGetterSetters.get(input, keyIdx, keyTypes.get(i));
				}
				if (field == null) {
					return null;
				}
				key.update(i, field);
			}
			return key;
		}
	}

	@Override
	public void close() throws Exception {
		super.close();
	}
}
