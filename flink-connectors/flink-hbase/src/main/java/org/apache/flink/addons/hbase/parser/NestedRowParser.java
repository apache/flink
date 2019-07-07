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

package org.apache.flink.addons.hbase.parser;

import org.apache.flink.addons.hbase.HBaseTableSchema;
import org.apache.flink.addons.hbase.util.HBaseTypeUtils;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * fieldName is only familyName, filedType is RowTypeInfo which indicates all of the Qualifier name and type.
 */
public class NestedRowParser implements RowParser<Result> {
	// family keys
	private final byte[][] families;
	// qualifier keys
	private final byte[][][] qualifiers;
	// qualifier types
	private final int[][] qualifierTypes;

	private final int rowKeySourceIndex;
	private final TypeInformation rowKeyType;
	private final int rowKeyInternalTypeIndex;

	private final int fieldLength;
	private final transient String charset;

	public NestedRowParser(TableSchema tableSchema, HBaseTableSchema hBaseSchema) {
		String rowkey = hackRowkey(tableSchema);
		String[] familyNames = hBaseSchema.getFamilyNames();
		this.families = hBaseSchema.getFamilyKeys();
		this.qualifiers = new byte[this.families.length][][];
		this.qualifierTypes = new int[this.families.length][];
		for (int f = 0; f < families.length; f++) {
			this.qualifiers[f] = hBaseSchema.getQualifierKeys(familyNames[f]);
			TypeInformation[] typeInfos = hBaseSchema.getQualifierTypes(familyNames[f]);
			this.qualifierTypes[f] = new int[typeInfos.length];
			for (int i = 0; i < typeInfos.length; i++) {
				qualifierTypes[f][i] = HBaseTypeUtils.getTypeIndex(typeInfos[i]);
			}
		}
		DataType[] typeInformations = tableSchema.getFieldDataTypes();
		this.rowKeySourceIndex = ParserHelper.initRowkeyIndex(tableSchema, rowkey);
		rowKeyType = TypeConversions.fromDataTypeToLegacyInfo(typeInformations[this.rowKeySourceIndex]);
		this.rowKeyInternalTypeIndex = HBaseTypeUtils.getTypeIndex(rowKeyType);
		this.fieldLength = families.length + 1;
		this.charset = hBaseSchema.getStringCharset();
	}

	private String hackRowkey(TableSchema tableSchema) {
		String[] columnNames = tableSchema.getFieldNames();
		TypeInformation[] columnTypes = tableSchema.getFieldTypes();
		String rowkey = null;
		int rowkeyCount = 0;
		for (int i = 0; i < columnTypes.length; i++) {
			if (!(columnTypes[i] instanceof RowTypeInfo)) {
				rowkey = columnNames[i];
				rowkeyCount++;
			}
		}
		Preconditions.checkArgument(rowkeyCount == 1,
			"a column whose type is not RowInfoType is regarded as rowkey, now only support 1 rowkey, but now has " + rowkeyCount);
		return rowkey;
	}

	@Override
	public Get createGet(Object rowKey) throws IOException {
		byte[] rowkey = HBaseTypeUtils.serializeFromObject(rowKey,
			rowKeyInternalTypeIndex,
			HBaseTypeUtils.DEFAULT_CHARSET);
		Get get = new Get(rowkey);
		for (int f = 0; f < families.length; f++) {
			byte[] family = families[f];
			for (byte[] qualifier : qualifiers[f]) {
				get.addColumn(family, qualifier);
			}
		}
		return get;
	}

	@Override
	public Row parseToRow(Result result, Object rowKey) throws IOException {
		Row row = new Row(fieldLength);

		for (int i = 0; i < fieldLength; i++) {
			if (rowKeySourceIndex == i) {
				row.setField(rowKeySourceIndex, rowKey);
			} else {
				int f = i > rowKeySourceIndex ? i - 1 : i;
				// get family key
				byte[] familyKey = families[f];
				Row familyRow = new Row(f);
				for (int q = 0; q < this.qualifiers[f].length; q++) {
					// get quantifier key
					byte[] qualifier = qualifiers[f][q];
					// get quantifier type idx
					int typeIdx = qualifierTypes[f][q];
					// read value
					byte[] value = result.getValue(familyKey, qualifier);
					if (value != null) {
						familyRow.setField(q, HBaseTypeUtils.deserializeToObject(value, typeIdx, charset));
					} else {
						familyRow.setField(q, null);
					}
				}
				row.setField(i, familyRow);
			}
		}
		return row;
	}

	@VisibleForTesting
	public int getRowKeyIndex() {
		return this.rowKeySourceIndex;
	}

	@VisibleForTesting
	public TypeInformation<?> getRowKeyType() {
		return this.rowKeyType;
	}

	@VisibleForTesting
	public String[] getFamilies() throws UnsupportedEncodingException {

		String[] result = new String[families.length];
		for (int i = 0; i < families.length; i++) {
			result[i] = new String(families[i], charset);
		}
		return result;
	}

	@VisibleForTesting
	public String[][] getQualifiers() throws UnsupportedEncodingException {
		String[][] result = new String[families.length][qualifiers.length];
		for (int i = 0; i < qualifiers.length; i++) {
			result[i] = new String[qualifiers[i].length];
			for (int j = 0; j < qualifiers[i].length; j++) {
				result[i][j] = new String(qualifiers[i][j], charset);
			}
		}
		return result;
	}

	@VisibleForTesting
	public int[][] getQualifierTypes() {
		return qualifierTypes;
	}
}
