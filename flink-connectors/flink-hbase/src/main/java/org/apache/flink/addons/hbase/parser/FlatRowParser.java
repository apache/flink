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
import org.apache.flink.addons.hbase.util.HBaseBytesSerializer;
import org.apache.flink.addons.hbase.util.HBaseTypeUtils;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.addons.hbase.HBaseValidator.COLUMNFAMILY_QUALIFIER_DELIMITER_PATTERN;
import static org.apache.flink.addons.hbase.HBaseValidator.CONNECTOR_QUALIFIER_DELIMITER;

/**
 * fieldName like c1:f1 is composed of familyName and qualifyName with delimiter.
 */
public class FlatRowParser implements RowParser<Result> {
	/**
	 * NOTICE: The HBase table's primary key can only contain one column.
	 */
	private final TypeInformation rowKeyType;
	private final int rowKeyInternalTypeIndex;
	private final int rowKeySourceIndex;
	private final int totalQualifiers;
	/**
	 * qualifier fields' indexes in source input row.
	 */
	private final List<Integer> qualifierSourceIndexes;
	private final List<Tuple3<byte[], byte[], TypeInformation<?>>> qualifierList;
	/**
	 * field serializer for HBase format, order by field index of input row.
	 */
	private final List<HBaseBytesSerializer> inputFieldSerializers;

	public FlatRowParser(
		TableSchema tableSchema, HBaseTableSchema hBaseTableSchema, DescriptorProperties descriptorProperties) {
		String rowkey = hackRowkey(tableSchema, descriptorProperties);
		this.totalQualifiers = tableSchema.getFieldCount() - 1;

		this.qualifierList = hBaseTableSchema.getFlatByteQualifiers();
		this.rowKeySourceIndex = ParserHelper.initRowkeyIndex(tableSchema, rowkey);
		this.qualifierSourceIndexes = initHBaseTableSchema(tableSchema);
		DataType[] typeInformations = tableSchema.getFieldDataTypes();
		rowKeyType = TypeConversions.fromDataTypeToLegacyInfo(typeInformations[this.rowKeySourceIndex]);
		String charset = hBaseTableSchema.getStringCharset();
		this.inputFieldSerializers = new ArrayList<>();
		this.rowKeyInternalTypeIndex = HBaseTypeUtils.getTypeIndex(rowKeyType);

		for (int index = 0; index <= totalQualifiers; index++) {
			if (index == rowKeySourceIndex) {
				inputFieldSerializers.add(new HBaseBytesSerializer(rowKeyType, charset));
			} else {
				Tuple3<byte[], byte[], TypeInformation<?>> typeInfo;
				if (index < rowKeySourceIndex) {
					typeInfo = qualifierList.get(index);
				} else {
					typeInfo = qualifierList.get(index - 1);
				}
				inputFieldSerializers.add(new HBaseBytesSerializer(typeInfo.f2, charset));
			}
		}
	}

	@Override
	public Get createGet(Object rowKey) throws IOException {
		// currently this rowKey always be a binary type (adapt to sql-level type system)
		byte[] rowkey = HBaseTypeUtils.serializeFromObject(rowKey,
			rowKeyInternalTypeIndex,
			HBaseTypeUtils.DEFAULT_CHARSET);
		Get get = new Get(rowkey);
		get.setMaxVersions(1);
		// add request columns
		for (Tuple3<byte[], byte[], TypeInformation<?>> typeInfo : qualifierList) {
			get.addColumn(typeInfo.f0, typeInfo.f1);
		}
		return get;
	}

	@Override
	public Row parseToRow(Result result, Object rowKey) throws IOException {
		// output rowKey + qualifiers
		Row row = new Row(totalQualifiers + 1);
		row.setField(rowKeySourceIndex, rowKey);
		for (int idx = 0; idx < totalQualifiers; idx++) {
			Tuple3<byte[], byte[], TypeInformation<?>> qInfo = qualifierList.get(idx);
			byte[] value = result.getValue(qInfo.f0, qInfo.f1);
			int qualifierSrcIdx = qualifierSourceIndexes.get(idx);
			row.setField(qualifierSrcIdx, inputFieldSerializers.get(qualifierSrcIdx).fromHBaseBytes(value));
		}
		return row;
	}

	private List<Integer> initHBaseTableSchema(TableSchema tableSchema) {
		Preconditions.checkArgument(this.rowKeySourceIndex > -1, "must invoke initRokeyIndex first");

		String[] columnNames = tableSchema.getFieldNames();
		List<Integer> qualifierSourceIndexes = new ArrayList<>();

		for (int idx = 0; idx < columnNames.length; idx++) {
			if (idx != this.rowKeySourceIndex) {
				qualifierSourceIndexes.add(idx);
			}
		}
		return qualifierSourceIndexes;
	}

	/**
	 * for flat schema.
	 */
	private String hackRowkey(TableSchema tableSchema, DescriptorProperties descriptorProperties) {
		String[] columnNames = tableSchema.getFieldNames();
		String delimiter = descriptorProperties.getOptionalString(CONNECTOR_QUALIFIER_DELIMITER).orElse(
			COLUMNFAMILY_QUALIFIER_DELIMITER_PATTERN);
		String rowkey = null;
		int rowkeyCount = 0;
		for (String column : columnNames) {
			if (!column.contains(delimiter)) {
				rowkey = column;
				rowkeyCount++;
			}
		}
		Preconditions.checkArgument(rowkeyCount == 1,
			"a column which doesn't contain delimiter(" + delimiter + ") is regarded as rowkey, now only support 1 rowkey, but now has " + rowkeyCount);
		return rowkey;
	}

	@VisibleForTesting
	public int getRowKeyIndex() {
		return this.rowKeySourceIndex;
	}

	@VisibleForTesting
	public List<Integer> getQualifierIndexes() {
		return this.qualifierSourceIndexes;
	}

	@VisibleForTesting
	public TypeInformation getRowKeyType() {
		return rowKeyType;
	}
}
