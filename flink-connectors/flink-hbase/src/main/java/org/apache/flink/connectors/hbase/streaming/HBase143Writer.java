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

package org.apache.flink.connectors.hbase.streaming;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connectors.hbase.table.HBaseTableSchemaV2;
import org.apache.flink.connectors.hbase.util.HBaseBytesSerializer;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The HBase143Writer implements the sink function that can be used both in bounded and unbounded stream.
 */
public class HBase143Writer extends HBaseWriterBase<Tuple2<Boolean, Row>> {

	private static final Logger LOG = LoggerFactory.getLogger(HBase143Writer.class);

	// rowkey field index in source input row
	private final int rowKeySourceIndex;
	// qualifier fields' indexes in source input row
	private final List<Integer> qualifierSourceIndexes;

	private final List<Tuple3<byte[], byte[], TypeInformation<?>>> qualifierList;

	// field serializer for HBase format, order by field index of input row
	private List<HBaseBytesSerializer> inputFieldSerializers;
	private int totalQualifiers;
	private String charset;

	public HBase143Writer(
			String hbaseTableName,
			HBaseTableSchemaV2 hbaseSchema,
			int rowKeySourceIndex,
			List<Integer> qualifierSourceIndexes,
			Configuration hbaseConfiguration) throws IOException {
		super(hbaseTableName, hbaseSchema, hbaseConfiguration);

		this.totalQualifiers = hbaseSchema.getFamilySchema().getTotalQualifiers();
		Preconditions.checkArgument(null != qualifierSourceIndexes && totalQualifiers == qualifierSourceIndexes.size(),
			"qualifierSourceIndexes info should be consistent with qualifiers defined in HBaseTableSchema!");
		Preconditions.checkArgument(rowKeySourceIndex > -1 && totalQualifiers > rowKeySourceIndex,
			"rowKeySourceIndex must > -1 and totalQualifiers number must > rowKeySourceIndex");

		this.qualifierList = hbaseSchema.getFamilySchema().getFlatByteQualifiers();
		this.rowKeySourceIndex = rowKeySourceIndex;
		this.qualifierSourceIndexes = qualifierSourceIndexes;
		this.charset = hbaseSchema.getFamilySchema().getStringCharset();
		this.inputFieldSerializers = new ArrayList<>();

		for (int index = 0; index <= totalQualifiers; index++) {
			if (index == rowKeySourceIndex) {
				inputFieldSerializers.add(new HBaseBytesSerializer(hbaseSchema.getRowKeyType(), charset));
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
	public void invoke(Tuple2<Boolean, Row> input, Context context) throws Exception {
		Row row = input.f1;
		if (null == row) {
			return;
		}
		if (row.getArity() != totalQualifiers + 1) {
			LOG.warn("discard invalid row:{}", row);
		} else {
			byte[] rowkey = inputFieldSerializers.get(rowKeySourceIndex).toHBaseBytes(row.getField(rowKeySourceIndex));
			if (input.f0) {
				// upsert
				Put put = new Put(rowkey);
				for (int index = 0; index <= totalQualifiers; index++) {
					if (index != rowKeySourceIndex) {
						int qualifierSrcIndex = index;
						if (index > rowKeySourceIndex) {
							qualifierSrcIndex = index - 1;
						}
						Tuple3<byte[], byte[], TypeInformation<?>> qualifierInfo = qualifierList.get(qualifierSrcIndex);
						int qualifierIndex = qualifierSourceIndexes.get(qualifierSrcIndex);
						byte[] value = inputFieldSerializers.get(index).toHBaseBytes(row.getField(qualifierIndex));
						put.addColumn(qualifierInfo.f0, qualifierInfo.f1, value);
					}
				}
				table.put(put);
			} else {
				// delete
				Delete delete = new Delete(rowkey);
				for (int index = 0; index <= totalQualifiers; index++) {
					if (index != rowKeySourceIndex) {
						int qualifierSrcIndex = index;
						if (index > rowKeySourceIndex) {
							qualifierSrcIndex = index - 1;
						}
						Tuple3<byte[], byte[], TypeInformation<?>> typeInfo = qualifierList.get(qualifierSrcIndex);
						delete.addColumn(typeInfo.f0, typeInfo.f1);
					}
				}
				table.delete(delete);
			}
		}
	}

	@Override
	public String toString() {
		return HBase143Writer.class.getSimpleName() + "-> table:" + hTableName + " schema:{" + hTableSchema.toString() + "}";
	}
}
