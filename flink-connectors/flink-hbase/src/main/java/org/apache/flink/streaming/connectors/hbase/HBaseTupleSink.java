/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.hbase;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

/**
 * Sink to write tuple-like values into a HBase cluster.
 *
 * @param <IN> Type of the elements emitted by this sink, it must extend {@link Tuple}
 */
public class HBaseTupleSink<IN extends Tuple> extends HBaseSinkBase<IN> {

	private final HBaseTableMapper tableMapper;
	private final String[] indexList;

	public HBaseTupleSink(HBaseTableBuilder builder, HBaseTableMapper tableMapper) {
		super(builder);
		this.tableMapper = tableMapper;
		this.indexList = tableMapper.getKeyList();
	}

	@VisibleForTesting
	public HBaseTupleSink(Table hTable, HBaseTableMapper tableMapper) {
		super(hTable);
		this.tableMapper = tableMapper;
		this.indexList = tableMapper.getKeyList();
	}

	@Override protected Object extract(IN value) throws Exception {
		int rowKeyIndex = Integer.parseInt(tableMapper.getRowKey());
		byte[] rowKey = HBaseTableMapper.serialize(tableMapper.getRowKeyType(), value.getField(rowKeyIndex));
		Put put = new Put(rowKey);
		for (String index : indexList) {
			Tuple3<byte[], byte[], TypeInformation<?>> colInfo = tableMapper.getColInfo(index);
			put.addColumn(colInfo.f0, colInfo.f1,
				HBaseTableMapper.serialize(colInfo.f2, value.getField(Integer.parseInt(index))));
		}
		return put;
	}
}
