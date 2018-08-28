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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.lang.reflect.Field;

/**
 * Flink Sink to save data into a HBase cluster.
 * @param <IN> Type of the element emitted by this sink
 */
public class HBasePojoSink<IN> extends HBaseSinkBase<IN> {

	private static final long serialVersionUID = 1L;

	private final HBaseTableMapper tableMapper;
	private final String[] fieldNameList;
	private final TypeInformation<IN> typeInfo;
	private transient Field[] fields;
	private transient Field rowKeyField;

	/**
	 * The main constructor for creating HBasePojoSink.
	 *
	 * @param builder A builder for build HBase connection and handle for communicating with a single HBase table.
	 * @param tableMapper The mapping from a Pojo to a HBase table
	 * @param typeInfo TypeInformation of the Pojo
	 */
	public HBasePojoSink(HBaseTableBuilder builder, HBaseTableMapper tableMapper, TypeInformation<IN> typeInfo) {
		super(builder);
		this.tableMapper = tableMapper;
		this.fieldNameList = tableMapper.getKeyList();
		this.typeInfo = typeInfo;
	}

	@VisibleForTesting
	public HBasePojoSink(Table hTable, HBaseTableMapper tableMapper, TypeInformation<IN> typeInfo) {
		super(hTable);
		this.tableMapper = tableMapper;
		this.fieldNameList = tableMapper.getKeyList();
		this.typeInfo = typeInfo;
	}

	@Override
	protected Object extract(IN value) throws Exception {
		byte[] rowKey = HBaseTableMapper.serialize(tableMapper.getRowKeyType(), rowKeyField.get(value));
		Put put = new Put(rowKey);
		for (int i = 0; i < fieldNameList.length; i++) {
			Tuple3<byte[], byte[], TypeInformation<?>> colInfo = tableMapper.getColInfo(fieldNameList[i]);
			put.addColumn(colInfo.f0, colInfo.f1,
				HBaseTableMapper.serialize(colInfo.f2, fields[i].get(value)));
		}
		return put;
	}

	@Override
	public void open(Configuration configuration) throws Exception {
		super.open(configuration);
		Class<IN> clazz = typeInfo.getTypeClass();
		fields = new Field[fieldNameList.length];
		for (int i = 0; i < fields.length; i++) {
			fields[i] = clazz.getDeclaredField(fieldNameList[i]);
			fields[i].setAccessible(true);
		}
		rowKeyField = clazz.getDeclaredField(tableMapper.getRowKey());
		rowKeyField.setAccessible(true);
	}
}
