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
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HBaseSinkBase is the common abstract class of {@link HBasePojoSink}, {@link HBaseTupleSink}, {@link HBaseRowSink} and
 * {@link HBaseScalaProductSink}.
 *
 * @param <IN> Type of the elements emitted by this sink
 */
public abstract class HBaseSinkBase<IN> extends RichSinkFunction<IN> implements CheckpointedFunction {
	protected final Logger log = LoggerFactory.getLogger(getClass());

	protected Connection connection;
	protected transient Table hTable;

	private final HBaseTableBuilder builder;

	public HBaseSinkBase(HBaseTableBuilder builder) {
		this.builder = builder;
		ClosureCleaner.clean(builder, true);
	}

	@VisibleForTesting
	public HBaseSinkBase(Table hTable){
		this.builder = null;
		this.connection = null;
		this.hTable = hTable;
	}

	@Override
	public void open(Configuration configuration) throws Exception {
		super.open(configuration);
		if (builder != null) {
			this.connection = builder.buildConnection();
		}
		if (connection != null) {
			this.hTable = builder.buildTable(connection);
		}
	}

	@Override
	public void invoke(IN value, Context context) throws Exception {
		Object obj = extract(value);
		if (obj == null) {
			return;
		} else if (obj instanceof Put) {
			this.hTable.put((Put) obj);
		} else if (obj instanceof Delete) {
			this.hTable.delete((Delete) obj);
		}
	}

	protected abstract Object extract(IN value) throws Exception;

	@Override public void close() throws Exception {
		super.close();
		try {
			if (this.hTable != null) {
				this.hTable.close();
			}
		} catch (Throwable t) {
			log.error("Error while closing HBase table.", t);
		}
		try {
			if (this.connection != null) {
				this.connection.close();
			}
		} catch (Throwable t) {
			log.error("Error while closing HBase connection.", t);
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		if (this.hTable != null && this.hTable instanceof HTable) {
			((HTable) this.hTable).flushCommits();
		}
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception { }

	protected byte[] getByteArray(Object obj, Class clazz) {
		if (obj == null) {
			return new byte[0];
		}
		if (clazz == Integer.class) {
			return Bytes.toBytes((Integer) obj);
		} else if (clazz == Long.class) {
			return Bytes.toBytes((Long) obj);
		} else if (clazz == String.class) {
			return Bytes.toBytes((String) obj);
		} else if (clazz == Byte.class) {
			return Bytes.toBytes((Byte) obj);
		} else if (clazz == Short.class) {
			return Bytes.toBytes((Short) obj);
		} else if (clazz == Float.class) {
			return Bytes.toBytes((Float) obj);
		} else if (clazz == Double.class) {
			return Bytes.toBytes((Double) obj);
		} else if (clazz == Character.class) {
			return Bytes.toBytes((Character) obj);
		} else if (clazz == Void.class) {
			return new byte[0];
		} else {
			return Bytes.toBytes(obj.toString());
		}
	}
}
