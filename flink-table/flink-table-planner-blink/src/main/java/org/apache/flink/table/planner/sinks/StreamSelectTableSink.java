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

package org.apache.flink.table.planner.sinks;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.experimental.CollectSink;
import org.apache.flink.streaming.experimental.SocketStreamIterator;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.internal.SelectTableSink;
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Iterator;

/**
 * A {@link SelectTableSink} for streaming select job.
 *
 * <p><strong>NOTES:</strong> This is a temporary solution,
 * once FLINK-14807 is finished, the implementation should be changed.
 * Currently, only insert changes (AppendStreamTableSink) is supported.
 * Once FLINK-16998 is finished, all kinds of changes will be supported.
 */
public class StreamSelectTableSink implements AppendStreamTableSink<Row>, SelectTableSink {
	private final TableSchema tableSchema;
	private final TypeSerializer<Row> typeSerializer;
	private final SocketStreamIterator<Row> iterator;

	@SuppressWarnings("unchecked")
	public StreamSelectTableSink(TableSchema tableSchema) {
		this.tableSchema = SelectTableSinkSchemaConverter.convertTimeAttributeToRegularTimestamp(
				SelectTableSinkSchemaConverter.changeDefaultConversionClass(tableSchema));
		this.typeSerializer = (TypeSerializer<Row>) TypeInfoDataTypeConverter
				.fromDataTypeToTypeInfo(this.tableSchema.toRowDataType())
				.createSerializer(new ExecutionConfig());
		try {
			// socket server should be started before running the job
			iterator = new SocketStreamIterator<>(0, InetAddress.getLocalHost(), typeSerializer);
		} catch (IOException e) {
			throw new TableException("Failed to get the address of the local host.");
		}
	}

	@Override
	public DataType getConsumedDataType() {
		return tableSchema.toRowDataType();
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	@Override
	public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
		return dataStream
				.addSink(new CollectSink<>(iterator.getBindAddress(), iterator.getPort(), typeSerializer))
				.name("Streaming select table sink")
				.setParallelism(1);
	}

	@Override
	public void setJobClient(JobClient jobClient) {
	}

	@Override
	public Iterator<Row> getResultIterator() {
		return iterator;
	}
}

