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

package org.apache.flink.table.sinks;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.operators.collect.CollectResultIterator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperatorFactory;
import org.apache.flink.streaming.api.operators.collect.CollectStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.internal.SelectResultProvider;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;

import java.util.UUID;

/**
 * A {@link RetractStreamTableSink} for streaming select job to collect the result to local.
 */
public class StreamSelectTableSink implements RetractStreamTableSink<Row> {

	private final TableSchema tableSchema;
	private final CollectSinkOperatorFactory<Tuple2<Boolean, Row>> factory;
	private final CollectResultIterator<Tuple2<Boolean, Row>> iterator;

	public StreamSelectTableSink(TableSchema tableSchema) {
		this.tableSchema = SelectTableSinkSchemaConverter.convertTimeAttributeToRegularTimestamp(tableSchema);

		TypeInformation<Tuple2<Boolean, Row>> tupleTypeInfo =
				new TupleTypeInfo<>(Types.BOOLEAN, this.tableSchema.toRowType());
		TypeSerializer<Tuple2<Boolean, Row>> typeSerializer = tupleTypeInfo.createSerializer(new ExecutionConfig());
		String accumulatorName = "tableResultCollect_" + UUID.randomUUID();

		this.factory = new CollectSinkOperatorFactory<>(typeSerializer, accumulatorName);
		CollectSinkOperator<Row> operator = (CollectSinkOperator<Row>) factory.getOperator();
		this.iterator = new CollectResultIterator<>(operator.getOperatorIdFuture(), typeSerializer, accumulatorName);
	}

	@Override
	public TypeInformation<Row> getRecordType() {
		return tableSchema.toRowType();
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	@Override
	public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		throw new UnsupportedOperationException();
	}

	@Override
	public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
		CollectStreamSink<?> sink = new CollectStreamSink<>(dataStream, factory);
		dataStream.getExecutionEnvironment().addOperator(sink.getTransformation());
		return sink.name("Streaming select table sink");
	}

	public SelectResultProvider getSelectResultProvider() {
		return new SelectResultProvider() {

			@Override
			public void setJobClient(JobClient jobClient) {
				iterator.setJobClient(jobClient);
			}

			@Override
			public CloseableIterator<Row> getResultIterator() {
				return new RowIteratorWrapper(iterator);
			}
		};
	}

	/**
	 * An Iterator wrapper class that converts Iterator&lt;Tuple2&lt;Boolean, Row&gt;&gt; to Iterator&lt;Row&gt;.
	 */
	private static class RowIteratorWrapper implements CloseableIterator<Row> {
		private final CollectResultIterator<Tuple2<Boolean, Row>> iterator;
		public RowIteratorWrapper(CollectResultIterator<Tuple2<Boolean, Row>> iterator) {
			this.iterator = iterator;
		}

		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		public Row next() {
			// convert Tuple2<Boolean, Row> to Row
			Tuple2<Boolean, Row> tuple2 = iterator.next();
			RowKind rowKind = tuple2.f0 ? RowKind.INSERT : RowKind.DELETE;
			Row row = tuple2.f1;
			row.setKind(rowKind);
			return row;
		}

		@Override
		public void close() throws Exception {
			iterator.close();
		}
	}
}
