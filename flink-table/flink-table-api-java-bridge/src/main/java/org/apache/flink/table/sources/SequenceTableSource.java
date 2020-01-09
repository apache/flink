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

package org.apache.flink.table.sources;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.StatefulSequenceSource;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;

import static org.apache.flink.table.sources.SequenceTableSourceFactory.ID_FIELD_NAME;

/**
 * A {@link StreamTableSource} that emits each number from a given interval exactly once,
 * possibly in parallel. See {@link StatefulSequenceSource}.
 */
public class SequenceTableSource implements StreamTableSource<Long> {

	private final long start;
	private final long end;
	private final long rowsPerSecond;

	public SequenceTableSource(long start, long end, long rowsPerSecond) {
		this.start = start;
		this.end = end;
		this.rowsPerSecond = rowsPerSecond;
	}

	@Override
	public DataStream<Long> getDataStream(StreamExecutionEnvironment execEnv) {
		return execEnv.addSource(new StatefulSequenceSource(start, end, rowsPerSecond));
	}

	@Override
	public TableSchema getTableSchema() {
		return TableSchema.builder().field(ID_FIELD_NAME, DataTypes.BIGINT()).build();
	}

	@Override
	public DataType getProducedDataType() {
		return DataTypes.BIGINT();
	}
}
