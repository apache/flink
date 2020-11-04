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

package org.apache.flink.table.connector.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.connector.ParallelismProvider;
import org.apache.flink.table.data.RowData;

import java.util.Optional;

/**
 * Provider of a {@link SinkFunction} instance as a runtime implementation for {@link DynamicTableSink}.
 */
@PublicEvolving
public interface SinkFunctionProvider extends DynamicTableSink.SinkRuntimeProvider, ParallelismProvider {

	/**
	 * Helper method for creating a static provider.
	 */
	static SinkFunctionProvider of(SinkFunction<RowData> sinkFunction) {
		return () -> sinkFunction;
	}

	/**
	 * Helper method for creating a SinkFunction provider with a provided sink parallelism.
	 */
	static SinkFunctionProvider of(SinkFunction<RowData> sinkFunction, Integer sinkParallelism) {
		return new SinkFunctionProvider() {

			@Override
			public SinkFunction<RowData> createSinkFunction() {
				return sinkFunction;
			}

			@Override
			public Optional<Integer> getParallelism() {
				return Optional.ofNullable(sinkParallelism);
			}
		};
	}

	/**
	 * Creates a {@link SinkFunction} instance.
	 */
	SinkFunction<RowData> createSinkFunction();
}
