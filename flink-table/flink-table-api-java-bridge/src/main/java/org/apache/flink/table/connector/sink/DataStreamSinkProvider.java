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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.connector.ParallelismProvider;
import org.apache.flink.table.data.RowData;

import java.util.Optional;

/**
 * Provider that consumes a Java {@link DataStream} as a runtime implementation for {@link
 * DynamicTableSink}.
 *
 * <p>Note: This provider is only meant for advanced connector developers. Usually, a sink should
 * consist of a single entity expressed via {@link SinkProvider}, {@link SinkFunctionProvider}, or
 * {@link OutputFormatProvider}. When using a {@link DataStream} an implementer needs to pay
 * attention to how changes are shuffled to not mess up the changelog per parallel subtask.
 */
@PublicEvolving
public interface DataStreamSinkProvider
        extends DynamicTableSink.SinkRuntimeProvider, ParallelismProvider {

    /**
     * Consumes the given Java {@link DataStream} and returns the sink transformation {@link
     * DataStreamSink}.
     */
    DataStreamSink<?> consumeDataStream(DataStream<RowData> dataStream);

    /**
     * {@inheritDoc}
     *
     * <p>Note: If a custom parallelism is returned and {@link #consumeDataStream(DataStream)}
     * applies multiple transformations, make sure to set the same custom parallelism to each
     * operator to not mess up the changelog.
     */
    @Override
    default Optional<Integer> getParallelism() {
        return Optional.empty();
    }
}
