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

package org.apache.flink.table.connector.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.data.RowData;

/**
 * Provider that produces a Java {@link DataStream} as a runtime implementation for {@link
 * ScanTableSource}.
 *
 * <p>Note: This provider is only meant for advanced connector developers. Usually, a source should
 * consist of a single entity expressed via {@link SourceProvider}, {@link SourceFunctionProvider},
 * or {@link InputFormatProvider}.
 */
@PublicEvolving
public interface DataStreamScanProvider extends ScanTableSource.ScanRuntimeProvider {

    /**
     * Creates a scan Java {@link DataStream} from a {@link StreamExecutionEnvironment}.
     *
     * <p>Note: If the {@link CompiledPlan} feature should be supported, this method MUST set a
     * unique identifier for each transformation/operator in the data stream. This enables stateful
     * Flink version upgrades for streaming jobs. The identifier is used to map state back from a
     * savepoint to an actual operator in the topology. The framework can generate topology-wide
     * unique identifiers with {@link ProviderContext#generateUid(String)}.
     *
     * @see SingleOutputStreamOperator#uid(String)
     */
    default DataStream<RowData> produceDataStream(
            ProviderContext providerContext, StreamExecutionEnvironment execEnv) {
        return produceDataStream(execEnv);
    }

    /** Creates a scan Java {@link DataStream} from a {@link StreamExecutionEnvironment}. */
    @Deprecated
    default DataStream<RowData> produceDataStream(StreamExecutionEnvironment execEnv) {
        throw new UnsupportedOperationException(
                "This method is deprecated. "
                        + "Use produceDataStream(ProviderContext, StreamExecutionEnvironment) instead");
    }
}
