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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

/**
 * Provider that produces a Java {@link DataStream} as a runtime implementation for {@link
 * ScanTableSource}.
 *
 * <p>Note: This provider is only meant for advanced connector developers. Usually, a source should
 * consist of a single entity expressed via {@link InputFormatProvider}, {@link
 * SourceFunctionProvider}, or {@link SourceProvider}.
 */
@PublicEvolving
public interface DataStreamScanProvider extends ScanTableSource.ScanRuntimeProvider {

    /** Creates a scan Java {@link DataStream} from a {@link StreamExecutionEnvironment}. */
    DataStream<RowData> produceDataStream(StreamExecutionEnvironment execEnv);
}
