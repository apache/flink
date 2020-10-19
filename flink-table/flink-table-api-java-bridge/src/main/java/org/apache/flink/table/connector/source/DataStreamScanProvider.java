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
 * Provider to create a {@link DataStream} instance as a runtime implementation for {@link ScanTableSource}.
 *
 * <p>NOTE: This is only for advanced connector developers.
 */
@PublicEvolving
public interface DataStreamScanProvider extends ScanTableSource.ScanRuntimeProvider {

  /**
   * Creates a scan {@link DataStream} from {@link StreamExecutionEnvironment}.
   */
  DataStream<RowData> createDataStream(StreamExecutionEnvironment execEnv);
}
