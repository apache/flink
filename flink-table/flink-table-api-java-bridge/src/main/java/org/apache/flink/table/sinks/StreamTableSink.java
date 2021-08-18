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

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.connector.sink.DynamicTableSink;

/**
 * Defines an external stream table and provides write access to its data.
 *
 * @param <T> Type of the {@link DataStream} created by this {@link TableSink}.
 * @deprecated This interface has been replaced by {@link DynamicTableSink}. The new interface
 *     consumes internal data structures. See FLIP-95 for more information.
 */
@Deprecated
public interface StreamTableSink<T> extends TableSink<T> {

    /**
     * Consumes the DataStream and return the sink transformation {@link DataStreamSink}. The
     * returned {@link DataStreamSink} will be used to set resources for the sink operator.
     */
    DataStreamSink<?> consumeDataStream(DataStream<T> dataStream);
}
