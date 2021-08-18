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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.connector.sink.DynamicTableSink;

/**
 * Defines an external {@link TableSink} to emit a bounded {@link Table}.
 *
 * @param <T> Type of the bounded {@link OutputFormat} that this {@link TableSink} expects and
 *     supports.
 * @deprecated This interface has been replaced by {@link DynamicTableSink}. The new interface
 *     consumes internal data structures. See FLIP-95 for more information.
 */
@Deprecated
@Experimental
public abstract class OutputFormatTableSink<T> implements StreamTableSink<T> {

    /** Returns an {@link OutputFormat} for writing the data of the table. */
    public abstract OutputFormat<T> getOutputFormat();

    @Override
    public final DataStreamSink<T> consumeDataStream(DataStream<T> dataStream) {
        return dataStream
                .writeUsingOutputFormat(getOutputFormat())
                .setParallelism(dataStream.getParallelism());
    }
}
