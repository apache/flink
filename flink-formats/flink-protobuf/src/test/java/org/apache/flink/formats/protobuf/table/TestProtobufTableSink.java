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

package org.apache.flink.formats.protobuf.table;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

/** Table sink for protobuf table factory test. */
public class TestProtobufTableSink implements DynamicTableSink {
    private final EncodingFormat<SerializationSchema<RowData>> encodingFormat;
    private final DataType dataType;

    public TestProtobufTableSink(
            EncodingFormat<SerializationSchema<RowData>> encodingFormat, DataType dataType) {
        this.encodingFormat = encodingFormat;
        this.dataType = dataType;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final SerializationSchema<RowData> serializer =
                encodingFormat.createRuntimeEncoder(context, dataType);
        return SinkFunctionProvider.of(new TestProtobufSinkFunction(serializer));
    }

    @Override
    public DynamicTableSink copy() {
        return new TestProtobufTableSink(encodingFormat, dataType);
    }

    @Override
    public String asSummaryString() {
        return TestProtobufTableSink.class.getName();
    }
}
