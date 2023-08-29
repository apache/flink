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

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;

/** Sink Function for protobuf table factory test. Must run in single parallelism. */
public class TestProtobufSinkFunction extends RichSinkFunction<RowData> {
    private final SerializationSchema<RowData> serializer;

    public TestProtobufSinkFunction(SerializationSchema<RowData> serializer) {
        this.serializer = serializer;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        this.serializer.open(null);
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {
        byte[] bytes = serializer.serialize(value);
        TestProtobufTestStore.sinkResults.add(bytes);
    }
}
