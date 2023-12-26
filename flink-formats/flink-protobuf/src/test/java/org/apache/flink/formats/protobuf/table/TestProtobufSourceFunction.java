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
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;

/** Source Function for protobuf table factory test. */
public class TestProtobufSourceFunction extends RichSourceFunction<RowData> {
    private final DeserializationSchema<RowData> deserializer;

    public TestProtobufSourceFunction(DeserializationSchema<RowData> deserializer) {
        this.deserializer = deserializer;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        this.deserializer.open(null);
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        for (byte[] bytes : TestProtobufTestStore.sourcePbInputs) {
            RowData rowData = deserializer.deserialize(bytes);
            if (rowData != null) {
                ctx.collect(rowData);
            }
        }
    }

    @Override
    public void cancel() {}
}
