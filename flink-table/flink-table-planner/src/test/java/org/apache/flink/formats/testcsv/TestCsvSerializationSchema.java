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

package org.apache.flink.formats.testcsv;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;

import java.nio.charset.StandardCharsets;

import static org.apache.flink.api.java.io.CsvOutputFormat.DEFAULT_FIELD_DELIMITER;

/** @see TestCsvFormatFactory */
class TestCsvSerializationSchema implements SerializationSchema<RowData> {

    private final DynamicTableSink.DataStructureConverter converter;

    public TestCsvSerializationSchema(DynamicTableSink.DataStructureConverter converter) {
        this.converter = converter;
    }

    @Override
    public void open(InitializationContext context) throws Exception {}

    @Override
    public byte[] serialize(RowData element) {
        Row row = (Row) converter.toExternal(element);
        StringBuilder builder = new StringBuilder();
        Object o;
        for (int i = 0; i < row.getArity(); i++) {
            if (i > 0) {
                builder.append(DEFAULT_FIELD_DELIMITER);
            }
            if ((o = row.getField(i)) != null) {
                builder.append(o);
            }
        }
        String str = builder.toString();
        return str.getBytes(StandardCharsets.UTF_8);
    }
}
