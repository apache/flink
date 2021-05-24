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

package org.apache.flink.table.factories;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.util.TernaryBoolean;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.descriptors.OldCsvValidator.FORMAT_FIELDS;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for CsvTableSourceFactory and CsvTableSinkFactory. */
@RunWith(Parameterized.class)
public class CsvTableSinkFactoryTest {

    private static TableSchema testingSchema =
            TableSchema.builder()
                    .field("myfield", DataTypes.STRING())
                    .field("myfield2", DataTypes.INT())
                    .field("myfield3", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                    .field(
                            "myfield4",
                            DataTypes.ROW(DataTypes.FIELD("nested_f1", DataTypes.BIGINT())))
                    // TODO: we can't declare the TINYINT as NOT NULL, because CSV connector will
                    // ignore the information
                    .field("myfield5", DataTypes.ARRAY(DataTypes.TINYINT()))
                    .build();

    @Parameterized.Parameter public TernaryBoolean deriveSchema;

    @Parameterized.Parameters(name = "deriveSchema = {0}")
    public static TernaryBoolean[] getDeriveSchema() {
        return new TernaryBoolean[] {
            TernaryBoolean.TRUE, TernaryBoolean.FALSE, TernaryBoolean.UNDEFINED
        };
    }

    @Test
    public void testAppendTableSinkFactory() {
        DescriptorProperties descriptor = createDescriptor(testingSchema);
        descriptor.putString("update-mode", "append");
        TableSink sink = createTableSink(descriptor);

        assertTrue(sink instanceof CsvTableSink);
        assertEquals(testingSchema.toRowDataType(), sink.getConsumedDataType());
    }

    @Test
    public void testBatchTableSinkFactory() {
        DescriptorProperties descriptor = createDescriptor(testingSchema);
        TableSink sink = createTableSink(descriptor);

        assertTrue(sink instanceof CsvTableSink);
        assertEquals(testingSchema.toRowDataType(), sink.getConsumedDataType());
    }

    @Test
    public void testAppendTableSourceFactory() {
        DescriptorProperties descriptor = createDescriptor(testingSchema);
        descriptor.putString("update-mode", "append");
        TableSource sink = createTableSource(descriptor);

        assertTrue(sink instanceof CsvTableSource);
        assertEquals(testingSchema.toRowDataType(), sink.getProducedDataType());
    }

    @Test
    public void testBatchTableSourceFactory() {
        DescriptorProperties descriptor = createDescriptor(testingSchema);
        TableSource sink = createTableSource(descriptor);

        assertTrue(sink instanceof CsvTableSource);
        assertEquals(testingSchema.toRowDataType(), sink.getProducedDataType());
    }

    private DescriptorProperties createDescriptor(TableSchema schema) {
        Map<String, String> properties = new HashMap<>();
        properties.put("connector.type", "filesystem");
        properties.put("connector.property-version", "1");
        properties.put("connector.path", "/path/to/csv");

        // schema
        properties.put("format.type", "csv");
        properties.put("format.property-version", "1");
        properties.put("format.field-delimiter", ";");

        DescriptorProperties descriptor = new DescriptorProperties(true);
        descriptor.putProperties(properties);
        descriptor.putTableSchema(SCHEMA, schema);
        if (deriveSchema == TernaryBoolean.TRUE) {
            descriptor.putBoolean("format.derive-schema", true);
        } else if (deriveSchema == TernaryBoolean.FALSE) {
            descriptor.putTableSchema(FORMAT_FIELDS, testingSchema);
        } // nothing to put for UNDEFINED
        return descriptor;
    }

    private static TableSource<?> createTableSource(DescriptorProperties descriptor) {
        return TableFactoryService.find(TableSourceFactory.class, descriptor.asMap())
                .createTableSource(descriptor.asMap());
    }

    private static TableSink<?> createTableSink(DescriptorProperties descriptor) {
        return TableFactoryService.find(TableSinkFactory.class, descriptor.asMap())
                .createTableSink(descriptor.asMap());
    }
}
