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

package org.apache.flink.table.api;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** Tests for {@link TableDescriptor}. */
public class TableDescriptorTest {

    private static final ConfigOption<Boolean> OPTION_A =
            ConfigOptions.key("a").booleanType().noDefaultValue();

    private static final ConfigOption<Integer> OPTION_B =
            ConfigOptions.key("b").intType().noDefaultValue();

    private static final ConfigOption<String> KEY_FORMAT =
            ConfigOptions.key("key.format").stringType().noDefaultValue();

    @Test
    public void testBasic() {
        final Schema schema =
                Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .column("f1", DataTypes.BIGINT())
                        .primaryKey("f0")
                        .build();

        final TableDescriptor descriptor =
                TableDescriptor.forConnector("test-connector")
                        .schema(schema)
                        .partitionedBy("f0")
                        .comment("Test Comment")
                        .build();

        assertTrue(descriptor.getSchema().isPresent());
        assertEquals(schema, descriptor.getSchema().get());

        assertEquals(1, descriptor.getPartitionKeys().size());
        assertEquals("f0", descriptor.getPartitionKeys().get(0));

        assertEquals(1, descriptor.getOptions().size());
        assertEquals("test-connector", descriptor.getOptions().get("connector"));

        assertEquals("Test Comment", descriptor.getComment().orElse(null));
    }

    @Test
    public void testNoSchema() {
        final TableDescriptor descriptor = TableDescriptor.forConnector("test-connector").build();
        assertFalse(descriptor.getSchema().isPresent());
    }

    @Test
    public void testOptions() {
        final TableDescriptor descriptor =
                TableDescriptor.forConnector("test-connector")
                        .schema(Schema.newBuilder().build())
                        .option(OPTION_A, false)
                        .option(OPTION_B, 42)
                        .option("c", "C")
                        .build();

        assertEquals(4, descriptor.getOptions().size());
        assertEquals("test-connector", descriptor.getOptions().get("connector"));
        assertEquals("false", descriptor.getOptions().get("a"));
        assertEquals("42", descriptor.getOptions().get("b"));
        assertEquals("C", descriptor.getOptions().get("c"));
    }

    @Test
    public void testFormatBasic() {
        final TableDescriptor descriptor =
                TableDescriptor.forConnector("test-connector")
                        .schema(Schema.newBuilder().build())
                        .format("json")
                        .build();

        assertEquals(2, descriptor.getOptions().size());
        assertEquals("test-connector", descriptor.getOptions().get("connector"));
        assertEquals("json", descriptor.getOptions().get("format"));
    }

    @Test
    public void testFormatWithFormatDescriptor() {
        final TableDescriptor descriptor =
                TableDescriptor.forConnector("test-connector")
                        .schema(Schema.newBuilder().build())
                        .format(
                                KEY_FORMAT,
                                FormatDescriptor.forFormat("test-format")
                                        .option(OPTION_A, true)
                                        .option(OPTION_B, 42)
                                        .option("c", "C")
                                        .build())
                        .build();

        assertEquals(5, descriptor.getOptions().size());
        assertEquals("test-connector", descriptor.getOptions().get("connector"));
        assertEquals("test-format", descriptor.getOptions().get("key.format"));
        assertEquals("true", descriptor.getOptions().get("key.test-format.a"));
        assertEquals("42", descriptor.getOptions().get("key.test-format.b"));
        assertEquals("C", descriptor.getOptions().get("key.test-format.c"));
    }

    @Test
    public void testToString() {
        final Schema schema = Schema.newBuilder().column("f0", DataTypes.STRING()).build();

        final FormatDescriptor formatDescriptor =
                FormatDescriptor.forFormat("test-format").option(OPTION_A, false).build();

        final TableDescriptor tableDescriptor =
                TableDescriptor.forConnector("test-connector")
                        .schema(schema)
                        .partitionedBy("f0")
                        .option(OPTION_A, true)
                        .format(formatDescriptor)
                        .comment("Test Comment")
                        .build();

        assertEquals("test-format[{a=false}]", formatDescriptor.toString());
        assertEquals(
                "(\n"
                        + "  `f0` STRING\n"
                        + ")\n"
                        + "COMMENT 'Test Comment'\n"
                        + "PARTITIONED BY (`f0`)\n"
                        + "WITH (\n"
                        + "  'a' = 'true',\n"
                        + "  'connector' = 'test-connector',\n"
                        + "  'test-format.a' = 'false',\n"
                        + "  'format' = 'test-format'\n"
                        + ")",
                tableDescriptor.toString());
    }

    @Test
    public void testFormatDescriptorWithPrefix() {
        assertThrows(
                "Format options set using #format(FormatDescriptor) should not contain the prefix 'test-format.', but found 'test-format.a'.",
                ValidationException.class,
                () -> {
                    TableDescriptor.forConnector("test-connector")
                            .schema(Schema.newBuilder().build())
                            .format(
                                    FormatDescriptor.forFormat("test-format")
                                            .option("test-format.a", "A")
                                            .build())
                            .build();
                });
    }
}
