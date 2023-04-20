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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TableDescriptor}. */
class TableDescriptorTest {

    private static final ConfigOption<Boolean> OPTION_A =
            ConfigOptions.key("a").booleanType().noDefaultValue();

    private static final ConfigOption<Integer> OPTION_B =
            ConfigOptions.key("b").intType().noDefaultValue();

    private static final ConfigOption<String> KEY_FORMAT =
            ConfigOptions.key("key.format").stringType().noDefaultValue();

    @Test
    void testBasic() {
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

        assertThat(descriptor.getSchema()).isPresent();
        assertThat(descriptor.getSchema().get()).isEqualTo(schema);

        assertThat(descriptor.getPartitionKeys()).hasSize(1);
        assertThat(descriptor.getPartitionKeys().get(0)).isEqualTo("f0");

        assertThat(descriptor.getOptions()).hasSize(1);
        assertThat(descriptor.getOptions().get("connector")).isEqualTo("test-connector");

        assertThat(descriptor.getComment().orElse(null)).isEqualTo("Test Comment");
    }

    @Test
    void testNoSchema() {
        final TableDescriptor descriptor = TableDescriptor.forConnector("test-connector").build();
        assertThat(descriptor.getSchema()).isNotPresent();
    }

    @Test
    void testOptions() {
        final TableDescriptor descriptor =
                TableDescriptor.forConnector("test-connector")
                        .schema(Schema.newBuilder().build())
                        .option(OPTION_A, false)
                        .option(OPTION_B, 42)
                        .option("c", "C")
                        .build();

        assertThat(descriptor.getOptions()).hasSize(4);
        assertThat(descriptor.getOptions().get("connector")).isEqualTo("test-connector");
        assertThat(descriptor.getOptions().get("a")).isEqualTo("false");
        assertThat(descriptor.getOptions().get("b")).isEqualTo("42");
        assertThat(descriptor.getOptions().get("c")).isEqualTo("C");
    }

    @Test
    void testFormatBasic() {
        final TableDescriptor descriptor =
                TableDescriptor.forConnector("test-connector")
                        .schema(Schema.newBuilder().build())
                        .format("json")
                        .build();

        assertThat(descriptor.getOptions()).hasSize(2);
        assertThat(descriptor.getOptions().get("connector")).isEqualTo("test-connector");
        assertThat(descriptor.getOptions().get("format")).isEqualTo("json");
    }

    @Test
    void testFormatWithFormatDescriptor() {
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

        assertThat(descriptor.getOptions()).hasSize(5);
        assertThat(descriptor.getOptions().get("connector")).isEqualTo("test-connector");
        assertThat(descriptor.getOptions().get("key.format")).isEqualTo("test-format");
        assertThat(descriptor.getOptions().get("key.test-format.a")).isEqualTo("true");
        assertThat(descriptor.getOptions().get("key.test-format.b")).isEqualTo("42");
        assertThat(descriptor.getOptions().get("key.test-format.c")).isEqualTo("C");
    }

    @Test
    void testToString() {
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

        assertThat(formatDescriptor.toString()).isEqualTo("test-format[{a=false}]");
        assertThat(tableDescriptor.toString())
                .isEqualTo(
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
                                + ")");
    }

    @Test
    void testFormatDescriptorWithPrefix() {
        assertThatThrownBy(
                        () -> {
                            TableDescriptor.forConnector("test-connector")
                                    .schema(Schema.newBuilder().build())
                                    .format(
                                            FormatDescriptor.forFormat("test-format")
                                                    .option("test-format.a", "A")
                                                    .build())
                                    .build();
                        })
                .as(
                        "Format options set using #format(FormatDescriptor) should not contain the prefix 'test-format.', but found 'test-format.a'.")
                .isInstanceOf(ValidationException.class);
    }
}
