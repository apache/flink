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

package org.apache.flink.connector.file.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.SINK_PARTITION_COMMIT_WATERMARK_TIME_ZONE;
import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FileSystemTableFactory}. */
public class FileSystemTableFactoryTest {

    private static final ResolvedSchema SCHEMA =
            ResolvedSchema.of(
                    Column.physical("f0", DataTypes.STRING()),
                    Column.physical("f1", DataTypes.BIGINT()),
                    Column.physical("f2", DataTypes.BIGINT()));

    @Test
    public void testSourceSink() {
        DescriptorProperties descriptor = new DescriptorProperties();
        descriptor.putString(FactoryUtil.CONNECTOR.key(), "filesystem");
        descriptor.putString("path", "/tmp");
        descriptor.putString("format", "testcsv");

        // test ignore format options
        descriptor.putString("testcsv.my_option", "my_value");

        DynamicTableSource source = createTableSource(SCHEMA, descriptor.asMap());
        assertThat(source).isInstanceOf(FileSystemTableSource.class);

        DynamicTableSink sink = createTableSink(SCHEMA, descriptor.asMap());
        assertThat(sink).isInstanceOf(FileSystemTableSink.class);
    }

    @Test
    public void testLackOptionSource() {
        DescriptorProperties descriptor = new DescriptorProperties();
        descriptor.putString(FactoryUtil.CONNECTOR.key(), "filesystem");
        descriptor.putString("path", "/tmp");

        assertThatThrownBy(() -> createTableSource(SCHEMA, descriptor.asMap()))
                .satisfies(
                        anyCauseMatches(
                                ValidationException.class,
                                "Missing required options are:\n\nformat"));
    }

    @Test
    public void testLackOptionSink() {
        DescriptorProperties descriptor = new DescriptorProperties();
        descriptor.putString(FactoryUtil.CONNECTOR.key(), "filesystem");
        descriptor.putString("path", "/tmp");

        assertThatThrownBy(() -> createTableSink(SCHEMA, descriptor.asMap()))
                .satisfies(
                        anyCauseMatches(
                                ValidationException.class,
                                "Missing required options are:\n\nformat"));
    }

    @Test
    public void testUnsupportedOptionSource() {
        DescriptorProperties descriptor = new DescriptorProperties();
        descriptor.putString(FactoryUtil.CONNECTOR.key(), "filesystem");
        descriptor.putString("path", "/tmp");
        descriptor.putString("format", "csv");
        descriptor.putString("my_option", "my");

        assertThatThrownBy(() -> createTableSource(SCHEMA, descriptor.asMap()))
                .satisfies(
                        anyCauseMatches(
                                ValidationException.class, "Unsupported options:\n\nmy_option"));
    }

    @Test
    public void testUnsupportedOptionSink() {
        DescriptorProperties descriptor = new DescriptorProperties();
        descriptor.putString(FactoryUtil.CONNECTOR.key(), "filesystem");
        descriptor.putString("path", "/tmp");
        descriptor.putString("format", "csv");
        descriptor.putString("my_option", "my");

        assertThatThrownBy(() -> createTableSink(SCHEMA, descriptor.asMap()))
                .satisfies(
                        anyCauseMatches(
                                ValidationException.class, "Unsupported options:\n\nmy_option"));
    }

    @Test
    public void testUnsupportedWatermarkTimeZone() {
        DescriptorProperties descriptor = new DescriptorProperties();
        descriptor.putString(FactoryUtil.CONNECTOR.key(), "filesystem");
        descriptor.putString("path", "/tmp");
        descriptor.putString("format", "csv");
        descriptor.putString(SINK_PARTITION_COMMIT_WATERMARK_TIME_ZONE.key(), "UTC+8");

        assertThatThrownBy(() -> createTableSource(SCHEMA, descriptor.asMap()))
                .satisfies(
                        anyCauseMatches(
                                ValidationException.class,
                                "Invalid time zone for 'sink.partition-commit.watermark-time-zone'."));
    }

    @Test
    public void testNoFormatFactoryFound() {
        DescriptorProperties descriptor = new DescriptorProperties();
        descriptor.putString(FactoryUtil.CONNECTOR.key(), "filesystem");
        descriptor.putString("path", "/tmp");
        descriptor.putString("format", "invalid");

        assertThatThrownBy(() -> createTableSource(SCHEMA, descriptor.asMap()))
                .satisfies(
                        anyCauseMatches(
                                ValidationException.class,
                                "Could not find any format factory for identifier 'invalid' in the classpath."));

        assertThatThrownBy(() -> createTableSink(SCHEMA, descriptor.asMap()))
                .satisfies(
                        anyCauseMatches(
                                ValidationException.class,
                                "Could not find any format factory for identifier 'invalid' in the classpath."));
    }

    @Test
    public void testFormatOptionsError() {
        DescriptorProperties descriptor = new DescriptorProperties();
        descriptor.putString(FactoryUtil.CONNECTOR.key(), "filesystem");
        descriptor.putString("path", "/tmp");
        descriptor.putString("format", "test-format");

        assertThatThrownBy(() -> createTableSource(SCHEMA, descriptor.asMap()))
                .satisfies(
                        anyCauseMatches(
                                ValidationException.class,
                                "One or more required options are missing.\n\n"
                                        + "Missing required options are:\n\n"
                                        + "delimiter"));

        assertThatThrownBy(() -> createTableSink(SCHEMA, descriptor.asMap()))
                .satisfies(
                        anyCauseMatches(
                                ValidationException.class,
                                "One or more required options are missing.\n\n"
                                        + "Missing required options are:\n\n"
                                        + "delimiter"));
    }

    @Test
    public void testSupportsMetadata() {
        Map<String, String> descriptor = new HashMap<>();
        descriptor.put(FactoryUtil.CONNECTOR.key(), "filesystem");
        descriptor.put("path", "/tmp");
        descriptor.put("format", "testcsv");
        descriptor.put("testcsv.my_option", "my_value");

        DynamicTableSource source = createTableSource(SCHEMA, descriptor);
        assertThat(source).isInstanceOf(FileSystemTableSource.class);

        Map<String, DataType> readableMetadata = new HashMap<>();
        readableMetadata.put("file.path", DataTypes.STRING().notNull());
        readableMetadata.put("file.name", DataTypes.STRING().notNull());
        readableMetadata.put("file.size", DataTypes.BIGINT().notNull());
        readableMetadata.put("file.modification-time", DataTypes.TIMESTAMP_LTZ(3).notNull());

        assertThat(((FileSystemTableSource) source).listReadableMetadata())
                .isEqualTo(readableMetadata);
    }
}
