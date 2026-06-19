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

package org.apache.flink.table.file.testutils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.table.FileSystemTableSink;
import org.apache.flink.connector.file.table.FileSystemTableSource;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.api.common.RuntimeExecutionMode.BATCH;
import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TestFileSystemTableFactory}. */
public class TestFileSystemTableFactoryTest {

    private static final ResolvedSchema SCHEMA =
            ResolvedSchema.of(
                    Column.physical("f0", DataTypes.STRING()),
                    Column.physical("f1", DataTypes.BIGINT()),
                    Column.physical("f2", DataTypes.BIGINT()));

    @Test
    void testCreateSourceSink() {
        Map<String, String> options = new HashMap<>();
        options.put(FactoryUtil.CONNECTOR.key(), "test-filesystem");
        options.put("path", "/tmp");
        options.put("format", "testcsv");

        // test ignore format options
        options.put("testcsv.my_option", "my_value");

        // test ignore partition fields
        options.put("partition.fields.f1.date-formatter", "yyyy-MM-dd");

        DynamicTableSource source = createTableSource(SCHEMA, options);
        assertThat(source).isInstanceOf(FileSystemTableSource.class);

        DynamicTableSink sink = createTableSink(SCHEMA, options);
        assertThat(sink).isInstanceOf(FileSystemTableSink.class);
    }

    @Test
    void testCreateUnboundedSource() {
        Map<String, String> options = new HashMap<>();
        options.put(FactoryUtil.CONNECTOR.key(), "test-filesystem");
        options.put("path", "/tmp");
        options.put("format", "testcsv");
        options.put("source.monitor-interval", "5S");

        DynamicTableSource source = createTableSource(SCHEMA, options);
        assertThat(source).isInstanceOf(FileSystemTableSource.class);

        // assert source is unbounded when specify source.monitor-interval
        ScanTableSource.ScanRuntimeProvider scanRuntimeProvider =
                ((FileSystemTableSource) source)
                        .getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        assertThat(scanRuntimeProvider.isBounded()).isFalse();
    }

    @Test
    void testCreateBoundedSource() {
        Map<String, String> options = new HashMap<>();
        options.put(FactoryUtil.CONNECTOR.key(), "test-filesystem");
        options.put("path", "/tmp");
        options.put("format", "testcsv");
        options.put("source.monitor-interval", "5S");

        Configuration configuration = new Configuration();
        configuration.set(RUNTIME_MODE, BATCH);

        DynamicTableSource source = createTableSource(SCHEMA, options, configuration);
        assertThat(source).isInstanceOf(FileSystemTableSource.class);

        // assert source is bounded when specify source.monitor-interval and in batch mode
        ScanTableSource.ScanRuntimeProvider scanRuntimeProvider =
                ((FileSystemTableSource) source)
                        .getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        assertThat(scanRuntimeProvider.isBounded()).isTrue();
    }
}
