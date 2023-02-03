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

package org.apache.flink.connector.hybrid.table;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.expressions.utils.ResolvedExpressionMock;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** Tests for the {@link HybridTableSourceFactory}. */
@ExtendWith(TestLoggerExtension.class)
public class HybridTableSourceFactoryTest {

    private static final String COMPUTED_SQL = "orig_ts - INTERVAL '1' MINUTE";
    private static final String WATERMARK_SQL = "ts - INTERVAL '5' SECOND";

    private static ResolvedSchema tableSchema;

    @BeforeAll
    public static void before() {
        tableSchema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("order_id", DataTypes.INT().notNull()),
                                Column.physical("product_id", DataTypes.VARCHAR(200)),
                                Column.physical("purchaser", DataTypes.VARCHAR(200)),
                                Column.metadata("topic", DataTypes.VARCHAR(200), "topic", false),
                                Column.metadata(
                                        "orig_ts", DataTypes.TIMESTAMP(3), "timestamp", false),
                                Column.computed(
                                        "ts",
                                        ResolvedExpressionMock.of(
                                                DataTypes.TIMESTAMP(3), COMPUTED_SQL))),
                        Collections.singletonList(
                                WatermarkSpec.of(
                                        "ts",
                                        ResolvedExpressionMock.of(
                                                DataTypes.TIMESTAMP(3), WATERMARK_SQL))),
                        UniqueConstraint.primaryKey(
                                "primary_constraint", Collections.singletonList("order_id")));
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void testTableSourceFactoryCorrectly() {
        DynamicTableSource source = createTableSource(tableSchema, getAllOptions());
        assertTrue(source instanceof HybridTableSource);

        HybridTableSource hybridTableSource = (HybridTableSource) source;
        assertNotNull(hybridTableSource.copy());

        assertEquals(
                ChangelogMode.newBuilder()
                        .addContainedKind(RowKind.INSERT)
                        .addContainedKind(RowKind.DELETE)
                        .build(),
                hybridTableSource.getChangelogMode());

        ScanTableSource.ScanRuntimeProvider provider =
                hybridTableSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        assertTrue(provider instanceof SourceProvider);

        SourceProvider sourceProvider = (SourceProvider) provider;
        assertTrue(sourceProvider.createSource() instanceof HybridSource);
        HybridSource hybridSource = (HybridSource) sourceProvider.createSource();
        assertTrue(provider.isBounded());
        assertEquals(hybridSource.getBoundedness(), Boundedness.BOUNDED);
    }

    @Test
    public void testSourceIdentifierNums() {
        Map<String, String> options = new HashMap<>(getAllOptions());
        options.put("source-identifiers", "historical");

        try {
            createTableSource(tableSchema, options);
            fail("Should fail");
        } catch (Exception e) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(e, "must specify at least 2 sources")
                            .isPresent());
        }
    }

    @Test
    public void testSourceIdentifierFormat() {
        Map<String, String> options = new HashMap<>(getAllOptions());
        options.put("source-identifiers", "historical#");

        try {
            createTableSource(tableSchema, options);
            fail("Should fail");
        } catch (Exception e) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                                    e,
                                    "source-identifier pattern must be "
                                            + HybridConnectorOptions.SOURCE_IDENTIFIER_REGEX)
                            .isPresent());
        }
    }

    @Test
    public void testSameSourceIdentifier() {
        Map<String, String> options = new HashMap<>(getAllOptions());
        options.put("source-identifiers", "historical,historical,realtime");

        try {
            createTableSource(tableSchema, options);
            fail("Should fail");
        } catch (Exception e) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                                    e, "each source identifier must not be the same")
                            .isPresent());
        }
    }

    @Test
    public void testNotExistedSourceIdentifier() {
        Map<String, String> options = new HashMap<>(getAllOptions());
        options.put("source-identifiers", "historical01,historical,realtime");

        try {
            createTableSource(tableSchema, options);
            fail("Should fail");
        } catch (Exception e) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                                    e, "Options cannot have null keys or values")
                            .isPresent());
        }
    }

    @Test
    public void testUnsupportedOptions() {
        Map<String, String> options = new HashMap<>(getAllOptions());
        options.put("key", "value");

        try {
            createTableSource(tableSchema, options);
            fail("Should fail");
        } catch (Exception e) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                                    e, "Unsupported options found for 'hybrid'")
                            .isPresent());
        }
    }

    @Test
    public void testExtractChildSourceOptions() {
        Map<String, String> mockOptions = getAllOptions();
        HybridTableSourceFactory mockFactory = new HybridTableSourceFactory();

        assertEquals(
                new HashMap<String, String>() {
                    {
                        put("connector", null);
                    }
                },
                mockFactory.extractChildSourceOptions(mockOptions, "unknown"));

        assertEquals(
                new HashMap<String, String>() {
                    {
                        put("connector", "values");
                        put("data-id", "1");
                        put("bounded", "true");
                        put("runtime-source", "NewSource");
                    }
                },
                mockFactory.extractChildSourceOptions(mockOptions, "historical"));

        assertEquals(
                new HashMap<String, String>() {
                    {
                        put("connector", "values");
                        put("changelog-mode", "I,D");
                        put("runtime-source", "NewSource");
                    }
                },
                mockFactory.extractChildSourceOptions(mockOptions, "realtime"));
    }

    private Map<String, String> getAllOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "hybrid");
        options.put("source-identifiers", "historical,realtime");
        options.put("historical.connector", "values");
        options.put("historical.data-id", "1");
        options.put("historical.runtime-source", "NewSource");
        options.put("historical.bounded", "true");
        options.put("realtime.connector", "values");
        options.put("realtime.changelog-mode", "I,D");
        options.put("realtime.runtime-source", "NewSource");
        return options;
    }
}
