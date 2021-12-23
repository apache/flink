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

package org.apache.flink.connector.jdbc.table;

import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.guava30.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static org.apache.flink.connector.jdbc.JdbcTestFixture.DERBY_EBOOKSHOP_DB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Test suite for {@link JdbcRowDataAsyncLookupFunction}. */
public class JdbcRowDataAsyncLookupFunctionTest extends JdbcLookupTestBase {

    private static String[] fieldNames = new String[] {"id1", "id2", "comment1", "comment2"};
    private static DataType[] fieldDataTypes =
            new DataType[] {
                DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING()
            };

    private static String[] lookupKeys = new String[] {"id1", "id2"};

    @Test
    public void testEval() throws Exception {

        JdbcLookupOptions lookupOptions = JdbcLookupOptions.builder().build();
        JdbcRowDataAsyncLookupFunction lookupFunction =
                buildRowDataAsyncLookupFunction(lookupOptions);
        lookupFunction.open(null);
        List<String> result = new ArrayList();
        Object[][] rowKeys =
                new Object[][] {
                    new Object[] {1, "1"},
                    new Object[] {2, "3"},
                };

        CountDownLatch latch = new CountDownLatch(rowKeys.length);

        for (Object[] rowKey : rowKeys) {
            CompletableFuture<Collection<RowData>> future = new CompletableFuture<>();
            lookupFunction.eval(future, rowKey[0], StringData.fromString(rowKey[1].toString()));
            future.whenComplete(
                    (rs, t) -> {
                        synchronized (result) {
                            rs.forEach(row -> result.add(row.toString()));
                        }
                        latch.countDown();
                    });
        }
        // this verifies lookup calls are async
        assertTrue(result.size() < rowKeys.length);
        latch.await();

        // close connection pool
        lookupFunction.close();
        List<String> sortResult =
                Lists.newArrayList(result).stream().sorted().collect(Collectors.toList());
        List<String> expected = new ArrayList<>();

        expected.add("+I(1,1,11-c1-v1,11-c2-v1)");
        expected.add("+I(1,1,11-c1-v2,11-c2-v2)");
        expected.add("+I(2,3,null,23-c2)");
        Collections.sort(expected);

        assertEquals(sortResult, expected);
    }

    @Test
    public void testEvalWithCacheMissingKeyPositive() throws Exception {
        JdbcLookupOptions lookupOptions =
                JdbcLookupOptions.builder()
                        .setCacheMissingKey(true)
                        .setCacheExpireMs(60000)
                        .setCacheMaxSize(10)
                        .setLookupAsyncParallelism(2)
                        .build();
        JdbcRowDataAsyncLookupFunction lookupFunction =
                buildRowDataAsyncLookupFunction(lookupOptions);
        lookupFunction.open(null);
        RowData keyRow = GenericRowData.of(4, StringData.fromString("9"));
        List<String> result = new ArrayList();

        CompletableFuture<Collection<RowData>> future = new CompletableFuture<>();
        lookupFunction.eval(future, 4, StringData.fromString("9"));
        future.whenCompleteAsync(
                (rs, t) -> {
                    rs.forEach(row -> result.add(row.toString()));
                });
        future.get();
        // get cache
        Cache<RowData, List<RowData>> cache = lookupFunction.getCache();

        // empty data should cache
        assertEquals(cache.getIfPresent(keyRow), Collections.<RowData>emptyList());

        // put db entry for keyRow
        // final cache output should also be empty till TTL expires
        insert(
                "INSERT INTO "
                        + LOOKUP_TABLE
                        + " (id1, id2, comment1, comment2) VALUES (4, '9', '49-c1', '49-c2')");

        lookupFunction.eval(future, 4, StringData.fromString("9"));
        future.whenCompleteAsync(
                (rs, t) -> {
                    rs.forEach(row -> result.add(row.toString()));
                });
        future.get();

        assertEquals(cache.getIfPresent(keyRow), Collections.<RowData>emptyList());
    }

    @Test
    public void testEvalWithCacheMissingKeyNegative() throws Exception {
        JdbcLookupOptions lookupOptions =
                JdbcLookupOptions.builder()
                        .setCacheMissingKey(false)
                        .setCacheExpireMs(60000)
                        .setCacheMaxSize(10)
                        .build();

        JdbcRowDataAsyncLookupFunction lookupFunction =
                buildRowDataAsyncLookupFunction(lookupOptions);
        lookupFunction.open(null);
        List<String> result = new ArrayList();
        Object[][] rowKeys = new Object[][] {new Object[] {5, "1"}};
        RowData keyRow = GenericRowData.of(5, StringData.fromString("1"));

        CountDownLatch latch = new CountDownLatch(rowKeys.length);
        for (Object[] rowKey : rowKeys) {
            CompletableFuture<Collection<RowData>> future = new CompletableFuture<>();
            lookupFunction.eval(future, rowKey[0], StringData.fromString(rowKey[1].toString()));
            future.whenComplete(
                    (rs, t) -> {
                        synchronized (result) {
                            rs.forEach(row -> result.add(row.toString()));
                        }
                        latch.countDown();
                    });
        }
        latch.await();
        Cache<RowData, List<RowData>> cache = lookupFunction.getCache();
        // empty data should not get cached
        assert cache.getIfPresent(keyRow) == null;

        // put db entry for keyRow
        // final cache output should contain data
        insert(
                "INSERT INTO "
                        + LOOKUP_TABLE
                        + " (id1, id2, comment1, comment2) VALUES (5, '1', '51-c1', '51-c2')");

        CountDownLatch latchAfter = new CountDownLatch(rowKeys.length);
        for (Object[] rowKey : rowKeys) {
            CompletableFuture<Collection<RowData>> future = new CompletableFuture<>();
            lookupFunction.eval(future, rowKey[0], StringData.fromString(rowKey[1].toString()));
            future.whenComplete(
                    (rs, t) -> {
                        synchronized (result) {
                            rs.forEach(row -> result.add(row.toString()));
                        }
                        latchAfter.countDown();
                    });
        }
        latchAfter.await();

        List<RowData> expectedOutput = new ArrayList<>();
        expectedOutput.add(
                GenericRowData.of(
                        5,
                        StringData.fromString("1"),
                        StringData.fromString("51-c1"),
                        StringData.fromString("51-c2")));
        assertEquals(cache.getIfPresent(keyRow), expectedOutput);
    }

    private JdbcRowDataAsyncLookupFunction buildRowDataAsyncLookupFunction(
            JdbcLookupOptions lookupOptions) {
        JdbcConnectorOptions jdbcOptions =
                JdbcConnectorOptions.builder()
                        .setDriverName(DERBY_EBOOKSHOP_DB.getDriverClass())
                        .setDBUrl(DB_URL)
                        .setTableName(LOOKUP_TABLE)
                        .build();

        RowType rowType =
                RowType.of(
                        Arrays.stream(fieldDataTypes)
                                .map(DataType::getLogicalType)
                                .toArray(LogicalType[]::new),
                        fieldNames);

        JdbcRowDataAsyncLookupFunction lookupFunction =
                new JdbcRowDataAsyncLookupFunction(
                        jdbcOptions,
                        lookupOptions,
                        fieldNames,
                        fieldDataTypes,
                        lookupKeys,
                        rowType);

        return lookupFunction;
    }
}
