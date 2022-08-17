/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.mongodb.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.mongodb.MongoTestUtil;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.StreamTestSink;
import org.apache.flink.table.runtime.functions.table.lookup.LookupCacheManager;
import org.apache.flink.table.test.lookup.cache.LookupCacheAssert;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.DockerImageVersions;
import org.apache.flink.util.TestLoggerExtension;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/** ITCase for {@link MongoDynamicTableSource}. */
@Testcontainers
@ExtendWith(TestLoggerExtension.class)
public class MongoDynamicTableSourceITCase {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDynamicTableSinkITCase.class);

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setConfiguration(new Configuration())
                            .build());

    @Container
    private static final MongoDBContainer MONGO_CONTAINER =
            MongoTestUtil.createMongoDBContainer(DockerImageVersions.MONGO_4_0, LOG);

    public static final String DATABASE = "test";
    public static final String COLLECTION = "mongo_table_source";

    private static MongoClient mongoClient;

    public static StreamExecutionEnvironment env;
    public static TableEnvironment tEnv;

    @BeforeAll
    static void beforeAll() {
        mongoClient = MongoClients.create(MONGO_CONTAINER.getConnectionString());

        MongoCollection<BsonDocument> coll =
                mongoClient
                        .getDatabase(DATABASE)
                        .getCollection(COLLECTION)
                        .withDocumentClass(BsonDocument.class);

        List<BsonDocument> testRecords = new ArrayList<>();
        testRecords.add(createTestData(1));
        testRecords.add(createTestData(2));
        coll.insertMany(testRecords);
    }

    @AfterAll
    static void afterAll() {
        if (mongoClient != null) {
            mongoClient.close();
        }
        StreamTestSink.clear();
    }

    @BeforeEach
    void before() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
    }

    @Test
    public void testSource() {
        tEnv.executeSql(createTestDDl(null));

        Iterator<Row> collected = tEnv.executeSql("SELECT * FROM mongo_source").collect();
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());

        List<String> expected =
                Stream.of(
                                "+I[1, 2, false, [3], 4, 5, 6, 2022-09-07T10:25:28.127Z, 2022-09-07T10:25:28Z, 0.9, 1.0, 1.10, {k=12}, +I[13], [14_1, 14_2], [+I[15_1], +I[15_2]]]",
                                "+I[2, 2, false, [3], 4, 5, 6, 2022-09-07T10:25:28.127Z, 2022-09-07T10:25:28Z, 0.9, 1.0, 1.10, {k=12}, +I[13], [14_1, 14_2], [+I[15_1], +I[15_2]]]")
                        .sorted()
                        .collect(Collectors.toList());

        assertThat(result, equalTo(expected));
    }

    @Test
    public void testProject() {
        tEnv.executeSql(createTestDDl(null));

        Iterator<Row> collected = tEnv.executeSql("SELECT f1, f13 FROM mongo_source").collect();
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());

        List<String> expected =
                Stream.of("+I[2, +I[13]]", "+I[2, +I[13]]").sorted().collect(Collectors.toList());

        assertThat(result, equalTo(expected));
    }

    @Test
    public void testLimit() {
        tEnv.executeSql(createTestDDl(null));

        Iterator<Row> collected = tEnv.executeSql("SELECT * FROM mongo_source LIMIT 1").collect();
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());

        Set<String> expected = new HashSet<>();
        expected.add(
                "+I[1, 2, false, [3], 4, 5, 6, 2022-09-07T10:25:28.127Z, 2022-09-07T10:25:28Z, 0.9, 1.0, 1.10, {k=12}, +I[13], [14_1, 14_2], [+I[15_1], +I[15_2]]]");
        expected.add(
                "+I[2, 2, false, [3], 4, 5, 6, 2022-09-07T10:25:28.127Z, 2022-09-07T10:25:28Z, 0.9, 1.0, 1.10, {k=12}, +I[13], [14_1, 14_2], [+I[15_1], +I[15_2]]]");

        assertThat(result.size(), is(1));
        assertThat(expected, hasItem(result.get(0)));
    }

    @ParameterizedTest
    @EnumSource(Caching.class)
    public void testLookupJoin(Caching caching) throws Exception {
        // Create MongoDB lookup table
        Map<String, String> lookupOptions = new HashMap<>();
        if (caching.equals(Caching.ENABLE_CACHE)) {
            lookupOptions.put("lookup.cache", "PARTIAL");
            lookupOptions.put("lookup.partial-cache.expire-after-write", "10min");
            lookupOptions.put("lookup.partial-cache.expire-after-access", "10min");
            lookupOptions.put("lookup.partial-cache.max-rows", "100");
            lookupOptions.put("lookup.max-retries", "10");
        }

        tEnv.executeSql(createTestDDl(lookupOptions));

        // Create and prepare a value source
        String dataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                Row.of(1L, "Alice"),
                                Row.of(1L, "Alice"),
                                Row.of(2L, "Bob"),
                                Row.of(3L, "Charlie")));
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE value_source (\n"
                                + "`id` BIGINT,\n"
                                + "`name` STRING,\n"
                                + "`proctime` AS PROCTIME()\n"
                                + ") WITH (\n"
                                + "'connector' = 'values', \n"
                                + "'data-id' = '%s')",
                        dataId));

        if (caching == Caching.ENABLE_CACHE) {
            LookupCacheManager.keepCacheOnRelease(true);
        }

        // Execute lookup join
        try (CloseableIterator<Row> iterator =
                tEnv.executeSql(
                                "SELECT S.id, S.name, D._id, D.f1, D.f2 FROM value_source"
                                        + " AS S JOIN mongo_source for system_time as of S.proctime AS D ON S.id = D._id")
                        .collect()) {
            List<String> result =
                    CollectionUtil.iteratorToList(iterator).stream()
                            .map(Row::toString)
                            .sorted()
                            .collect(Collectors.toList());
            List<String> expected = new ArrayList<>();
            expected.add("+I[1, Alice, 1, 2, false]");
            expected.add("+I[1, Alice, 1, 2, false]");
            expected.add("+I[2, Bob, 2, 2, false]");

            assertThat(result.size(), is(3));
            assertThat(result, equalTo(expected));
            if (caching == Caching.ENABLE_CACHE) {
                // Validate cache
                Map<String, LookupCacheManager.RefCountedCache> managedCaches =
                        LookupCacheManager.getInstance().getManagedCaches();
                assertThat(managedCaches.size(), is(1));
                LookupCache cache =
                        managedCaches.get(managedCaches.keySet().iterator().next()).getCache();
                validateCachedValues(cache);
            }

        } finally {
            if (caching == Caching.ENABLE_CACHE) {
                LookupCacheManager.getInstance().checkAllReleased();
                LookupCacheManager.getInstance().clear();
                LookupCacheManager.keepCacheOnRelease(false);
            }
        }
    }

    private void validateCachedValues(LookupCache cache) {
        // mongo does support project push down, the cached row has been projected
        RowData key1 = GenericRowData.of(1L);
        RowData value1 = GenericRowData.of(1L, StringData.fromString("2"), false);

        RowData key2 = GenericRowData.of(2L);
        RowData value2 = GenericRowData.of(2L, StringData.fromString("2"), false);

        RowData key3 = GenericRowData.of(3L);

        Map<RowData, Collection<RowData>> expectedEntries = new HashMap<>();
        expectedEntries.put(key1, Collections.singletonList(value1));
        expectedEntries.put(key2, Collections.singletonList(value2));
        expectedEntries.put(key3, Collections.emptyList());

        LookupCacheAssert.assertThat(cache).containsExactlyEntriesOf(expectedEntries);
    }

    private enum Caching {
        ENABLE_CACHE,
        DISABLE_CACHE
    }

    private static String createTestDDl(Map<String, String> extraOptions) {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "mongodb");
        options.put("uri", MONGO_CONTAINER.getConnectionString());
        options.put("database", DATABASE);
        options.put("collection", COLLECTION);
        if (extraOptions != null) {
            options.putAll(extraOptions);
        }

        String optionString =
                options.entrySet().stream()
                        .map(e -> String.format("'%s' = '%s'", e.getKey(), e.getValue()))
                        .collect(Collectors.joining(",\n"));

        return String.join(
                "\n",
                Arrays.asList(
                        "CREATE TABLE mongo_source",
                        "(",
                        "  _id BIGINT,",
                        "  f1 STRING,",
                        "  f2 BOOLEAN,",
                        "  f3 BINARY,",
                        "  f4 TINYINT,",
                        "  f5 SMALLINT,",
                        "  f6 INTEGER,",
                        "  f7 TIMESTAMP_LTZ(6),",
                        "  f8 TIMESTAMP_LTZ(3),",
                        "  f9 FLOAT,",
                        "  f10 DOUBLE,",
                        "  f11 DECIMAL(10, 2),",
                        "  f12 MAP<STRING, INTEGER>,",
                        "  f13 ROW<k INTEGER>,",
                        "  f14 ARRAY<STRING>,",
                        "  f15 ARRAY<ROW<k STRING>>",
                        ") WITH (",
                        optionString,
                        ")"));
    }

    private static BsonDocument createTestData(long id) {
        return new BsonDocument()
                .append("_id", new BsonInt64(id))
                .append("f1", new BsonString("2"))
                .append("f2", BsonBoolean.FALSE)
                .append("f3", new BsonBinary(new byte[] {(byte) 3}))
                .append("f4", new BsonInt32(4))
                .append("f5", new BsonInt32(5))
                .append("f6", new BsonInt32(6))
                // 2022-09-07T10:25:28.127Z
                .append("f7", new BsonDateTime(1662546328127L))
                .append("f8", new BsonTimestamp(1662546328, 0))
                .append("f9", new BsonDouble(0.9d))
                .append("f10", new BsonDouble(1.0d))
                .append("f11", new BsonDecimal128(new Decimal128(new BigDecimal("1.10"))))
                .append("f12", new BsonDocument("k", new BsonInt32(12)))
                .append("f13", new BsonDocument("k", new BsonInt32(13)))
                .append(
                        "f14",
                        new BsonArray(
                                Arrays.asList(new BsonString("14_1"), new BsonString("14_2"))))
                .append(
                        "f15",
                        new BsonArray(
                                Arrays.asList(
                                        new BsonDocument("k", new BsonString("15_1")),
                                        new BsonDocument("k", new BsonString("15_2")))));
    }
}
