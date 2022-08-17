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

package org.apache.flink.connector.mongodb.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.mongodb.MongoTestUtil;
import org.apache.flink.connector.mongodb.table.config.MongoConnectorOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.DockerImageVersions;
import org.apache.flink.util.TestLoggerExtension;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.table.api.Expressions.row;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/** IT tests for {@link MongoDynamicTableSink}. */
@Testcontainers
@ExtendWith(TestLoggerExtension.class)
public class MongoDynamicTableSinkITCase {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDynamicTableSinkITCase.class);

    @Container
    private static final MongoDBContainer MONGO_CONTAINER =
            MongoTestUtil.createMongoDBContainer(DockerImageVersions.MONGO_4_0, LOG);

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setConfiguration(new Configuration())
                            .build());

    private MongoClient mongoClient;

    @BeforeEach
    public void setUp() {
        mongoClient = MongoClients.create(MONGO_CONTAINER.getConnectionString());
    }

    @AfterEach
    public void tearDown() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    @Test
    public void testSinkWithAllSupportedTypes() throws ExecutionException, InterruptedException {
        String database = "test";
        String collection = "sink_with_all_supported_types";

        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        tEnv.executeSql(
                String.join(
                        "\n",
                        Arrays.asList(
                                "CREATE TABLE mongo_sink",
                                "(",
                                "  _id BIGINT,",
                                "  f1 STRING,",
                                "  f2 BOOLEAN,",
                                "  f3 BINARY,",
                                "  f4 TINYINT,",
                                "  f5 SMALLINT,",
                                "  f6 INTEGER,",
                                "  f7 TIMESTAMP_LTZ(6),",
                                "  f8 TIMESTAMP(3),",
                                "  f9 FLOAT,",
                                "  f10 DOUBLE,",
                                "  f11 DECIMAL(10, 2),",
                                "  f12 MAP<STRING, INTEGER>,",
                                "  f13 ROW<k INTEGER>,",
                                "  f14 ARRAY<STRING>,",
                                "  f15 ARRAY<ROW<k STRING>>,",
                                "  PRIMARY KEY (_id) NOT ENFORCED",
                                ") WITH (",
                                getConnectorSql(database, collection),
                                ")")));

        Instant now = Instant.now();
        tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("_id", DataTypes.BIGINT()),
                                DataTypes.FIELD("f1", DataTypes.STRING()),
                                DataTypes.FIELD("f2", DataTypes.BOOLEAN()),
                                DataTypes.FIELD("f3", DataTypes.BINARY(1)),
                                DataTypes.FIELD("f4", DataTypes.TINYINT()),
                                DataTypes.FIELD("f5", DataTypes.SMALLINT()),
                                DataTypes.FIELD("f6", DataTypes.INT()),
                                DataTypes.FIELD("f7", DataTypes.TIMESTAMP_LTZ(6)),
                                DataTypes.FIELD("f8", DataTypes.TIMESTAMP(3)),
                                DataTypes.FIELD("f9", DataTypes.FLOAT()),
                                DataTypes.FIELD("f10", DataTypes.DOUBLE()),
                                DataTypes.FIELD("f11", DataTypes.DECIMAL(10, 2)),
                                DataTypes.FIELD(
                                        "f12", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())),
                                DataTypes.FIELD(
                                        "f13",
                                        DataTypes.ROW(DataTypes.FIELD("k", DataTypes.INT()))),
                                DataTypes.FIELD("f14", DataTypes.ARRAY(DataTypes.STRING())),
                                DataTypes.FIELD(
                                        "f15",
                                        DataTypes.ARRAY(
                                                DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                "K", DataTypes.STRING()))))),
                        Row.of(
                                1L,
                                "ABCDE",
                                true,
                                new byte[] {(byte) 3},
                                (byte) 4,
                                (short) 5,
                                6,
                                now,
                                Timestamp.from(now),
                                9.9f,
                                10.10d,
                                new BigDecimal("11.11"),
                                Collections.singletonMap("k", 12),
                                Row.of(13),
                                Arrays.asList("14_1", "14_2"),
                                Arrays.asList(Row.of("15_1"), Row.of("15_2"))))
                .executeInsert("mongo_sink")
                .await();

        MongoCollection<Document> coll =
                mongoClient.getDatabase(database).getCollection(collection);

        Document actual = coll.find(Filters.eq("_id", 1L)).first();

        Document expected =
                new Document("_id", 1L)
                        .append("f1", "ABCDE")
                        .append("f2", true)
                        .append("f3", new Binary(new byte[] {(byte) 3}))
                        .append("f4", 4)
                        .append("f5", 5)
                        .append("f6", 6)
                        .append("f7", Date.from(now))
                        .append("f8", Date.from(now))
                        .append("f9", (double) 9.9f)
                        .append("f10", 10.10d)
                        .append("f11", new Decimal128(new BigDecimal("11.11")))
                        .append("f12", new Document("k", 12))
                        .append("f13", new Document("k", 13))
                        .append("f14", Arrays.asList("14_1", "14_2"))
                        .append(
                                "f15",
                                Arrays.asList(
                                        new Document("k", "15_1"), new Document("k", "15_2")));

        assertThat(actual, equalTo(expected));
    }

    @Test
    public void testSinkWithAllRowKind() throws ExecutionException, InterruptedException {
        String database = "test";
        String collection = "test_sink_with_all_row_kind";

        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        String dataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                Row.ofKind(RowKind.INSERT, 1L, "Alice"),
                                Row.ofKind(RowKind.DELETE, 1L, "Alice"),
                                Row.ofKind(RowKind.INSERT, 2L, "Bob"),
                                Row.ofKind(RowKind.UPDATE_BEFORE, 2L, "Bob"),
                                Row.ofKind(RowKind.UPDATE_AFTER, 2L, "Tom")));

        tEnv.executeSql(
                String.format(
                        "CREATE TABLE value_source (\n"
                                + "`_id` BIGINT,\n"
                                + "`name` STRING\n"
                                + ") WITH (\n"
                                + "'connector' = 'values', \n"
                                + "'data-id' = '%s')",
                        dataId));

        tEnv.executeSql(
                String.format(
                        "CREATE TABLE mongo_sink (\n"
                                + "`_id` BIGINT,\n"
                                + "`name` STRING,\n"
                                + " PRIMARY KEY (_id) NOT ENFORCED\n"
                                + ") WITH ( %s )",
                        getConnectorSql(database, collection)));

        tEnv.executeSql("insert into mongo_sink select * from value_source").await();

        MongoCollection<Document> coll =
                mongoClient.getDatabase(database).getCollection(collection);

        List<Document> expected =
                Collections.singletonList(new Document("_id", 2L).append("name", "Tom"));

        List<Document> actual = coll.find().into(new ArrayList<>());

        assertThat(actual, equalTo(expected));
    }

    @Test
    public void testSinkWithReversedId() throws Exception {
        String database = "test";
        String collection = "sink_with_reversed_id";

        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        tEnv.executeSql(
                String.format(
                        "CREATE TABLE mongo_sink ("
                                + "_id STRING NOT NULL,\n"
                                + "f1 STRING NOT NULL,\n"
                                + "PRIMARY KEY (_id) NOT ENFORCED\n"
                                + ")\n"
                                + "WITH (%s)",
                        getConnectorSql(database, collection)));

        ObjectId objectId = new ObjectId();
        tEnv.fromValues(row(objectId.toHexString(), "r1"), row("str", "r2"))
                .executeInsert("mongo_sink")
                .await();

        MongoCollection<Document> coll =
                mongoClient.getDatabase(database).getCollection(collection);

        List<Document> actual = new ArrayList<>();
        coll.find(Filters.in("_id", objectId, "str")).into(actual);

        Document[] expected =
                new Document[] {
                    new Document("_id", objectId).append("f1", "r1"),
                    new Document("_id", "str").append("f1", "r2")
                };
        assertThat(actual, containsInAnyOrder(expected));
    }

    @Test
    public void testSinkWithoutPrimaryKey() throws Exception {
        String database = "test";
        String collection = "sink_without_primary_key";

        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        tEnv.executeSql(
                String.format(
                        "CREATE TABLE mongo_sink (" + "f1 STRING NOT NULL\n" + ")\n" + "WITH (%s)",
                        getConnectorSql(database, collection)));

        tEnv.fromValues(row("d1"), row("d1")).executeInsert("mongo_sink").await();

        MongoCollection<Document> coll =
                mongoClient.getDatabase(database).getCollection(collection);

        List<Document> actual = new ArrayList<>();
        coll.find().into(actual);

        assertThat(actual, hasSize(2));
        for (Document doc : actual) {
            assertThat(doc.get("f1"), equalTo("d1"));
        }
    }

    @Test
    public void testSinkWithNonCompositePrimaryKey() throws Exception {
        String database = "test";
        String collection = "sink_with_non_composite_pk";

        Instant now = Instant.now();
        List<Expression> testValues =
                Collections.singletonList(
                        row(
                                1L,
                                true,
                                "ABCDE",
                                12.12d,
                                24.24f,
                                (byte) 2,
                                (short) 3,
                                4,
                                Timestamp.from(now),
                                now));
        List<String> primaryKeys = Collections.singletonList("a");

        testSinkWithoutReversedId(database, collection, primaryKeys, testValues);

        MongoCollection<Document> coll =
                mongoClient.getDatabase(database).getCollection(collection);

        Document actual = coll.find(Filters.eq("_id", 1L)).first();
        Document expected = new Document();
        expected.put("_id", 1L);
        expected.put("a", 1L);
        expected.put("b", true);
        expected.put("c", "ABCDE");
        expected.put("d", 12.12d);
        expected.put("e", (double) 24.24f);
        expected.put("f", 2);
        expected.put("g", 3);
        expected.put("h", 4);
        expected.put("i", Date.from(now));
        expected.put("j", Date.from(now));
        assertThat(actual, equalTo(expected));
    }

    @Test
    public void testSinkWithCompositePrimaryKey() throws Exception {
        String database = "test";
        String collection = "sink_with_composite_pk";

        Instant now = Instant.now();
        List<Expression> testValues =
                Collections.singletonList(
                        row(
                                1L,
                                true,
                                "ABCDE",
                                12.12d,
                                24.24f,
                                (byte) 2,
                                (short) 3,
                                4,
                                Timestamp.from(now),
                                now));
        List<String> primaryKeys = Arrays.asList("a", "c");

        testSinkWithoutReversedId(database, collection, primaryKeys, testValues);

        MongoCollection<Document> coll =
                mongoClient.getDatabase(database).getCollection(collection);

        Document compositeId = new Document();
        compositeId.put("a", 1L);
        compositeId.put("c", "ABCDE");
        Document actual = coll.find(Filters.eq("_id", compositeId)).first();

        Document expected = new Document();
        expected.put("_id", new Document(compositeId));
        expected.put("a", 1L);
        expected.put("b", true);
        expected.put("c", "ABCDE");
        expected.put("d", 12.12d);
        expected.put("e", (double) 24.24f);
        expected.put("f", 2);
        expected.put("g", 3);
        expected.put("h", 4);
        expected.put("i", Date.from(now));
        expected.put("j", Date.from(now));
        assertThat(actual, equalTo(expected));
    }

    private void testSinkWithoutReversedId(
            String database, String collection, List<String> primaryKeys, List<Expression> values)
            throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        tEnv.executeSql(
                String.format(
                        "CREATE TABLE mongo_sink ("
                                + "a BIGINT NOT NULL,\n"
                                + "b BOOLEAN,\n"
                                + "c STRING NOT NULL,\n"
                                + "d DOUBLE,\n"
                                + "e FLOAT,\n"
                                + "f TINYINT NOT NULL,\n"
                                + "g SMALLINT NOT NULL,\n"
                                + "h INT NOT NULL,\n"
                                + "i TIMESTAMP NOT NULL,\n"
                                + "j TIMESTAMP_LTZ NOT NULL,\n"
                                + "PRIMARY KEY (%s) NOT ENFORCED\n"
                                + ")\n"
                                + "WITH (%s)",
                        getPrimaryKeys(primaryKeys), getConnectorSql(database, collection)));

        tEnv.fromValues(values).executeInsert("mongo_sink").await();
    }

    private String getPrimaryKeys(List<String> fieldNames) {
        return String.join(",", fieldNames);
    }

    private String getConnectorSql(String database, String collection) {
        return String.format("'%s'='%s',\n", "connector", "mongodb")
                + String.format(
                        "'%s'='%s',\n",
                        MongoConnectorOptions.URI.key(), MONGO_CONTAINER.getConnectionString())
                + String.format("'%s'='%s',\n", MongoConnectorOptions.DATABASE.key(), database)
                + String.format("'%s'='%s'\n", MongoConnectorOptions.COLLECTION.key(), collection);
    }
}
