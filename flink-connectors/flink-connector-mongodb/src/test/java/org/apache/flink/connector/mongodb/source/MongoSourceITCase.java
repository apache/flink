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

package org.apache.flink.connector.mongodb.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.mongodb.MongoTestUtil;
import org.apache.flink.connector.mongodb.source.enumerator.splitter.PartitionStrategy;
import org.apache.flink.connector.mongodb.source.reader.deserializer.MongoJsonDeserializationSchema;
import org.apache.flink.connector.mongodb.table.serialization.MongoRowDataDeserializationSchema;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.DockerImageVersions;
import org.apache.flink.util.TestLoggerExtension;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.apache.commons.lang3.RandomStringUtils;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
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

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for using Mongo Sink. */
@Testcontainers
@ExtendWith(TestLoggerExtension.class)
public class MongoSourceITCase {

    private static final Logger LOG = LoggerFactory.getLogger(MongoSourceITCase.class);

    @Container
    private static final MongoDBContainer MONGO_CONTAINER =
            MongoTestUtil.createMongoDBContainer(DockerImageVersions.MONGO_4_0, LOG);

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .setConfiguration(new Configuration())
                            .build());

    private static MongoClient mongoClient;

    private static final String TEST_DATABASE = "test_source";

    public static final String TEST_COLLECTION = "mongo_source";

    @BeforeAll
    static void beforeAll() {
        mongoClient = MongoClients.create(MONGO_CONTAINER.getConnectionString());

        MongoCollection<BsonDocument> coll =
                mongoClient
                        .getDatabase(TEST_DATABASE)
                        .getCollection(TEST_COLLECTION)
                        .withDocumentClass(BsonDocument.class);

        List<BsonDocument> testRecords = new ArrayList<>();
        for (int i = 1; i <= 30000; i++) {
            testRecords.add(createTestData(i));
            if (testRecords.size() >= 10000) {
                coll.insertMany(testRecords);
                testRecords.clear();
            }
        }
    }

    @AfterAll
    static void afterAll() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    @ParameterizedTest
    @EnumSource(PartitionStrategy.class)
    public void testPartitionStrategy(PartitionStrategy partitionStrategy) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        MongoSource<RowData> mongoSource =
                defaultSourceBuilder()
                        .setPartitionSize(MemorySize.parse("1mb"))
                        .setSamplesPerPartition(3)
                        .setPartitionStrategy(partitionStrategy)
                        .build();

        List<RowData> results =
                CollectionUtil.iteratorToList(
                        env.fromSource(
                                        mongoSource,
                                        WatermarkStrategy.noWatermarks(),
                                        "MongoDB-Source")
                                .executeAndCollect());

        assertThat(results).hasSize(30000);
    }

    @Test
    public void testLimit() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        MongoSource<RowData> mongoSource = defaultSourceBuilder().setLimit(100).build();

        List<RowData> results =
                CollectionUtil.iteratorToList(
                        env.fromSource(
                                        mongoSource,
                                        WatermarkStrategy.noWatermarks(),
                                        "MongoDB-Source")
                                .executeAndCollect());

        assertThat(results).hasSize(100);
    }

    @Test
    public void testProject() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        MongoSource<String> mongoSource =
                MongoSource.<String>builder()
                        .setUri(MONGO_CONTAINER.getConnectionString())
                        .setDatabase(TEST_DATABASE)
                        .setCollection(TEST_COLLECTION)
                        .setProjectedFields("f0")
                        .setDeserializationSchema(new MongoJsonDeserializationSchema())
                        .build();

        List<String> results =
                CollectionUtil.iteratorToList(
                        env.fromSource(
                                        mongoSource,
                                        WatermarkStrategy.noWatermarks(),
                                        "MongoDB-Source")
                                .executeAndCollect());

        assertThat(results).hasSize(30000);
        assertThat(Document.parse(results.get(0))).containsOnlyKeys("f0");
    }

    private MongoSourceBuilder<RowData> defaultSourceBuilder() {
        ResolvedSchema schema = defaultSourceSchema();
        RowType rowType = (RowType) schema.toPhysicalRowDataType().getLogicalType();
        TypeInformation<RowData> typeInfo = InternalTypeInfo.of(rowType);

        return MongoSource.<RowData>builder()
                .setUri(MONGO_CONTAINER.getConnectionString())
                .setDatabase(TEST_DATABASE)
                .setCollection(TEST_COLLECTION)
                .setDeserializationSchema(new MongoRowDataDeserializationSchema(rowType, typeInfo));
    }

    private ResolvedSchema defaultSourceSchema() {
        return ResolvedSchema.of(
                Column.physical("f0", DataTypes.INT()), Column.physical("f1", DataTypes.STRING()));
    }

    private static BsonDocument createTestData(int id) {
        return new BsonDocument("f0", new BsonInt32(id))
                .append("f1", new BsonString(RandomStringUtils.randomAlphabetic(32)));
    }
}
