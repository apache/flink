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

package org.apache.flink.connector.mongodb.sink.writer;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.mongodb.MongoTestUtil;
import org.apache.flink.connector.mongodb.common.config.MongoConnectionOptions;
import org.apache.flink.connector.mongodb.sink.config.MongoWriteOptions;
import org.apache.flink.connector.mongodb.sink.writer.context.MongoSinkContext;
import org.apache.flink.connector.mongodb.sink.writer.serializer.MongoSerializationSchema;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.mailbox.SyncMailboxExecutor;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.InternalSinkWriterMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.DockerImageVersions;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.flink.util.UserCodeClassLoader;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.annotation.Nullable;

import java.util.Optional;
import java.util.OptionalLong;

import static org.apache.flink.connector.mongodb.MongoTestUtil.assertThatIdsAreNotWritten;
import static org.apache.flink.connector.mongodb.MongoTestUtil.assertThatIdsAreWritten;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

/** Tests for {@link MongoWriter}. */
@Testcontainers
@ExtendWith(TestLoggerExtension.class)
public class MongoWriterITCase {

    private static final Logger LOG = LoggerFactory.getLogger(MongoWriterITCase.class);

    private static final String TEST_DATABASE = "test_writer";

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .setConfiguration(new Configuration())
                            .build());

    @Container
    private static final MongoDBContainer MONGO_CONTAINER =
            MongoTestUtil.createMongoDBContainer(DockerImageVersions.MONGO_4_0, LOG);

    private static final int DEFAULT_SINK_PARALLELISM = 2;

    private static MongoClient mongoClient;
    private static MetricListener metricListener;

    @BeforeAll
    static void beforeAll() {
        mongoClient = MongoClients.create(MONGO_CONTAINER.getConnectionString());
    }

    @AfterAll
    static void afterAll() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    @BeforeEach
    void setUp() {
        metricListener = new MetricListener();
    }

    @Test
    void testWriteOnBulkFlush() throws Exception {
        final String collection = "test-bulk-flush-without-checkpoint";
        final boolean flushOnCheckpoint = false;
        final int bulkFlushMaxActions = 5;
        final int bulkFlushInterval = -1;

        try (final MongoWriter<Document> writer =
                createWriter(
                        collection, bulkFlushMaxActions, bulkFlushInterval, flushOnCheckpoint)) {
            writer.write(buildMessage(1), null);
            writer.write(buildMessage(2), null);
            writer.write(buildMessage(3), null);
            writer.write(buildMessage(4), null);

            // Ignore flush on checkpoint
            writer.flush(false);

            assertThatIdsAreNotWritten(collectionOf(collection), 1, 2, 3, 4);

            // Trigger flush
            writer.write(buildMessage(5), null);
            assertThatIdsAreWritten(collectionOf(collection), 1, 2, 3, 4, 5);

            writer.write(buildMessage(6), null);
            assertThatIdsAreNotWritten(collectionOf(collection), 6);

            // Force flush
            writer.doBulkWrite();
            assertThatIdsAreWritten(collectionOf(collection), 1, 2, 3, 4, 5, 6);
        }
    }

    @Test
    void testWriteOnBulkIntervalFlush() throws Exception {
        final String collection = "test-bulk-flush-with-interval";
        final boolean flushOnCheckpoint = false;
        final int bulkFlushMaxActions = -1;
        final int bulkFlushInterval = 1000;

        try (final MongoWriter<Document> writer =
                createWriter(
                        collection, bulkFlushMaxActions, bulkFlushInterval, flushOnCheckpoint)) {
            writer.write(buildMessage(1), null);
            writer.write(buildMessage(2), null);
            writer.write(buildMessage(3), null);
            writer.write(buildMessage(4), null);
            writer.doBulkWrite();
        }

        assertThatIdsAreWritten(collectionOf(collection), 1, 2, 3, 4);
    }

    @Test
    void testWriteOnCheckpoint() throws Exception {
        final String collection = "test-bulk-flush-with-checkpoint";
        final boolean flushOnCheckpoint = true;
        final int bulkFlushMaxActions = -1;
        final int bulkFlushInterval = -1;

        // Enable flush on checkpoint
        try (final MongoWriter<Document> writer =
                createWriter(
                        collection, bulkFlushMaxActions, bulkFlushInterval, flushOnCheckpoint)) {
            writer.write(buildMessage(1), null);
            writer.write(buildMessage(2), null);
            writer.write(buildMessage(3), null);

            assertThatIdsAreNotWritten(collectionOf(collection), 1, 2, 3);

            // Trigger flush
            writer.flush(false);

            assertThatIdsAreWritten(collectionOf(collection), 1, 2, 3);
        }
    }

    @Test
    void testIncrementRecordsSendMetric() throws Exception {
        final String collection = "test-inc-records-send";
        final boolean flushOnCheckpoint = false;
        final int bulkFlushMaxActions = 2;
        final int bulkFlushInterval = -1;

        try (final MongoWriter<Document> writer =
                createWriter(
                        collection, bulkFlushMaxActions, bulkFlushInterval, flushOnCheckpoint)) {
            final Optional<Counter> recordsSend =
                    metricListener.getCounter(MetricNames.NUM_RECORDS_SEND);
            writer.write(buildMessage(1), null);
            // Update existing index
            writer.write(buildMessage(2, "u"), null);
            // Delete index
            writer.write(buildMessage(3, "d"), null);

            writer.doBulkWrite();

            assertThat(recordsSend.isPresent(), is(true));
            assertThat(recordsSend.get().getCount(), is(3L));
        }
    }

    @Test
    void testCurrentSendTime() throws Exception {
        final String collection = "test-current-send-time";
        boolean flushOnCheckpoint = false;
        final int bulkFlushMaxActions = 2;
        final int bulkFlushInterval = -1;

        try (final MongoWriter<Document> writer =
                createWriter(
                        collection, bulkFlushMaxActions, bulkFlushInterval, flushOnCheckpoint)) {
            final Optional<Gauge<Long>> currentSendTime =
                    metricListener.getGauge("currentSendTime");

            writer.write(buildMessage(1), null);
            writer.write(buildMessage(2), null);

            writer.doBulkWrite();

            assertThat(currentSendTime.isPresent(), is(true));
            assertThat(currentSendTime.get().getValue(), greaterThan(0L));
        }
    }

    @Test
    void testSinkContext() throws Exception {
        final String collection = "test-sink-context";
        boolean flushOnCheckpoint = false;
        final int bulkFlushMaxActions = 2;
        final int bulkFlushInterval = -1;
        final int sinkParallelism = 1;

        MongoWriteOptions expectOptions =
                MongoWriteOptions.builder()
                        .setBulkFlushMaxActions(bulkFlushMaxActions)
                        .setBulkFlushIntervalMs(bulkFlushInterval)
                        .setMaxRetryTimes(0)
                        .setParallelism(sinkParallelism)
                        .build();

        Sink.InitContext initContext = new MockInitContext(metricListener);

        MongoSerializationSchema<Document> testSerializationSchema =
                (element, context) -> {
                    assertThat(context.getParallelInstanceId(), equalTo(0));
                    assertThat(context.getNumberOfParallelInstances(), equalTo(1));
                    assertThat(context.getWriteOptions(), equalTo(expectOptions));
                    assertThat(
                            context.processTime(),
                            equalTo(
                                    initContext
                                            .getProcessingTimeService()
                                            .getCurrentProcessingTime()));
                    return new InsertOneModel<>(element.toBsonDocument());
                };

        try (MongoWriter<Document> writer =
                createWriter(
                        collection,
                        bulkFlushMaxActions,
                        bulkFlushInterval,
                        flushOnCheckpoint,
                        sinkParallelism,
                        initContext,
                        testSerializationSchema)) {
            writer.write(buildMessage(1), null);
            writer.write(buildMessage(2), null);

            writer.doBulkWrite();
        }
    }

    private MongoCollection<Document> collectionOf(String collection) {
        return mongoClient.getDatabase(TEST_DATABASE).getCollection(collection);
    }

    private MongoWriter<Document> createWriter(
            String collection,
            int bulkFlushMaxActions,
            long bulkFlushInterval,
            boolean flushOnCheckpoint) {
        return createWriter(
                collection,
                bulkFlushMaxActions,
                bulkFlushInterval,
                flushOnCheckpoint,
                DEFAULT_SINK_PARALLELISM,
                new MockInitContext(metricListener),
                new UpsertSerializationSchema());
    }

    private MongoWriter<Document> createWriter(
            String collection,
            int bulkFlushMaxActions,
            long bulkFlushInterval,
            boolean flushOnCheckpoint,
            @Nullable Integer parallelism,
            Sink.InitContext initContext,
            MongoSerializationSchema<Document> serializationSchema) {

        MongoConnectionOptions connectionOptions =
                MongoConnectionOptions.builder()
                        .setUri(MONGO_CONTAINER.getConnectionString())
                        .setDatabase(TEST_DATABASE)
                        .setCollection(collection)
                        .build();

        MongoWriteOptions writeOptions =
                MongoWriteOptions.builder()
                        .setBulkFlushMaxActions(bulkFlushMaxActions)
                        .setBulkFlushIntervalMs(bulkFlushInterval)
                        .setMaxRetryTimes(0)
                        .setParallelism(parallelism)
                        .build();

        return new MongoWriter<>(
                connectionOptions,
                writeOptions,
                flushOnCheckpoint,
                initContext,
                serializationSchema);
    }

    private Document buildMessage(int id) {
        return buildMessage(id, "i");
    }

    private Document buildMessage(int id, String op) {
        return new Document("_id", id).append("op", op);
    }

    private static class UpsertSerializationSchema implements MongoSerializationSchema<Document> {

        @Override
        public WriteModel<BsonDocument> serialize(Document element, MongoSinkContext sinkContext) {
            String operation = element.getString("op");
            switch (operation) {
                case "i":
                    return new InsertOneModel<>(element.toBsonDocument());
                case "u":
                    {
                        BsonDocument document = element.toBsonDocument();
                        BsonDocument filter = new BsonDocument("_id", document.getInt32("_id"));
                        // _id is immutable so we remove it here to prevent exception.
                        document.remove("_id");
                        BsonDocument update = new BsonDocument("$set", document);
                        return new UpdateOneModel<>(
                                filter, update, new UpdateOptions().upsert(true));
                    }
                case "d":
                    {
                        BsonDocument document = element.toBsonDocument();
                        BsonDocument filter = new BsonDocument("_id", document.getInt32("_id"));
                        return new DeleteOneModel<>(filter);
                    }
                default:
                    throw new UnsupportedOperationException("op is not supported " + operation);
            }
        }
    }

    private static class MockInitContext implements Sink.InitContext {

        private final OperatorIOMetricGroup ioMetricGroup;
        private final SinkWriterMetricGroup metricGroup;
        private final ProcessingTimeService timeService;

        private MockInitContext(MetricListener metricListener) {
            this.ioMetricGroup =
                    UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup()
                            .getIOMetricGroup();
            MetricGroup metricGroup = metricListener.getMetricGroup();
            this.metricGroup = InternalSinkWriterMetricGroup.mock(metricGroup, ioMetricGroup);
            this.timeService = new TestProcessingTimeService();
        }

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public MailboxExecutor getMailboxExecutor() {
            return new SyncMailboxExecutor();
        }

        @Override
        public ProcessingTimeService getProcessingTimeService() {
            return timeService;
        }

        @Override
        public int getSubtaskId() {
            return 0;
        }

        @Override
        public int getNumberOfParallelSubtasks() {
            return 1;
        }

        @Override
        public SinkWriterMetricGroup metricGroup() {
            return metricGroup;
        }

        @Override
        public OptionalLong getRestoredCheckpointId() {
            return OptionalLong.empty();
        }

        @Override
        public SerializationSchema.InitializationContext
                asSerializationSchemaInitializationContext() {
            return new SerializationSchema.InitializationContext() {
                @Override
                public MetricGroup getMetricGroup() {
                    return metricGroup;
                }

                @Override
                public UserCodeClassLoader getUserCodeClassLoader() {
                    return null;
                }
            };
        }
    }
}
