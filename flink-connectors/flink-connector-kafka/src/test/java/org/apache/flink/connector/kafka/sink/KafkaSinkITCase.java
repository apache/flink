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

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.testutils.KafkaSinkExternalContextFactory;
import org.apache.flink.connector.kafka.testutils.KafkaUtil;
import org.apache.flink.connector.testframe.environment.MiniClusterTestEnvironment;
import org.apache.flink.connector.testframe.external.DefaultContainerizedExternalSystem;
import org.apache.flink.connector.testframe.junit.annotations.TestContext;
import org.apache.flink.connector.testframe.junit.annotations.TestEnv;
import org.apache.flink.connector.testframe.junit.annotations.TestExternalSystem;
import org.apache.flink.connector.testframe.junit.annotations.TestSemantics;
import org.apache.flink.connector.testframe.testsuites.SinkTestSuiteBase;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.util.TestUtils;
import org.apache.flink.testutils.junit.SharedObjects;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.util.DockerImageVersions;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava30.com.google.common.base.Joiner;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Nested;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.apache.flink.connector.kafka.testutils.KafkaUtil.createKafkaContainer;
import static org.apache.flink.util.DockerImageVersions.KAFKA;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for using KafkaSink writing to a Kafka cluster. */
public class KafkaSinkITCase extends TestLogger {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSinkITCase.class);
    private static final String INTER_CONTAINER_KAFKA_ALIAS = "kafka";
    private static final Network NETWORK = Network.newNetwork();
    private static final int ZK_TIMEOUT_MILLIS = 30000;
    private static final short TOPIC_REPLICATION_FACTOR = 1;
    private static AdminClient admin;

    private String topic;
    private SharedReference<AtomicLong> emittedRecordsCount;
    private SharedReference<AtomicLong> emittedRecordsWithCheckpoint;
    private SharedReference<AtomicBoolean> failed;
    private SharedReference<AtomicLong> lastCheckpointedRecord;

    @ClassRule
    public static final KafkaContainer KAFKA_CONTAINER =
            createKafkaContainer(KAFKA, LOG)
                    .withEmbeddedZookeeper()
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_KAFKA_ALIAS);

    @Rule public final SharedObjects sharedObjects = SharedObjects.create();

    @Rule public final TemporaryFolder temp = new TemporaryFolder();

    @BeforeClass
    public static void setupAdmin() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA_CONTAINER.getBootstrapServers());
        admin = AdminClient.create(properties);
    }

    @AfterClass
    public static void teardownAdmin() {
        admin.close();
    }

    @Before
    public void setUp() throws ExecutionException, InterruptedException, TimeoutException {
        emittedRecordsCount = sharedObjects.add(new AtomicLong());
        emittedRecordsWithCheckpoint = sharedObjects.add(new AtomicLong());
        failed = sharedObjects.add(new AtomicBoolean(false));
        lastCheckpointedRecord = sharedObjects.add(new AtomicLong(0));
        topic = UUID.randomUUID().toString();
        createTestTopic(topic, 1, TOPIC_REPLICATION_FACTOR);
    }

    @After
    public void tearDown() throws ExecutionException, InterruptedException, TimeoutException {
        deleteTestTopic(topic);
    }

    /** Integration test based on connector testing framework. */
    @Nested
    class IntegrationTests extends SinkTestSuiteBase<String> {
        // Defines test environment on Flink MiniCluster
        @SuppressWarnings("unused")
        @TestEnv
        MiniClusterTestEnvironment flink = new MiniClusterTestEnvironment();

        // Defines external system
        @TestExternalSystem
        DefaultContainerizedExternalSystem<KafkaContainer> kafka =
                DefaultContainerizedExternalSystem.builder()
                        .fromContainer(
                                new KafkaContainer(
                                        DockerImageName.parse(DockerImageVersions.KAFKA)))
                        .build();

        @SuppressWarnings("unused")
        @TestSemantics
        CheckpointingMode[] semantics =
                new CheckpointingMode[] {
                    CheckpointingMode.EXACTLY_ONCE, CheckpointingMode.AT_LEAST_ONCE
                };

        @SuppressWarnings("unused")
        @TestContext
        KafkaSinkExternalContextFactory sinkContext =
                new KafkaSinkExternalContextFactory(kafka.getContainer(), Collections.emptyList());
    }

    @Test
    public void testWriteRecordsToKafkaWithAtLeastOnceGuarantee() throws Exception {
        writeRecordsToKafka(DeliveryGuarantee.AT_LEAST_ONCE, emittedRecordsCount);
    }

    @Test
    public void testWriteRecordsToKafkaWithNoneGuarantee() throws Exception {
        writeRecordsToKafka(DeliveryGuarantee.NONE, emittedRecordsCount);
    }

    @Test
    public void testWriteRecordsToKafkaWithExactlyOnceGuarantee() throws Exception {
        writeRecordsToKafka(DeliveryGuarantee.EXACTLY_ONCE, emittedRecordsWithCheckpoint);
    }

    @Test
    public void testRecoveryWithAtLeastOnceGuarantee() throws Exception {
        testRecoveryWithAssertion(
                DeliveryGuarantee.AT_LEAST_ONCE,
                1,
                (records) ->
                        assertThat(records, hasItems(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L)));
    }

    @Test
    public void testRecoveryWithExactlyOnceGuarantee() throws Exception {
        testRecoveryWithAssertion(
                DeliveryGuarantee.EXACTLY_ONCE,
                1,
                (records) ->
                        assertThat(
                                records,
                                contains(
                                        LongStream.range(1, lastCheckpointedRecord.get().get() + 1)
                                                .boxed()
                                                .toArray())));
    }

    @Test
    public void testRecoveryWithExactlyOnceGuaranteeAndConcurrentCheckpoints() throws Exception {
        testRecoveryWithAssertion(
                DeliveryGuarantee.EXACTLY_ONCE,
                2,
                (records) ->
                        assertThat(
                                records,
                                contains(
                                        LongStream.range(1, lastCheckpointedRecord.get().get() + 1)
                                                .boxed()
                                                .toArray())));
    }

    @Test
    public void testAbortTransactionsOfPendingCheckpointsAfterFailure() throws Exception {
        // Run a first job failing during the async phase of a checkpoint to leave some
        // lingering transactions
        final Configuration config = new Configuration();
        config.setString(StateBackendOptions.STATE_BACKEND, "filesystem");
        final File checkpointDir = temp.newFolder();
        config.setString(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toURI().toString());
        config.set(
                ExecutionCheckpointingOptions.EXTERNALIZED_CHECKPOINT,
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        config.set(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 2);
        try {
            executeWithMapper(new FailAsyncCheckpointMapper(1), config, "firstPrefix");
        } catch (Exception e) {
            assertThat(
                    e.getCause().getCause().getMessage(),
                    containsString("Exceeded checkpoint tolerable failure"));
        }
        final File completedCheckpoint = TestUtils.getMostRecentCompletedCheckpoint(checkpointDir);

        config.set(SavepointConfigOptions.SAVEPOINT_PATH, completedCheckpoint.toURI().toString());

        // Run a second job which aborts all lingering transactions and new consumer should
        // immediately see the newly written records
        failed.get().set(true);
        executeWithMapper(
                new FailingCheckpointMapper(failed, lastCheckpointedRecord), config, "newPrefix");
        final List<ConsumerRecord<byte[], byte[]>> collectedRecords =
                drainAllRecordsFromTopic(topic, true);
        assertThat(
                deserializeValues(collectedRecords),
                contains(
                        LongStream.range(1, lastCheckpointedRecord.get().get() + 1)
                                .boxed()
                                .toArray()));
    }

    @Test
    public void testAbortTransactionsAfterScaleInBeforeFirstCheckpoint() throws Exception {
        // Run a first job opening 5 transactions one per subtask and fail in async checkpoint phase
        final Configuration config = new Configuration();
        config.set(CoreOptions.DEFAULT_PARALLELISM, 5);
        try {
            executeWithMapper(new FailAsyncCheckpointMapper(0), config, null);
        } catch (Exception e) {
            assertThat(
                    e.getCause().getCause().getMessage(),
                    containsString("Exceeded checkpoint tolerable failure"));
        }
        assertTrue(deserializeValues(drainAllRecordsFromTopic(topic, true)).isEmpty());

        // Second job aborts all transactions from previous runs with higher parallelism
        config.set(CoreOptions.DEFAULT_PARALLELISM, 1);
        failed.get().set(true);
        executeWithMapper(
                new FailingCheckpointMapper(failed, lastCheckpointedRecord), config, null);
        final List<ConsumerRecord<byte[], byte[]>> collectedRecords =
                drainAllRecordsFromTopic(topic, true);
        assertThat(
                deserializeValues(collectedRecords),
                contains(
                        LongStream.range(1, lastCheckpointedRecord.get().get() + 1)
                                .boxed()
                                .toArray()));
    }

    private void executeWithMapper(
            MapFunction<Long, Long> mapper,
            Configuration config,
            @Nullable String transactionalIdPrefix)
            throws Exception {
        final StreamExecutionEnvironment env = new LocalStreamEnvironment(config);
        env.enableCheckpointing(100L);
        env.setRestartStrategy(RestartStrategies.noRestart());
        final DataStreamSource<Long> source = env.fromSequence(1, 10);
        final DataStream<Long> stream = source.map(mapper);
        final KafkaSinkBuilder<Long> builder =
                new KafkaSinkBuilder<Long>()
                        .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .setBootstrapServers(KAFKA_CONTAINER.getBootstrapServers())
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic(topic)
                                        .setValueSerializationSchema(new RecordSerializer())
                                        .build());
        if (transactionalIdPrefix == null) {
            transactionalIdPrefix = "kafka-sink";
        }
        builder.setTransactionalIdPrefix(transactionalIdPrefix);
        stream.sinkTo(builder.build());
        env.execute();
        checkProducerLeak();
    }

    private void testRecoveryWithAssertion(
            DeliveryGuarantee guarantee,
            int maxConcurrentCheckpoints,
            java.util.function.Consumer<List<Long>> recordsAssertion)
            throws Exception {
        final StreamExecutionEnvironment env = new LocalStreamEnvironment();
        env.enableCheckpointing(300L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(maxConcurrentCheckpoints);
        DataStreamSource<Long> source = env.fromSequence(1, 10);
        DataStream<Long> stream =
                source.map(new FailingCheckpointMapper(failed, lastCheckpointedRecord));

        stream.sinkTo(
                new KafkaSinkBuilder<Long>()
                        .setDeliverGuarantee(guarantee)
                        .setBootstrapServers(KAFKA_CONTAINER.getBootstrapServers())
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic(topic)
                                        .setValueSerializationSchema(new RecordSerializer())
                                        .build())
                        .setTransactionalIdPrefix("kafka-sink")
                        .build());
        env.execute();

        final List<ConsumerRecord<byte[], byte[]>> collectedRecords =
                drainAllRecordsFromTopic(topic, guarantee == DeliveryGuarantee.EXACTLY_ONCE);
        recordsAssertion.accept(deserializeValues(collectedRecords));
        checkProducerLeak();
    }

    private void writeRecordsToKafka(
            DeliveryGuarantee deliveryGuarantee, SharedReference<AtomicLong> expectedRecords)
            throws Exception {
        final StreamExecutionEnvironment env = new LocalStreamEnvironment();
        env.enableCheckpointing(100L);
        final DataStream<Long> source =
                env.addSource(
                        new InfiniteIntegerSource(
                                emittedRecordsCount, emittedRecordsWithCheckpoint));
        source.sinkTo(
                new KafkaSinkBuilder<Long>()
                        .setBootstrapServers(KAFKA_CONTAINER.getBootstrapServers())
                        .setDeliverGuarantee(deliveryGuarantee)
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic(topic)
                                        .setValueSerializationSchema(new RecordSerializer())
                                        .build())
                        .setTransactionalIdPrefix("kafka-sink")
                        .build());
        env.execute();

        final List<ConsumerRecord<byte[], byte[]>> collectedRecords =
                drainAllRecordsFromTopic(
                        topic, deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE);
        final long recordsCount = expectedRecords.get().get();
        assertEquals(collectedRecords.size(), recordsCount);
        assertThat(
                deserializeValues(collectedRecords),
                contains(LongStream.range(1, recordsCount + 1).boxed().toArray()));
        checkProducerLeak();
    }

    private static List<Long> deserializeValues(List<ConsumerRecord<byte[], byte[]>> records) {
        return records.stream()
                .map(
                        record -> {
                            final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
                            final byte[] value = record.value();
                            buffer.put(value, 0, value.length);
                            buffer.flip();
                            return buffer.getLong();
                        })
                .collect(Collectors.toList());
    }

    private static Properties getKafkaClientConfiguration() {
        final Properties standardProps = new Properties();
        standardProps.put("bootstrap.servers", KAFKA_CONTAINER.getBootstrapServers());
        standardProps.put("group.id", UUID.randomUUID().toString());
        standardProps.put("enable.auto.commit", false);
        standardProps.put("auto.offset.reset", "earliest");
        standardProps.put("max.partition.fetch.bytes", 256);
        standardProps.put("zookeeper.session.timeout.ms", ZK_TIMEOUT_MILLIS);
        standardProps.put("zookeeper.connection.timeout.ms", ZK_TIMEOUT_MILLIS);
        return standardProps;
    }

    private static Consumer<byte[], byte[]> createTestConsumer(
            String topic, Properties properties) {
        final Properties consumerConfig = new Properties();
        consumerConfig.putAll(properties);
        consumerConfig.put("key.deserializer", ByteArrayDeserializer.class.getName());
        consumerConfig.put("value.deserializer", ByteArrayDeserializer.class.getName());
        consumerConfig.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        final KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<>(consumerConfig);
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        return kafkaConsumer;
    }

    private void createTestTopic(String topic, int numPartitions, short replicationFactor)
            throws ExecutionException, InterruptedException, TimeoutException {
        final CreateTopicsResult result =
                admin.createTopics(
                        Collections.singletonList(
                                new NewTopic(topic, numPartitions, replicationFactor)));
        result.all().get(1, TimeUnit.MINUTES);
    }

    private void deleteTestTopic(String topic)
            throws ExecutionException, InterruptedException, TimeoutException {
        final DeleteTopicsResult result = admin.deleteTopics(Collections.singletonList(topic));
        result.all().get(1, TimeUnit.MINUTES);
    }

    private List<ConsumerRecord<byte[], byte[]>> drainAllRecordsFromTopic(
            String topic, boolean committed) {
        Properties properties = getKafkaClientConfiguration();
        return KafkaUtil.drainAllRecordsFromTopic(topic, properties, committed);
    }

    private static class RecordSerializer implements SerializationSchema<Long> {

        @Override
        public byte[] serialize(Long element) {
            final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            buffer.putLong(element);
            return buffer.array();
        }
    }

    private static class FailAsyncCheckpointMapper
            implements MapFunction<Long, Long>, CheckpointedFunction {
        private static final ListStateDescriptor<Integer> stateDescriptor =
                new ListStateDescriptor<>("test-state", new SlowSerializer());
        private int failAfterCheckpoint;

        private ListState<Integer> state;

        public FailAsyncCheckpointMapper(int failAfterCheckpoint) {
            this.failAfterCheckpoint = failAfterCheckpoint;
        }

        @Override
        public Long map(Long value) throws Exception {
            Thread.sleep(100);
            return value;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            state.clear();
            if (failAfterCheckpoint <= 0) {
                // Trigger a failure in the serializer
                state.add(-1);
            } else {
                state.add(1);
            }
            failAfterCheckpoint--;
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            state = context.getOperatorStateStore().getListState(stateDescriptor);
        }
    }

    private static class SlowSerializer extends TypeSerializerSingleton<Integer> {

        @Override
        public boolean isImmutableType() {
            return false;
        }

        @Override
        public Integer createInstance() {
            return 1;
        }

        @Override
        public Integer copy(Integer from) {
            return from;
        }

        @Override
        public Integer copy(Integer from, Integer reuse) {
            return from;
        }

        @Override
        public int getLength() {
            return 0;
        }

        @Override
        public void serialize(Integer record, DataOutputView target) throws IOException {
            if (record != -1) {
                return;
            }
            throw new RuntimeException("Expected failure during async checkpoint phase");
        }

        @Override
        public Integer deserialize(DataInputView source) throws IOException {
            return 1;
        }

        @Override
        public Integer deserialize(Integer reuse, DataInputView source) throws IOException {
            return 1;
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {}

        @Override
        public TypeSerializerSnapshot<Integer> snapshotConfiguration() {
            return new SlowSerializerSnapshot();
        }
    }

    /** Snapshot used in {@link FailAsyncCheckpointMapper}. */
    public static class SlowSerializerSnapshot extends SimpleTypeSerializerSnapshot<Integer> {
        public SlowSerializerSnapshot() {
            super(SlowSerializer::new);
        }
    }

    /** Fails after a checkpoint is taken and the next record was emitted. */
    private static class FailingCheckpointMapper
            implements MapFunction<Long, Long>, CheckpointListener, CheckpointedFunction {

        private final SharedReference<AtomicBoolean> failed;
        private final SharedReference<AtomicLong> lastCheckpointedRecord;

        private volatile long lastSeenRecord;
        private volatile long checkpointedRecord;
        private volatile long lastCheckpointId = 0;
        private final AtomicInteger emittedBetweenCheckpoint = new AtomicInteger(0);

        FailingCheckpointMapper(
                SharedReference<AtomicBoolean> failed,
                SharedReference<AtomicLong> lastCheckpointedRecord) {
            this.failed = failed;
            this.lastCheckpointedRecord = lastCheckpointedRecord;
        }

        @Override
        public Long map(Long value) throws Exception {
            lastSeenRecord = value;
            if (lastCheckpointId >= 1
                    && emittedBetweenCheckpoint.get() > 0
                    && !failed.get().get()) {
                failed.get().set(true);
                throw new RuntimeException("Planned exception.");
            }
            // Delay execution to ensure that at-least one checkpoint is triggered before finish
            Thread.sleep(50);
            emittedBetweenCheckpoint.incrementAndGet();
            return value;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            LOG.info("notifyCheckpointComplete {} @ {}", checkpointedRecord, checkpointId);
            lastCheckpointId = checkpointId;
            emittedBetweenCheckpoint.set(0);
            lastCheckpointedRecord.get().set(checkpointedRecord);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            LOG.info("snapshotState {} @ {}", lastSeenRecord, context.getCheckpointId());
            checkpointedRecord = lastSeenRecord;
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {}
    }

    private void checkProducerLeak() throws InterruptedException {
        List<Map.Entry<Thread, StackTraceElement[]>> leaks = null;
        for (int tries = 0; tries < 10; tries++) {
            leaks =
                    Thread.getAllStackTraces().entrySet().stream()
                            .filter(this::findAliveKafkaThread)
                            .collect(Collectors.toList());
            if (leaks.isEmpty()) {
                return;
            }
            Thread.sleep(1000);
        }

        for (Map.Entry<Thread, StackTraceElement[]> leak : leaks) {
            leak.getKey().stop();
        }
        fail(
                "Detected producer leaks:\n"
                        + leaks.stream().map(this::format).collect(Collectors.joining("\n\n")));
    }

    private String format(Map.Entry<Thread, StackTraceElement[]> leak) {
        return leak.getKey().getName() + ":\n" + Joiner.on("\n").join(leak.getValue());
    }

    private boolean findAliveKafkaThread(Map.Entry<Thread, StackTraceElement[]> threadStackTrace) {
        return threadStackTrace.getKey().getState() != Thread.State.TERMINATED
                && threadStackTrace.getKey().getName().contains("kafka-producer-network-thread");
    }

    /**
     * Exposes information about how man records have been emitted overall and finishes after
     * receiving the checkpoint completed event.
     */
    private static final class InfiniteIntegerSource
            implements SourceFunction<Long>, CheckpointListener, CheckpointedFunction {

        private final SharedReference<AtomicLong> emittedRecordsCount;
        private final SharedReference<AtomicLong> emittedRecordsWithCheckpoint;

        private volatile boolean running = true;
        private volatile long temp;
        private Object lock;

        InfiniteIntegerSource(
                SharedReference<AtomicLong> emittedRecordsCount,
                SharedReference<AtomicLong> emittedRecordsWithCheckpoint) {
            this.emittedRecordsCount = emittedRecordsCount;
            this.emittedRecordsWithCheckpoint = emittedRecordsWithCheckpoint;
        }

        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            lock = ctx.getCheckpointLock();
            while (running) {
                synchronized (lock) {
                    ctx.collect(emittedRecordsCount.get().addAndGet(1));
                    Thread.sleep(1);
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            emittedRecordsWithCheckpoint.get().set(temp);
            running = false;
            LOG.info("notifyCheckpointCompleted {}", checkpointId);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            temp = emittedRecordsCount.get().get();
            LOG.info(
                    "snapshotState, {}, {}",
                    context.getCheckpointId(),
                    emittedRecordsCount.get().get());
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {}
    }
}
