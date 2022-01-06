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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.metrics.jmx.JMXReporter;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.testutils.junit.RetryOnFailure;
import org.apache.flink.testutils.junit.RetryRule;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TestLogger;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import scala.concurrent.duration.FiniteDuration;

import static org.junit.Assert.fail;

/**
 * The base for the Kafka tests. It brings up:
 *
 * <ul>
 *   <li>A ZooKeeper mini cluster
 *   <li>Three Kafka Brokers (mini clusters)
 *   <li>A Flink mini cluster
 * </ul>
 *
 * <p>Code in this test is based on the following GitHub repository: <a
 * href="https://github.com/sakserv/hadoop-mini-clusters">
 * https://github.com/sakserv/hadoop-mini-clusters</a> (ASL licensed), as per commit
 * <i>bc6b2b2d5f6424d5f377aa6c0871e82a956462ef</i>
 *
 * <p>Tests inheriting from this class are known to be unstable due to the test setup. All tests
 * implemented in subclasses will be retried on failures.
 */
@SuppressWarnings("serial")
@RetryOnFailure(times = 3)
public abstract class KafkaTestBase extends TestLogger {

    public static final Logger LOG = LoggerFactory.getLogger(KafkaTestBase.class);

    public static final int NUMBER_OF_KAFKA_SERVERS = 3;

    public static String brokerConnectionStrings;

    public static Properties standardProps;

    public static FiniteDuration timeout = new FiniteDuration(10, TimeUnit.SECONDS);

    public static KafkaTestEnvironment kafkaServer;

    @ClassRule public static TemporaryFolder tempFolder = new TemporaryFolder();

    public static Properties secureProps = new Properties();

    @Rule public final RetryRule retryRule = new RetryRule();

    // ------------------------------------------------------------------------
    //  Setup and teardown of the mini clusters
    // ------------------------------------------------------------------------

    @BeforeClass
    public static void prepare() throws Exception {
        LOG.info("-------------------------------------------------------------------------");
        LOG.info("    Starting KafkaTestBase ");
        LOG.info("-------------------------------------------------------------------------");

        startClusters(false);
    }

    @AfterClass
    public static void shutDownServices() throws Exception {

        LOG.info("-------------------------------------------------------------------------");
        LOG.info("    Shut down KafkaTestBase ");
        LOG.info("-------------------------------------------------------------------------");

        TestStreamEnvironment.unsetAsContext();

        shutdownClusters();

        LOG.info("-------------------------------------------------------------------------");
        LOG.info("    KafkaTestBase finished");
        LOG.info("-------------------------------------------------------------------------");
    }

    public static Configuration getFlinkConfiguration() {
        Configuration flinkConfig = new Configuration();
        flinkConfig.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("16m"));
        flinkConfig.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "my_reporter."
                        + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX,
                JMXReporter.class.getName());
        return flinkConfig;
    }

    public static void startClusters() throws Exception {
        startClusters(
                KafkaTestEnvironment.createConfig().setKafkaServersNumber(NUMBER_OF_KAFKA_SERVERS));
    }

    public static void startClusters(boolean secureMode) throws Exception {
        startClusters(
                KafkaTestEnvironment.createConfig()
                        .setKafkaServersNumber(NUMBER_OF_KAFKA_SERVERS)
                        .setSecureMode(secureMode));
    }

    public static void startClusters(KafkaTestEnvironment.Config environmentConfig)
            throws Exception {
        kafkaServer = constructKafkaTestEnvionment();

        LOG.info("Starting KafkaTestBase.prepare() for Kafka " + kafkaServer.getVersion());

        kafkaServer.prepare(environmentConfig);

        standardProps = kafkaServer.getStandardProperties();

        brokerConnectionStrings = kafkaServer.getBrokerConnectionString();

        if (environmentConfig.isSecureMode()) {
            if (!kafkaServer.isSecureRunSupported()) {
                throw new IllegalStateException(
                        "Attempting to test in secure mode but secure mode not supported by the KafkaTestEnvironment.");
            }
            secureProps = kafkaServer.getSecureProperties();
        }
    }

    public static KafkaTestEnvironment constructKafkaTestEnvionment() throws Exception {
        Class<?> clazz =
                Class.forName(
                        "org.apache.flink.streaming.connectors.kafka.KafkaTestEnvironmentImpl");
        return (KafkaTestEnvironment) InstantiationUtil.instantiate(clazz);
    }

    public static void shutdownClusters() throws Exception {
        if (secureProps != null) {
            secureProps.clear();
        }

        if (kafkaServer != null) {
            kafkaServer.shutdown();
        }
    }

    // ------------------------------------------------------------------------
    //  Execution utilities
    // ------------------------------------------------------------------------

    public static void tryExecutePropagateExceptions(StreamExecutionEnvironment see, String name)
            throws Exception {
        try {
            see.execute(name);
        } catch (ProgramInvocationException | JobExecutionException root) {
            Throwable cause = root.getCause();

            // search for nested SuccessExceptions
            int depth = 0;
            while (!(cause instanceof SuccessException)) {
                if (cause == null || depth++ == 20) {
                    throw root;
                } else {
                    cause = cause.getCause();
                }
            }
        }
    }

    public static void createTestTopic(
            String topic, int numberOfPartitions, int replicationFactor) {
        kafkaServer.createTestTopic(topic, numberOfPartitions, replicationFactor);
    }

    public static void deleteTestTopic(String topic) {
        kafkaServer.deleteTestTopic(topic);
    }

    public static <K, V> void produceToKafka(
            Collection<ProducerRecord<K, V>> records,
            Class<? extends org.apache.kafka.common.serialization.Serializer<K>> keySerializerClass,
            Class<? extends org.apache.kafka.common.serialization.Serializer<V>>
                    valueSerializerClass)
            throws Throwable {
        Properties props = new Properties();
        props.putAll(standardProps);
        props.putAll(kafkaServer.getIdempotentProducerConfig());
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass.getName());
        props.setProperty(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass.getName());

        AtomicReference<Throwable> sendingError = new AtomicReference<>();
        Callback callback =
                (metadata, exception) -> {
                    if (exception != null) {
                        if (!sendingError.compareAndSet(null, exception)) {
                            sendingError.get().addSuppressed(exception);
                        }
                    }
                };
        try (KafkaProducer<K, V> producer = new KafkaProducer<>(props)) {
            for (ProducerRecord<K, V> record : records) {
                producer.send(record, callback);
            }
        }
        if (sendingError.get() != null) {
            throw sendingError.get();
        }
    }

    /**
     * We manually handle the timeout instead of using JUnit's timeout to return failure instead of
     * timeout error. After timeout we assume that there are missing records and there is a bug, not
     * that the test has run out of time.
     */
    public void assertAtLeastOnceForTopic(
            Properties properties,
            String topic,
            int partition,
            Set<Integer> expectedElements,
            long timeoutMillis)
            throws Exception {

        long startMillis = System.currentTimeMillis();
        Set<Integer> actualElements = new HashSet<>();

        // until we timeout...
        while (System.currentTimeMillis() < startMillis + timeoutMillis) {
            properties.put(
                    "key.deserializer",
                    "org.apache.kafka.common.serialization.IntegerDeserializer");
            properties.put(
                    "value.deserializer",
                    "org.apache.kafka.common.serialization.IntegerDeserializer");
            // We need to set these two properties so that they are lower than request.timeout.ms.
            // This is
            // required for some old KafkaConsumer versions.
            properties.put("session.timeout.ms", "2000");
            properties.put("heartbeat.interval.ms", "500");

            // query kafka for new records ...
            Collection<ConsumerRecord<Integer, Integer>> records =
                    kafkaServer.getAllRecordsFromTopic(properties, topic);

            for (ConsumerRecord<Integer, Integer> record : records) {
                actualElements.add(record.value());
            }

            // succeed if we got all expectedElements
            if (actualElements.containsAll(expectedElements)) {
                return;
            }
        }

        fail(
                String.format(
                        "Expected to contain all of: <%s>, but was: <%s>",
                        expectedElements, actualElements));
    }

    public void assertExactlyOnceForTopic(
            Properties properties, String topic, List<Integer> expectedElements) {

        List<Integer> actualElements = new ArrayList<>();

        Properties consumerProperties = new Properties();
        consumerProperties.putAll(properties);
        consumerProperties.put(
                "key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        consumerProperties.put(
                "value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        consumerProperties.put("isolation.level", "read_committed");

        // query kafka for new records ...
        Collection<ConsumerRecord<byte[], byte[]>> records =
                kafkaServer.getAllRecordsFromTopic(consumerProperties, topic);

        for (ConsumerRecord<byte[], byte[]> record : records) {
            actualElements.add(ByteBuffer.wrap(record.value()).getInt());
        }

        // succeed if we got all expectedElements
        if (actualElements.equals(expectedElements)) {
            return;
        }

        fail(
                String.format(
                        "Expected %s, but was: %s",
                        formatElements(expectedElements), formatElements(actualElements)));
    }

    private String formatElements(List<Integer> elements) {
        if (elements.size() > 50) {
            return String.format("number of elements: <%s>", elements.size());
        } else {
            return String.format("elements: <%s>", elements);
        }
    }
}
