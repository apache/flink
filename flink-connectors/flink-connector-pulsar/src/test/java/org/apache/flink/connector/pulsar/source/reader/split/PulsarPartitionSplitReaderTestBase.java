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

package org.apache.flink.connector.pulsar.source.reader.split;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.reader.message.PulsarMessage;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.connector.pulsar.testutils.PulsarTestSuiteBase;
import org.apache.flink.connector.pulsar.testutils.extension.TestOrderlinessExtension;
import org.apache.flink.connectors.test.common.junit.extensions.TestLoggerExtension;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static java.time.Duration.ofSeconds;
import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.flink.connector.pulsar.common.utils.PulsarExceptionUtils.sneakyAdmin;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_ENABLE_AUTO_ACKNOWLEDGE_MESSAGE;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_MAX_FETCH_RECORDS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_MAX_FETCH_TIME;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_NAME;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.topicNameWithPartition;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange.createFullRange;
import static org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema.flinkSchema;
import static org.apache.flink.connector.pulsar.testutils.extension.TestOrderlinessExtension.PULSAR_SOURCE_READER_SUBSCRIPTION_TYPE_STORE_KEY;
import static org.apache.flink.connector.pulsar.testutils.extension.TestOrderlinessExtension.PULSAR_TEST_RESOURCE_NAMESPACE;
import static org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntimeOperator.DEFAULT_PARTITIONS;
import static org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntimeOperator.NUM_RECORDS_PER_PARTITION;
import static org.apache.flink.core.testutils.CommonTestUtils.waitUtil;
import static org.apache.flink.shaded.guava30.com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.pulsar.client.api.Schema.STRING;
import static org.assertj.core.api.Assertions.assertThat;

/** Test utils for split readers. */
@ExtendWith({
    TestOrderlinessExtension.class,
    TestLoggerExtension.class,
})
public abstract class PulsarPartitionSplitReaderTestBase extends PulsarTestSuiteBase {

    @RegisterExtension
    PulsarSplitReaderInvocationContextProvider provider =
            new PulsarSplitReaderInvocationContextProvider();

    protected Configuration readerConfig() {
        Configuration config = operator().config();
        config.set(PULSAR_MAX_FETCH_RECORDS, 1);
        config.set(PULSAR_MAX_FETCH_TIME, 1000L);
        config.set(PULSAR_SUBSCRIPTION_NAME, randomAlphabetic(10));
        config.set(PULSAR_ENABLE_AUTO_ACKNOWLEDGE_MESSAGE, true);

        return config;
    }

    protected SourceConfiguration sourceConfig(Configuration config) {
        return new SourceConfiguration(config);
    }

    protected void handleSplit(
            PulsarPartitionSplitReaderBase<String> reader, String topicName, int partitionId) {
        handleSplit(reader, topicName, partitionId, null);
    }

    protected void handleSplit(
            PulsarPartitionSplitReaderBase<String> reader,
            String topicName,
            int partitionId,
            MessageId startPosition) {
        TopicPartition partition = new TopicPartition(topicName, partitionId, createFullRange());
        PulsarPartitionSplit split =
                new PulsarPartitionSplit(partition, StopCursor.never(), startPosition, null);
        SplitsAddition<PulsarPartitionSplit> addition = new SplitsAddition<>(singletonList(split));
        reader.handleSplitsChanges(addition);
    }

    private void seekStartPositionAndHandleSplit(
            PulsarPartitionSplitReaderBase<String> reader, String topicName, int partitionId) {
        seekStartPositionAndHandleSplit(reader, topicName, partitionId, MessageId.latest);
    }

    private void seekStartPositionAndHandleSplit(
            PulsarPartitionSplitReaderBase<String> reader,
            String topicName,
            int partitionId,
            MessageId startPosition) {
        TopicPartition partition = new TopicPartition(topicName, partitionId, createFullRange());
        PulsarPartitionSplit split =
                new PulsarPartitionSplit(partition, StopCursor.never(), null, null);
        SplitsAddition<PulsarPartitionSplit> addition = new SplitsAddition<>(singletonList(split));

        // Create the subscription and set the start position for this reader.
        // Remember not to use Consumer.seek(startPosition)
        SourceConfiguration sourceConfiguration = reader.sourceConfiguration;
        PulsarAdmin pulsarAdmin = reader.pulsarAdmin;
        String subscriptionName = sourceConfiguration.getSubscriptionName();
        List<String> subscriptions =
                sneakyAdmin(() -> pulsarAdmin.topics().getSubscriptions(topicName));
        if (!subscriptions.contains(subscriptionName)) {
            // If this subscription is not available. Just create it.
            sneakyAdmin(
                    () ->
                            pulsarAdmin
                                    .topics()
                                    .createSubscription(
                                            topicName, subscriptionName, startPosition));
        } else {
            // Reset the subscription if this is existed.
            sneakyAdmin(
                    () ->
                            pulsarAdmin
                                    .topics()
                                    .resetCursor(topicName, subscriptionName, startPosition));
        }

        // Accept the split and start consuming.
        reader.handleSplitsChanges(addition);
    }

    private <T> PulsarMessage<T> fetchedMessage(PulsarPartitionSplitReaderBase<T> splitReader) {
        return fetchedMessages(splitReader, 1, false).stream().findFirst().orElse(null);
    }

    protected <T> List<PulsarMessage<T>> fetchedMessages(
            PulsarPartitionSplitReaderBase<T> splitReader, int expectedCount, boolean verify) {
        return fetchedMessages(
                splitReader, expectedCount, verify, Boundedness.CONTINUOUS_UNBOUNDED);
    }

    private <T> List<PulsarMessage<T>> fetchedMessages(
            PulsarPartitionSplitReaderBase<T> splitReader,
            int expectedCount,
            boolean verify,
            Boundedness boundedness) {
        List<PulsarMessage<T>> messages = new ArrayList<>(expectedCount);
        List<String> finishedSplits = new ArrayList<>();
        for (int i = 0; i < 3; ) {
            try {
                RecordsWithSplitIds<PulsarMessage<T>> recordsBySplitIds = splitReader.fetch();
                if (recordsBySplitIds.nextSplit() != null) {
                    // Collect the records in this split.
                    PulsarMessage<T> record;
                    while ((record = recordsBySplitIds.nextRecordFromSplit()) != null) {
                        messages.add(record);
                    }
                    finishedSplits.addAll(recordsBySplitIds.finishedSplits());
                } else {
                    i++;
                }
            } catch (IOException e) {
                i++;
            }
            sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
        if (verify) {
            assertThat(messages).as("We should fetch the expected size").hasSize(expectedCount);
            if (boundedness == Boundedness.CONTINUOUS_UNBOUNDED) {
                assertThat(finishedSplits).as("Split should not be marked as finished").isEmpty();
            } else {
                assertThat(finishedSplits).as("Split should be marked as finished").hasSize(1);
            }
        }

        return messages;
    }

    @TestTemplate
    void pollMessageAfterTimeout(PulsarPartitionSplitReaderBase<String> splitReader)
            throws InterruptedException, TimeoutException {
        String topicName = randomAlphabetic(10);

        // Add a split
        handleSplit(splitReader, topicName, 0, MessageId.latest);

        // Poll once with a null message
        PulsarMessage<String> message1 = fetchedMessage(splitReader);
        assertThat(message1).isNull();

        // Send a message to pulsar
        String topic = topicNameWithPartition(topicName, 0);
        operator().sendMessage(topic, STRING, randomAlphabetic(10));

        // Poll this message again
        waitUtil(
                () -> {
                    PulsarMessage<String> message2 = fetchedMessage(splitReader);
                    return message2 != null;
                },
                ofSeconds(Integer.MAX_VALUE),
                "Couldn't poll message from Pulsar.");
    }

    @TestTemplate
    void consumeMessageCreatedAfterHandleSplitChangesAndFetch(
            PulsarPartitionSplitReaderBase<String> splitReader) {
        String topicName = randomAlphabetic(10);
        handleSplit(splitReader, topicName, 0, MessageId.latest);
        operator().sendMessage(topicNameWithPartition(topicName, 0), STRING, randomAlphabetic(10));
        fetchedMessages(splitReader, 1, true);
    }

    @TestTemplate
    void consumeMessageCreatedBeforeHandleSplitsChanges(
            PulsarPartitionSplitReaderBase<String> splitReader) {
        String topicName = randomAlphabetic(10);
        operator().setupTopic(topicName, STRING, () -> randomAlphabetic(10));
        seekStartPositionAndHandleSplit(splitReader, topicName, 0);
        fetchedMessages(splitReader, 0, true);
    }

    @TestTemplate
    void consumeMessageCreatedBeforeHandleSplitsChangesAndResetToEarliestPosition(
            PulsarPartitionSplitReaderBase<String> splitReader) {
        String topicName = randomAlphabetic(10);
        operator().setupTopic(topicName, STRING, () -> randomAlphabetic(10));
        seekStartPositionAndHandleSplit(splitReader, topicName, 0, MessageId.earliest);
        fetchedMessages(splitReader, NUM_RECORDS_PER_PARTITION, true);
    }

    @TestTemplate
    void consumeMessageCreatedBeforeHandleSplitsChangesAndResetToLatestPosition(
            PulsarPartitionSplitReaderBase<String> splitReader) {
        String topicName = randomAlphabetic(10);
        operator().setupTopic(topicName, STRING, () -> randomAlphabetic(10));
        seekStartPositionAndHandleSplit(splitReader, topicName, 0, MessageId.latest);
        fetchedMessages(splitReader, 0, true);
    }

    @TestTemplate
    void consumeMessageCreatedBeforeHandleSplitsChangesAndUseSecondLastMessageIdCursor(
            PulsarPartitionSplitReaderBase<String> splitReader) {

        String topicName = randomAlphabetic(10);
        operator().setupTopic(topicName, STRING, () -> randomAlphabetic(10));
        MessageIdImpl lastMessageId =
                (MessageIdImpl)
                        sneakyAdmin(
                                () ->
                                        operator()
                                                .admin()
                                                .topics()
                                                .getLastMessageId(
                                                        topicNameWithPartition(topicName, 0)));
        // when doing seek directly on consumer, by default it includes the specified messageId
        seekStartPositionAndHandleSplit(
                splitReader,
                topicName,
                0,
                new MessageIdImpl(
                        lastMessageId.getLedgerId(),
                        lastMessageId.getEntryId() - 1,
                        lastMessageId.getPartitionIndex()));
        fetchedMessages(splitReader, 2, true);
    }

    @TestTemplate
    void emptyTopic(PulsarPartitionSplitReaderBase<String> splitReader) {
        String topicName = randomAlphabetic(10);
        operator().createTopic(topicName, DEFAULT_PARTITIONS);
        seekStartPositionAndHandleSplit(splitReader, topicName, 0);
        fetchedMessages(splitReader, 0, true);
    }

    @TestTemplate
    void emptyTopicWithoutSeek(PulsarPartitionSplitReaderBase<String> splitReader) {
        String topicName = randomAlphabetic(10);
        operator().createTopic(topicName, DEFAULT_PARTITIONS);
        handleSplit(splitReader, topicName, 0);
        fetchedMessages(splitReader, 0, true);
    }

    @TestTemplate
    void wakeupSplitReaderShouldNotCauseException(
            PulsarPartitionSplitReaderBase<String> splitReader) {
        handleSplit(splitReader, "non-exist", 0);
        AtomicReference<Throwable> error = new AtomicReference<>();
        Thread t =
                new Thread(
                        () -> {
                            try {
                                splitReader.fetch();
                            } catch (Throwable e) {
                                error.set(e);
                            }
                        },
                        "testWakeUp-thread");
        t.start();
        long deadline = System.currentTimeMillis() + 5000L;
        while (t.isAlive() && System.currentTimeMillis() < deadline) {
            splitReader.wakeUp();
            sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
        }
        assertThat(error.get()).isNull();
    }

    @TestTemplate
    void assignNoSplits(PulsarPartitionSplitReaderBase<String> splitReader) {
        assertThat(fetchedMessage(splitReader)).isNull();
    }

    /** Create a split reader with max message 1, fetch timeout 1s. */
    private PulsarPartitionSplitReaderBase<String> splitReader(SubscriptionType subscriptionType) {
        Configuration configuration = readerConfig();
        if (subscriptionType == SubscriptionType.Failover) {
            return new PulsarOrderedPartitionSplitReader<>(
                    operator().client(),
                    operator().admin(),
                    configuration,
                    sourceConfig(configuration),
                    flinkSchema(new SimpleStringSchema()));
        } else {
            return new PulsarUnorderedPartitionSplitReader<>(
                    operator().client(),
                    operator().admin(),
                    configuration,
                    sourceConfig(configuration),
                    flinkSchema(new SimpleStringSchema()),
                    null);
        }
    }

    /** Context Provider for PulsarSplitReaderTestBase. */
    public class PulsarSplitReaderInvocationContextProvider
            implements TestTemplateInvocationContextProvider {

        @Override
        public boolean supportsTestTemplate(ExtensionContext context) {
            return true;
        }

        @Override
        public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(
                ExtensionContext context) {
            SubscriptionType subscriptionType =
                    (SubscriptionType)
                            context.getStore(PULSAR_TEST_RESOURCE_NAMESPACE)
                                    .get(PULSAR_SOURCE_READER_SUBSCRIPTION_TYPE_STORE_KEY);
            return Stream.of(new PulsarSplitReaderInvocationContext(splitReader(subscriptionType)));
        }
    }

    /** Parameter resolver for Split Reader. */
    public static class PulsarSplitReaderInvocationContext
            implements TestTemplateInvocationContext {

        private final PulsarPartitionSplitReaderBase<?> splitReader;

        public PulsarSplitReaderInvocationContext(PulsarPartitionSplitReaderBase<?> splitReader) {
            this.splitReader = checkNotNull(splitReader);
        }

        @Override
        public String getDisplayName(int invocationIndex) {
            return splitReader.getClass().getSimpleName();
        }

        @Override
        public List<Extension> getAdditionalExtensions() {
            return Collections.singletonList(
                    new ParameterResolver() {
                        @Override
                        public boolean supportsParameter(
                                ParameterContext parameterContext,
                                ExtensionContext extensionContext)
                                throws ParameterResolutionException {
                            return parameterContext
                                    .getParameter()
                                    .getType()
                                    .equals(PulsarPartitionSplitReaderBase.class);
                        }

                        @Override
                        public Object resolveParameter(
                                ParameterContext parameterContext,
                                ExtensionContext extensionContext)
                                throws ParameterResolutionException {
                            return splitReader;
                        }
                    });
        }
    }
}
