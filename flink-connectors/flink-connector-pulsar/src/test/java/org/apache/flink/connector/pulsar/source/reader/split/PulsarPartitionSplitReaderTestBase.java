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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.reader.message.PulsarMessage;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.connector.pulsar.testutils.PulsarTestSuiteBase;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static java.time.Duration.ofSeconds;
import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_ENABLE_AUTO_ACKNOWLEDGE_MESSAGE;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_MAX_FETCH_RECORDS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_MAX_FETCH_TIME;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_NAME;
import static org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor.never;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.topicNameWithPartition;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange.createFullRange;
import static org.apache.flink.core.testutils.CommonTestUtils.waitUtil;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.pulsar.client.api.Schema.STRING;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Test utils for split readers. */
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

    protected SourceConfiguration sourceConfig() {
        return new SourceConfiguration(readerConfig());
    }

    protected SplitsAddition<PulsarPartitionSplit> createSplit(String topicName, int partitionId) {
        TopicPartition partition = new TopicPartition(topicName, partitionId, createFullRange());
        PulsarPartitionSplit split = new PulsarPartitionSplit(partition, never());
        return new SplitsAddition<>(singletonList(split));
    }

    protected <T> PulsarMessage<T> fetchedMessage(PulsarPartitionSplitReaderBase<T> splitReader) {
        try {
            RecordsWithSplitIds<PulsarMessage<T>> records = splitReader.fetch();
            if (records.nextSplit() != null) {
                return records.nextRecordFromSplit();
            } else {
                return null;
            }
        } catch (IOException e) {
            return null;
        }
    }

    @TestTemplate
    @DisplayName("Retrieve message after timeout by using given split reader")
    void pollMessageAfterTimeout(PulsarPartitionSplitReaderBase<String> splitReader)
            throws InterruptedException, TimeoutException {
        String topicName = randomAlphabetic(10);

        // Add a split
        splitReader.handleSplitsChanges(createSplit(topicName, 0));

        // Poll once with a null message
        PulsarMessage<String> message1 = fetchedMessage(splitReader);
        assertNull(message1);

        // Send a message to pulsar
        String topic = topicNameWithPartition(topicName, 0);
        operator().sendMessage(topic, STRING, randomAlphabetic(10));

        // Poll this message again
        waitUtil(
                () -> {
                    PulsarMessage<String> message2 = fetchedMessage(splitReader);
                    return message2 != null;
                },
                ofSeconds(10),
                "Couldn't poll message from Pulsar.");
    }

    /** Create a split reader with max message 1, fetch timeout 1s. */
    protected abstract PulsarPartitionSplitReaderBase<String> splitReader();

    /** JUnit5 extension for all the TestTemplate methods in this class. */
    public class PulsarSplitReaderInvocationContextProvider
            implements TestTemplateInvocationContextProvider {

        @Override
        public boolean supportsTestTemplate(ExtensionContext context) {
            return true;
        }

        @Override
        public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(
                ExtensionContext context) {
            return Stream.of(new PulsarSplitReaderInvocationContext(splitReader()));
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
