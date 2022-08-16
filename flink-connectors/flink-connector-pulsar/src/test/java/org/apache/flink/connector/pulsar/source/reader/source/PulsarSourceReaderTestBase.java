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

package org.apache.flink.connector.pulsar.source.reader.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils;
import org.apache.flink.connector.pulsar.source.reader.PulsarSourceReaderFactory;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchemaInitializationContext;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.connector.pulsar.testutils.PulsarTestSuiteBase;
import org.apache.flink.connector.pulsar.testutils.extension.TestOrderlinessExtension;
import org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntimeOperator;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.junit.jupiter.api.BeforeEach;
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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_ENABLE_AUTO_ACKNOWLEDGE_MESSAGE;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_MAX_FETCH_RECORDS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_MAX_FETCH_TIME;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_NAME;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_TYPE;
import static org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema.pulsarSchema;
import static org.apache.flink.connector.pulsar.testutils.PulsarTestCommonUtils.createPartitionSplit;
import static org.apache.flink.connector.pulsar.testutils.PulsarTestCommonUtils.isAssignableFromParameterContext;
import static org.apache.flink.connector.pulsar.testutils.extension.TestOrderlinessExtension.PULSAR_SOURCE_READER_SUBSCRIPTION_TYPE_STORE_KEY;
import static org.apache.flink.connector.pulsar.testutils.extension.TestOrderlinessExtension.PULSAR_TEST_RESOURCE_NAMESPACE;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;

@ExtendWith({
    TestOrderlinessExtension.class,
    TestLoggerExtension.class,
})
abstract class PulsarSourceReaderTestBase extends PulsarTestSuiteBase {

    @RegisterExtension
    PulsarSourceReaderInvocationContextProvider provider =
            new PulsarSourceReaderInvocationContextProvider();

    @BeforeEach
    void beforeEach(String topicName) {
        Random random = new Random(System.currentTimeMillis());
        operator().setupTopic(topicName, Schema.INT32, () -> random.nextInt(20));
    }

    @TestTemplate
    void assignZeroSplitsCreatesZeroSubscription(
            PulsarSourceReaderBase<Integer> reader, Boundedness boundedness, String topicName)
            throws Exception {
        reader.snapshotState(100L);
        reader.notifyCheckpointComplete(100L);
        // Verify the committed offsets.
        reader.close();
        for (int i = 0; i < PulsarRuntimeOperator.DEFAULT_PARTITIONS; i++) {
            verifyNoSubscriptionCreated(TopicNameUtils.topicNameWithPartition(topicName, i));
        }
    }

    @TestTemplate
    void assigningEmptySplits(
            PulsarSourceReaderBase<Integer> reader, Boundedness boundedness, String topicName)
            throws Exception {
        final PulsarPartitionSplit emptySplit =
                createPartitionSplit(
                        topicName, 0, Boundedness.CONTINUOUS_UNBOUNDED, MessageId.latest);

        reader.addSplits(Collections.singletonList(emptySplit));

        TestingReaderOutput<Integer> output = new TestingReaderOutput<>();
        InputStatus status = reader.pollNext(output);
        assertThat(status).isEqualTo(InputStatus.NOTHING_AVAILABLE);
        reader.close();
    }

    private void verifyNoSubscriptionCreated(String partitionName) throws PulsarAdminException {
        Map<String, ? extends SubscriptionStats> subscriptionStats =
                operator().admin().topics().getStats(partitionName, true, true).getSubscriptions();
        assertThat(subscriptionStats).isEmpty();
    }

    private PulsarSourceReaderBase<Integer> sourceReader(
            boolean autoAcknowledgementEnabled, SubscriptionType subscriptionType) {
        Configuration configuration = operator().config();
        configuration.set(PULSAR_MAX_FETCH_RECORDS, 1);
        configuration.set(PULSAR_MAX_FETCH_TIME, 1000L);
        configuration.set(PULSAR_SUBSCRIPTION_NAME, randomAlphabetic(10));
        configuration.set(PULSAR_SUBSCRIPTION_TYPE, subscriptionType);
        if (autoAcknowledgementEnabled
                || configuration.get(PULSAR_SUBSCRIPTION_TYPE) == SubscriptionType.Shared) {
            configuration.set(PULSAR_ENABLE_AUTO_ACKNOWLEDGE_MESSAGE, true);
        }
        PulsarDeserializationSchema<Integer> deserializationSchema = pulsarSchema(Schema.INT32);
        SourceReaderContext context = new TestingReaderContext();
        try {
            deserializationSchema.open(
                    new PulsarDeserializationSchemaInitializationContext(context),
                    mock(SourceConfiguration.class));
        } catch (Exception e) {
            fail("Error while opening deserializationSchema");
        }

        SourceConfiguration sourceConfiguration = new SourceConfiguration(configuration);
        return (PulsarSourceReaderBase<Integer>)
                PulsarSourceReaderFactory.create(
                        context, deserializationSchema, sourceConfiguration);
    }

    public class PulsarSourceReaderInvocationContextProvider
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
            return Stream.of(
                    new PulsarSourceReaderInvocationContext(
                            sourceReader(true, subscriptionType), Boundedness.CONTINUOUS_UNBOUNDED),
                    new PulsarSourceReaderInvocationContext(
                            sourceReader(false, subscriptionType),
                            Boundedness.CONTINUOUS_UNBOUNDED));
        }
    }

    public static class PulsarSourceReaderInvocationContext
            implements TestTemplateInvocationContext {

        private final PulsarSourceReaderBase<?> sourceReader;
        private final Boundedness boundedness;
        private final String randomTopicName;

        public PulsarSourceReaderInvocationContext(
                PulsarSourceReaderBase<?> splitReader, Boundedness boundedness) {
            this.sourceReader = checkNotNull(splitReader);
            this.boundedness = checkNotNull(boundedness);
            this.randomTopicName = randomAlphabetic(5);
        }

        @Override
        public String getDisplayName(int invocationIndex) {
            return "AutoAckEnabled: "
                    + sourceReader.sourceConfiguration.isEnableAutoAcknowledgeMessage()
                    + "  Boundedness: "
                    + boundedness.toString();
        }

        @Override
        public List<Extension> getAdditionalExtensions() {
            return Arrays.asList(
                    new ParameterResolver() {
                        @Override
                        public boolean supportsParameter(
                                ParameterContext parameterContext,
                                ExtensionContext extensionContext)
                                throws ParameterResolutionException {
                            return isAssignableFromParameterContext(
                                            PulsarSourceReaderBase.class, parameterContext)
                                    || isAssignableFromParameterContext(
                                            Boundedness.class, parameterContext)
                                    || isAssignableFromParameterContext(
                                            String.class, parameterContext);
                        }

                        @Override
                        public Object resolveParameter(
                                ParameterContext parameterContext,
                                ExtensionContext extensionContext)
                                throws ParameterResolutionException {
                            if (parameterContext
                                    .getParameter()
                                    .getType()
                                    .equals(PulsarSourceReaderBase.class)) {
                                return sourceReader;
                            } else if (parameterContext
                                    .getParameter()
                                    .getType()
                                    .equals(Boundedness.class)) {
                                return boundedness;
                            } else {
                                return randomTopicName;
                            }
                        }
                    });
        }
    }
}
