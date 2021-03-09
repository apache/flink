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

package org.apache.flink.streaming.connectors.gcp.pubsub;

import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.AcknowledgeIdsForCheckpoint;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.AcknowledgeOnCheckpoint;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubDeserializationSchema;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubSubscriber;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubSubscriberFactory;

import com.google.auth.Credentials;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

/** Test for {@link SourceFunction}. */
@RunWith(MockitoJUnitRunner.class)
public class PubSubSourceTest {
    @Mock private PubSubDeserializationSchema<String> deserializationSchema;
    @Mock private PubSubSource.AcknowledgeOnCheckpointFactory acknowledgeOnCheckpointFactory;
    @Mock private AcknowledgeOnCheckpoint<String> acknowledgeOnCheckpoint;
    @Mock private StreamingRuntimeContext streamingRuntimeContext;
    @Mock private MetricGroup metricGroup;
    @Mock private PubSubSubscriberFactory pubSubSubscriberFactory;
    @Mock private Credentials credentials;
    @Mock private PubSubSubscriber pubsubSubscriber;
    @Mock private FlinkConnectorRateLimiter rateLimiter;

    private PubSubSource<String> pubSubSource;

    @Before
    public void setup() throws Exception {
        when(pubSubSubscriberFactory.getSubscriber(eq(credentials))).thenReturn(pubsubSubscriber);
        when(streamingRuntimeContext.isCheckpointingEnabled()).thenReturn(true);
        when(streamingRuntimeContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup(any(String.class))).thenReturn(metricGroup);
        when(acknowledgeOnCheckpointFactory.create(any())).thenReturn(acknowledgeOnCheckpoint);

        pubSubSource =
                new PubSubSource<>(
                        deserializationSchema,
                        pubSubSubscriberFactory,
                        credentials,
                        acknowledgeOnCheckpointFactory,
                        rateLimiter,
                        1024);
        pubSubSource.setRuntimeContext(streamingRuntimeContext);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testOpenWithoutCheckpointing() throws Exception {
        when(streamingRuntimeContext.isCheckpointingEnabled()).thenReturn(false);
        pubSubSource.open(null);
    }

    @Test
    public void testOpenWithCheckpointing() throws Exception {
        when(streamingRuntimeContext.isCheckpointingEnabled()).thenReturn(true);

        pubSubSource.open(null);

        verify(pubSubSubscriberFactory, times(1)).getSubscriber(eq(credentials));
        verify(acknowledgeOnCheckpointFactory, times(1)).create(pubsubSubscriber);
    }

    @Test
    public void testTypeInformationFromDeserializationSchema() {
        TypeInformation<String> schemaTypeInformation = TypeInformation.of(String.class);
        when(deserializationSchema.getProducedType()).thenReturn(schemaTypeInformation);

        TypeInformation<String> actualTypeInformation = pubSubSource.getProducedType();

        assertThat(actualTypeInformation, is(schemaTypeInformation));
        verify(deserializationSchema, times(1)).getProducedType();
    }

    @Test
    public void testNotifyCheckpointComplete() throws Exception {
        pubSubSource.open(null);
        pubSubSource.notifyCheckpointComplete(45L);

        verify(acknowledgeOnCheckpoint, times(1)).notifyCheckpointComplete(45L);
    }

    @Test
    public void testRestoreState() throws Exception {
        pubSubSource.open(null);

        List<AcknowledgeIdsForCheckpoint<String>> input = new ArrayList<>();
        pubSubSource.restoreState(input);

        verify(acknowledgeOnCheckpoint, times(1)).restoreState(refEq(input));
    }

    @Test
    public void testSnapshotState() throws Exception {
        pubSubSource.open(null);
        pubSubSource.snapshotState(1337L, 15000L);

        verify(acknowledgeOnCheckpoint, times(1)).snapshotState(1337L, 15000L);
    }

    @Test
    public void testOpen() throws Exception {
        doAnswer(
                        (args) -> {
                            DeserializationSchema.InitializationContext context =
                                    args.getArgument(0);
                            Assert.assertThat(
                                    context.getMetricGroup(), Matchers.equalTo(metricGroup));
                            return null;
                        })
                .when(deserializationSchema)
                .open(any(DeserializationSchema.InitializationContext.class));
        pubSubSource.open(null);

        verify(deserializationSchema, times(1))
                .open(any(DeserializationSchema.InitializationContext.class));
    }
}
