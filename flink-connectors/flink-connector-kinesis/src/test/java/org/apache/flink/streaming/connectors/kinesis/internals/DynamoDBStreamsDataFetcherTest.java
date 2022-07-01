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

package org.apache.flink.streaming.connectors.kinesis.internals;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyInterface;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kinesis.testutils.TestSourceContext;
import org.apache.flink.streaming.connectors.kinesis.testutils.TestUtils;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static com.amazonaws.services.kinesis.model.ShardIteratorType.LATEST;
import static java.util.Collections.singletonList;
import static org.apache.flink.streaming.connectors.kinesis.internals.KinesisDataFetcher.DEFAULT_SHARD_ASSIGNER;
import static org.apache.flink.streaming.connectors.kinesis.internals.ShardConsumerTestUtils.createFakeShardConsumerMetricGroup;
import static org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber.SENTINEL_LATEST_SEQUENCE_NUM;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/** Tests for {@link DynamoDBStreamsDataFetcher}. */
public class DynamoDBStreamsDataFetcherTest {

    @Test
    public void testCreateRecordPublisherRespectsShardIteratorTypeLatest() throws Exception {
        RuntimeContext runtimeContext = TestUtils.getMockedRuntimeContext(1, 0);
        KinesisProxyInterface kinesis = mock(KinesisProxyInterface.class);

        DynamoDBStreamsDataFetcher<String> fetcher =
                new DynamoDBStreamsDataFetcher<>(
                        singletonList("fakeStream"),
                        new TestSourceContext<>(),
                        runtimeContext,
                        TestUtils.getStandardProperties(),
                        new KinesisDeserializationSchemaWrapper<>(new SimpleStringSchema()),
                        DEFAULT_SHARD_ASSIGNER,
                        config -> kinesis);

        StreamShardHandle dummyStreamShardHandle =
                TestUtils.createDummyStreamShardHandle("dummy-stream", "0");

        fetcher.createRecordPublisher(
                SENTINEL_LATEST_SEQUENCE_NUM.get(),
                new Properties(),
                createFakeShardConsumerMetricGroup(runtimeContext.getMetricGroup()),
                dummyStreamShardHandle);

        verify(kinesis).getShardIterator(dummyStreamShardHandle, LATEST.toString(), null);
    }
}
