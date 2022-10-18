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

package org.apache.flink.streaming.connectors.kinesis.proxy;

import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;
import org.apache.flink.streaming.connectors.kinesis.testutils.FakeKinesisClientFactory;
import org.apache.flink.streaming.connectors.kinesis.testutils.TestUtils;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.Shard;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for methods in the {@link DynamoDBStreamsProxy} class. */
class DynamoDBStreamsProxyTest {

    private static final String FAKE_STREAM_NAME = "fake-stream";

    private static final List<String> SHARD_IDS =
            Arrays.asList(
                    "shardId-000000000000",
                    "shardId-000000000001",
                    "shardId-000000000002",
                    "shardId-000000000003");

    @Test
    void testGetShardIterator() throws Exception {

        String invalidShardId = "shardId-000000000004";

        DynamoDBStreamsProxy ddbStreamsProxy = new TestableDynamoDBStreamsProxy();

        for (String shardId : SHARD_IDS) {
            String shardIterator =
                    ddbStreamsProxy.getShardIterator(getStreamShardHandle(shardId), "LATEST", null);

            assertThat(shardIterator).isEqualTo("fakeShardIterator");
        }

        String invalidShardIterator =
                ddbStreamsProxy.getShardIterator(
                        getStreamShardHandle(invalidShardId), "LATEST", null);

        assertThat(invalidShardIterator).isNull();
    }

    private StreamShardHandle getStreamShardHandle(String shardId) {
        return new StreamShardHandle(FAKE_STREAM_NAME, new Shard().withShardId(shardId));
    }

    private static class TestableDynamoDBStreamsProxy extends DynamoDBStreamsProxy {

        private TestableDynamoDBStreamsProxy() {
            super(TestUtils.getStandardProperties());
        }

        protected AmazonKinesis createKinesisClient(Properties configProps) {
            return FakeKinesisClientFactory.resourceNotFoundWhenGettingShardIterator(
                    FAKE_STREAM_NAME, SHARD_IDS);
        }
    }
}
