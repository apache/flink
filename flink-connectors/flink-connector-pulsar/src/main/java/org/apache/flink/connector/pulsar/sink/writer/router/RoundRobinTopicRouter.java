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

package org.apache.flink.connector.pulsar.sink.writer.router;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;
import org.apache.flink.connector.pulsar.sink.writer.context.PulsarSinkContext;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.flink.shaded.guava30.com.google.common.base.Preconditions.checkArgument;

/**
 * If you choose the {@link TopicRoutingMode#ROUND_ROBIN} policy, we would use this implementation.
 * We would pick the topic one by one in a fixed batch size.
 *
 * @param <IN> The message type which should write to Pulsar.
 */
@Internal
public class RoundRobinTopicRouter<IN> implements TopicRouter<IN> {
    private static final long serialVersionUID = -1160533263474038206L;

    /** The internal counter for counting the messages. */
    private final AtomicLong counter = new AtomicLong(0);

    /** The size when we switch to another topic. */
    private final int partitionSwitchSize;

    public RoundRobinTopicRouter(SinkConfiguration configuration) {
        this.partitionSwitchSize = configuration.getPartitionSwitchSize();
        Preconditions.checkArgument(partitionSwitchSize > 0);
    }

    @Override
    public String route(IN in, String key, List<String> partitions, PulsarSinkContext context) {
        checkArgument(
                !partitions.isEmpty(),
                "You should provide topics for routing topic by message key hash.");

        long counts = counter.getAndAdd(1);
        long index = (counts / partitionSwitchSize) % partitions.size();
        // Avoid digit overflow for message counter.
        int topicIndex = (int) (Math.abs(index) % Integer.MAX_VALUE);

        return partitions.get(topicIndex);
    }
}
