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

package org.apache.flink.streaming.connectors.kinesis.internals.publisher;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.connectors.kinesis.model.StartingPosition;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;

import java.util.Properties;

/** A factory interface used to create instances of {@link RecordPublisher}. */
@Internal
public interface RecordPublisherFactory {

    /**
     * Create a {@link RecordPublisher}.
     *
     * @param startingPosition the position in the shard to start consuming records from
     * @param consumerConfig the properties used to configure the {@link RecordPublisher}.
     * @param metricGroup the {@link MetricGroup} used to report metrics to
     * @param streamShardHandle the stream shard in which to consume from
     * @return the constructed {@link RecordPublisher}
     */
    RecordPublisher create(
            StartingPosition startingPosition,
            Properties consumerConfig,
            MetricGroup metricGroup,
            StreamShardHandle streamShardHandle)
            throws InterruptedException;

    /** Destroy any open resources used by the factory. */
    default void close() {
        // Do nothing by default
    }
}
