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

package org.apache.flink.connector.pulsar.sink.writer.topic;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;
import org.apache.flink.connector.pulsar.sink.writer.router.TopicRouter;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicMetadata;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;

import org.apache.pulsar.client.admin.PulsarAdmin;

import java.io.Serializable;

/** Choose topics from the message, used for dynamic generate topics in Pulsar sink. */
@PublicEvolving
public interface TopicExtractor<IN> extends Serializable {

    /**
     * @param in The message would be written to Pulsar.
     * @param provider Used for query topic metadata.
     * @return The topic you want to use. You can use both partitioned topic name or a topic name
     *     without partition information. We would query the partition information and pass it to
     *     {@link TopicRouter} if you return a topic name without partition information.
     */
    TopicPartition extract(IN in, TopicMetadataProvider provider);

    /** Implement this method if you have some non-serializable field. */
    default void open(SinkConfiguration sinkConfiguration) {
        // Nothing to do by default.
    }

    /**
     * A wrapper for {@link PulsarAdmin} instance, we won't expose the Pulsar admin interface for
     * better control the abstraction. And add cache support.
     */
    @PublicEvolving
    interface TopicMetadataProvider {

        /** @throws Exception Failed to query Pulsar metadata would throw this exception. */
        TopicMetadata query(String topic) throws Exception;
    }
}
