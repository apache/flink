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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.pulsar.sink.PulsarSinkBuilder;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;
import org.apache.flink.connector.pulsar.sink.writer.context.PulsarSinkContext;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils;

import java.io.Serializable;
import java.util.List;

/**
 * The router for choosing the desired topic to write flink records. The user can implement this
 * router for complex requirements. We have provided some easy to use implementations.
 *
 * <p>This topic router is stateless and don't have any initialize logic. Make sure you don't
 * require some dynamic state.
 *
 * @param <IN> The record type which need to be write to Pulsar.
 */
@PublicEvolving
public interface TopicRouter<IN> extends Serializable {

    /**
     * Choose the topic by given record & available partition list. You can return a totally new
     * topic name if you need.
     *
     * @param in The record instance which need to be write to Pulsar.
     * @param partitions The available partition list. This could be empty if you don't provide any
     *     topics in {@link PulsarSinkBuilder#setTopics(String...)}. You can return a custom topic,
     *     but make sure it should have partition index. Using {@link
     *     TopicNameUtils#topicNameWithPartition(String, int)} can easily create a topic name with
     *     partition index.
     * @param context The context which contains the useful information for determine the topic.
     * @return The topic name to use.
     */
    String route(IN in, List<String> partitions, PulsarSinkContext context);

    /** Implement this method if you have some non-serializable field. */
    default void open(SinkConfiguration sinkConfiguration) {
        // Nothing to do by default.
    }
}
