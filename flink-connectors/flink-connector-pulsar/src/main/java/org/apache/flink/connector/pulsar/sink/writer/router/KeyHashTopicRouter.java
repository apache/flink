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
import org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema;

import org.apache.flink.shaded.guava30.com.google.common.base.Strings;

import org.apache.pulsar.client.impl.Hash;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.pulsar.client.util.MathUtils.signSafeMod;

/**
 * If you choose the {@link TopicRoutingMode#MESSAGE_KEY_HASH} policy, we would use this
 * implementation. We would pick the topic by the message key's hash code. If no message key was
 * provided, we would randomly pick one.
 *
 * @param <IN> The message type which should write to Pulsar.
 */
@Internal
public class KeyHashTopicRouter<IN> implements TopicRouter<IN> {
    private static final long serialVersionUID = 2475614648095079804L;

    private final MessageKeyHash messageKeyHash;
    private final PulsarSerializationSchema<IN> serializationSchema;

    public KeyHashTopicRouter(
            SinkConfiguration sinkConfiguration,
            PulsarSerializationSchema<IN> serializationSchema) {
        this.messageKeyHash = sinkConfiguration.getMessageKeyHash();
        this.serializationSchema = serializationSchema;
    }

    @Override
    public String route(IN in, List<String> partitions, PulsarSinkContext context) {
        String key = serializationSchema.key(in, context);
        int topicIndex;

        if (Strings.isNullOrEmpty(key)) {
            // We would randomly pick one topic to write.
            topicIndex = ThreadLocalRandom.current().nextInt(partitions.size());
        } else {
            // Hash the message key and choose the topic to write.
            Hash hash = messageKeyHash.getHash();
            int code = hash.makeHash(key);
            topicIndex = signSafeMod(code, partitions.size());
        }

        return partitions.get(topicIndex);
    }
}
