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

package org.apache.flink.connector.pulsar.source.enumerator.topic.range;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.pulsar.sink.writer.message.PulsarMessageBuilder;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicMetadata;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.TypedMessageBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.apache.flink.connector.pulsar.source.enumerator.topic.range.TopicRangeUtils.NONE_KEY;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.range.TopicRangeUtils.keyBytesHash;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.range.TopicRangeUtils.keyHash;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.range.TopicRangeUtils.validateTopicRanges;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Pulsar didn't expose the key hash range method. We have to provide an implementation for
 * end-user. You can add the keys you want to consume, no need to provide any hash ranges.
 *
 * <p>Since the key's hash isn't specified to only one key. The consuming results may contain the
 * messages with different keys comparing the keys you have defined in this range generator.
 * Remember to use flink's <code>DataStream.filter()</code> method.
 *
 * <p>Usage: <code><pre>
 * FixedKeysRangeGenerator.builder()
 *     .supportNullKey()
 *     .key("someKey")
 *     .keys(Arrays.asList("key1", "key2"))
 *     .build()
 * </pre></code>
 */
@PublicEvolving
public class FixedKeysRangeGenerator implements RangeGenerator {
    private static final long serialVersionUID = 2372969466289052100L;

    private final List<TopicRange> ranges;
    private final KeySharedMode sharedMode;

    private FixedKeysRangeGenerator(List<TopicRange> ranges, KeySharedMode sharedMode) {
        this.ranges = ranges;
        this.sharedMode = sharedMode;
    }

    @Override
    public List<TopicRange> range(TopicMetadata metadata, int parallelism) {
        return ranges;
    }

    @Override
    public KeySharedMode keyShareMode(TopicMetadata metadata, int parallelism) {
        return sharedMode;
    }

    public static FixedKeysRangeGeneratorBuilder builder() {
        return new FixedKeysRangeGeneratorBuilder();
    }

    /** The builder for {@link FixedKeysRangeGenerator}. */
    @PublicEvolving
    public static class FixedKeysRangeGeneratorBuilder {

        private final SortedSet<Integer> keyHashes = new TreeSet<>();
        private KeySharedMode sharedMode = KeySharedMode.JOIN;

        private FixedKeysRangeGeneratorBuilder() {
            // No public for builder
        }

        /**
         * Some {@link Message} in Pulsar may not have {@link Message#getOrderingKey()} or {@link
         * Message#getKey()}, use this method for supporting consuming such messages.
         */
        public FixedKeysRangeGeneratorBuilder supportNullKey() {
            keyHashes.add(keyHash(NONE_KEY));
            return this;
        }

        /**
         * If you set the message key by using {@link PulsarMessageBuilder#key(String)} or {@link
         * TypedMessageBuilder#key(String)}, use this method for supporting consuming such messages.
         */
        public FixedKeysRangeGeneratorBuilder key(String key) {
            keyHashes.add(keyHash(key));
            return this;
        }

        /** Same as the {@link #key(String)}, support setting multiple keys in the same time. */
        public FixedKeysRangeGeneratorBuilder keys(Collection<String> someKeys) {
            checkNotNull(someKeys);
            for (String someKey : someKeys) {
                keyHashes.add(keyHash(someKey));
            }
            return this;
        }

        /**
         * If you set the message key by using {@link TypedMessageBuilder#keyBytes(byte[])}, use
         * this method for supporting consuming such messages.
         */
        public FixedKeysRangeGeneratorBuilder keyBytes(byte[] keyBytes) {
            keyHashes.add(keyBytesHash(keyBytes));
            return this;
        }

        /**
         * Pulsar's ordering key is prior to the message key. If you set the ordering key by using
         * {@link PulsarMessageBuilder#orderingKey(byte[])} or {@link
         * TypedMessageBuilder#orderingKey(byte[])}, use this method for supporting consuming such
         * messages.
         */
        public FixedKeysRangeGeneratorBuilder orderingKey(byte[] keyBytes) {
            keyHashes.add(keyHash(keyBytes));
            return this;
        }

        /** Override the default {@link KeySharedMode#JOIN} to the mode your have provided. */
        public FixedKeysRangeGeneratorBuilder keySharedMode(KeySharedMode sharedMode) {
            this.sharedMode = sharedMode;
            return this;
        }

        /** Create the FixedKeysRangeGenerator by the given keys. */
        public FixedKeysRangeGenerator build() {
            List<TopicRange> ranges = new ArrayList<>();
            // Calculate the topic ranges.
            Integer start = null;
            Integer next = null;
            for (Integer hash : keyHashes) {
                // Start
                if (start == null) {
                    start = hash;
                    next = hash;
                    continue;
                }

                // Continue range.
                if (hash - next == 1) {
                    next = hash;
                    continue;
                }

                // Found one continues topic range.
                TopicRange range = new TopicRange(start, next);
                ranges.add(range);

                start = hash;
                next = hash;
            }

            // Support the last range.
            if (start != null) {
                TopicRange range = new TopicRange(start, next);
                ranges.add(range);
            }

            validateTopicRanges(ranges, sharedMode);
            return new FixedKeysRangeGenerator(ranges, sharedMode);
        }
    }
}
