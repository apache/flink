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
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange;
import org.apache.flink.connector.pulsar.source.enumerator.topic.range.RangeGenerator.KeySharedMode;

import org.apache.pulsar.client.api.KeySharedPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.common.util.Murmur3_32Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static java.util.Comparator.comparingLong;
import static java.util.stream.Collectors.toList;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange.MAX_RANGE;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This is the helper class for Pulsar's {@link Range}, used in Key Shared mode. It will be exposed
 * to the end users for simplifying the implementation of the {@link RangeGenerator}.
 */
@PublicEvolving
public final class TopicRangeUtils {
    private static final Logger LOG = LoggerFactory.getLogger(TopicRangeUtils.class);

    /** Pulsar would use this as default key if no key was provided. */
    public static final String NONE_KEY = "NONE_KEY";

    private TopicRangeUtils() {
        // No public constructor.
    }

    /** Make sure all the ranges should be valid in Pulsar Key Shared Policy. */
    public static void validateTopicRanges(List<TopicRange> ranges, KeySharedMode sharedMode) {
        List<Range> pulsarRanges = ranges.stream().map(TopicRange::toPulsarRange).collect(toList());
        KeySharedPolicy.stickyHashRange().ranges(pulsarRanges).validate();

        if (!isFullTopicRanges(ranges) && KeySharedMode.SPLIT == sharedMode) {
            LOG.warn(
                    "You have provided a partial key hash range with KeySharedMode.SPLIT. "
                            + "You can't consume any message if there are any messages with keys that are out of the given ranges.");
        }
    }

    /** Check if the given topic ranges are full Pulsar range. */
    public static boolean isFullTopicRanges(List<TopicRange> ranges) {
        List<TopicRange> sorted =
                ranges.stream().sorted(comparingLong(TopicRange::getStart)).collect(toList());
        int start = 0;
        for (TopicRange range : sorted) {
            if (start == 0) {
                if (range.getStart() == 0) {
                    start = range.getEnd();
                    continue;
                } else {
                    return false;
                }
            }

            if (range.getStart() - start != 1) {
                return false;
            }
            start = range.getEnd();
        }

        return start == MAX_RANGE;
    }

    /**
     * Pulsar didn't expose the key hash range method. We have to manually define it here.
     *
     * @param key The key of Pulsar's {@link Message}. Pulsar would try to use {@link
     *     Message#getOrderingKey()} first. If it doesn't exist Pulsar will use {@link
     *     Message#getKey()} instead. Remember that the {@link Message#getOrderingKey()} could be
     *     configured by {@link PulsarMessageBuilder#orderingKey(byte[])} and the {@link
     *     Message#getKey()} could be configured by {@link PulsarMessageBuilder#key(String)}.
     */
    public static int keyHash(String key) {
        checkNotNull(key);
        return keyHash(key.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * This method is a bit of different compared to the {@link #keyHash(byte[])}. We only define
     * this method when you set the message key by using {@link
     * TypedMessageBuilder#keyBytes(byte[])}. Because the Pulsar would calculate the message key
     * hash in a different way.
     *
     * <p>It should be <strong>a bug on Pulsar</strong>, but we can't fix it for backward
     * compatibility.
     */
    public static int keyBytesHash(byte[] keyBytes) {
        String encodedKey = Base64.getEncoder().encodeToString(checkNotNull(keyBytes));
        byte[] encodedKeyBytes = encodedKey.getBytes(StandardCharsets.UTF_8);
        return keyHash(encodedKeyBytes);
    }

    /**
     * Pulsar didn't expose the key hash range method. We have to manually define it here.
     *
     * @param keyBytes The key bytes of Pulsar's {@link Message}. Pulsar would try to use {@link
     *     Message#getOrderingKey()} first. If it doesn't exist Pulsar will use {@link
     *     Message#getKey()} instead. Remember that the {@link Message#getOrderingKey()} could be
     *     configured by {@link PulsarMessageBuilder#orderingKey(byte[])} and the {@link
     *     Message#getKey()} could be configured by {@link PulsarMessageBuilder#key(String)}.
     */
    public static int keyHash(byte[] keyBytes) {
        int stickyKeyHash = Murmur3_32Hash.getInstance().makeHash(checkNotNull(keyBytes));
        return stickyKeyHash % TopicRange.RANGE_SIZE;
    }
}
