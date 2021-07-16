/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.pulsar.source.enumerator.topic;

import org.apache.pulsar.client.api.Range;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * This class is used to define the consuming scope for the different subscription type.
 *
 * <p>{@link Range} is a hash scope for pulsar messages. The total hash range size is 65536, so the
 * max end of the range should be less than or equal to 65535, the min of the range should be above
 * or equal to 0.
 */
public class TopicRange implements Serializable {
    private static final long serialVersionUID = 3176938692775594400L;

    /** The start position for both hash and partition range. */
    public static final int MIN_RANGE = 0;

    /** The end position for full hash range. */
    public static final int MAX_HASH_RANGE = 65535;

    /** The start of the range, default is zero. */
    private final int start;

    /** The end of the range, included. */
    private final int end;

    private TopicRange(int start, int end) {
        checkArgument(start >= MIN_RANGE, "Start range %s shouldn't below zero.", start);
        checkArgument(end <= MAX_HASH_RANGE, "End range %s shouldn't exceed 65535.", end);
        this.start = start;
        this.end = end;
    }

    /** Convert to pulsar's {@link Range} API for consuming in client. */
    public Range toPulsarRange() {
        return new Range(start, end);
    }

    /** Create a topic range which contains the fully hash range */
    public static TopicRange createFullRange() {
        return createTopicRange(MIN_RANGE, MAX_HASH_RANGE);
    }

    public static TopicRange createTopicRange(int start, int end) {
        return new TopicRange(start, end);
    }

    public int getStart() {
        return start;
    }

    public int getEnd() {
        return end;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopicRange that = (TopicRange) o;
        return start == that.start && end == that.end;
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, end);
    }

    @Override
    public String toString() {
        return getStart() + "-" + getEnd();
    }
}
