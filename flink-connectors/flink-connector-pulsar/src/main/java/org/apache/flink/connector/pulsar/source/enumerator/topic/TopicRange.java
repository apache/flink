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

import org.apache.flink.annotation.PublicEvolving;

import org.apache.pulsar.client.api.KeySharedPolicy;
import org.apache.pulsar.client.api.Range;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * This class is used to define the range for KeyShared subscription.
 *
 * <p>{@link Range} is a hash scope for pulsar message key. The total hash range size is 65536, so
 * the end of the range should be 65535, the start the range should be 0.
 *
 * @see KeySharedPolicy.KeySharedPolicySticky
 */
@PublicEvolving
public class TopicRange implements Serializable {
    private static final long serialVersionUID = 3176938692775594400L;

    public static final int RANGE_SIZE = KeySharedPolicy.DEFAULT_HASH_RANGE_SIZE;

    /** The start position for hash range. */
    public static final int MIN_RANGE = 0;

    /** The end position for hash range, it's 65535. */
    public static final int MAX_RANGE = RANGE_SIZE - 1;

    /** The start of the range, default is zero. */
    private final int start;

    /** The end of the range, included. */
    private final int end;

    public TopicRange(int start, int end) {
        checkArgument(start >= MIN_RANGE, "Start range %s shouldn't below zero.", start);
        checkArgument(end <= MAX_RANGE, "End range %s shouldn't exceed 65535.", end);
        checkArgument(start <= end, "Range end must >= range start.");

        this.start = start;
        this.end = end;
    }

    /** Convert to pulsar's {@link Range} API for consuming in client. */
    public Range toPulsarRange() {
        return new Range(start, end);
    }

    /** Create a topic range which contains the fully hash range. */
    public static TopicRange createFullRange() {
        return new TopicRange(MIN_RANGE, MAX_RANGE);
    }

    public int getStart() {
        return start;
    }

    public int getEnd() {
        return end;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
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
