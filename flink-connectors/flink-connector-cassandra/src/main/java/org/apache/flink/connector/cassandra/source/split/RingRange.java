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

package org.apache.flink.connector.cassandra.source.split;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.math.BigInteger;

/**
 * Represents a portion of Cassandra token ring. It is a range between a start token and an end
 * token.
 */
public final class RingRange implements Serializable {

    private final BigInteger start;
    private final BigInteger end;

    private RingRange(BigInteger start, BigInteger end) {
        this.start = start;
        this.end = end;
    }

    public static RingRange of(BigInteger start, BigInteger end) {
        return new RingRange(start, end);
    }

    public BigInteger getStart() {
        return start;
    }

    public BigInteger getEnd() {
        return end;
    }

    /**
     * Returns the size of this range.
     *
     * @return size of the range, max - range, in case of wrap
     */
    BigInteger span(BigInteger ringSize) {
        return (start.compareTo(end) >= 0)
                ? end.subtract(start).add(ringSize)
                : end.subtract(start);
    }

    /** @return true if the ringRange overlaps. Note that if start == end, then wrapping is true */
    public boolean isWrapping() {
        return start.compareTo(end) >= 0;
    }

    @Override
    public String toString() {
        return String.format("(%s,%s]", start.toString(), end.toString());
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RingRange ringRange = (RingRange) o;

        if (getStart() != null
                ? !getStart().equals(ringRange.getStart())
                : ringRange.getStart() != null) {
            return false;
        }
        return getEnd() != null ? getEnd().equals(ringRange.getEnd()) : ringRange.getEnd() == null;
    }

    @Override
    public int hashCode() {
        int result = getStart() != null ? getStart().hashCode() : 0;
        result = 31 * result + (getEnd() != null ? getEnd().hashCode() : 0);
        return result;
    }
}
