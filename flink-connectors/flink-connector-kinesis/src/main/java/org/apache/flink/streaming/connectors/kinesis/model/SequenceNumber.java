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

package org.apache.flink.streaming.connectors.kinesis.model;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A serializable representation of a Kinesis record's sequence number. It has two fields: the main
 * sequence number, and also a subsequence number. If this {@link SequenceNumber} is referring to an
 * aggregated Kinesis record, the subsequence number will be a non-negative value representing the
 * order of the sub-record within the aggregation.
 */
@Internal
public class SequenceNumber implements Serializable {

    private static final long serialVersionUID = 876972197938972667L;

    private static final String DELIMITER = "-";

    private final String sequenceNumber;
    private final long subSequenceNumber;

    private final int cachedHash;

    /**
     * Create a new instance for a non-aggregated Kinesis record without a subsequence number.
     *
     * @param sequenceNumber the sequence number
     */
    public SequenceNumber(String sequenceNumber) {
        this(sequenceNumber, -1);
    }

    /**
     * Create a new instance, with the specified sequence number and subsequence number. To
     * represent the sequence number for a non-aggregated Kinesis record, the subsequence number
     * should be -1. Otherwise, give a non-negative sequence number to represent an aggregated
     * Kinesis record.
     *
     * @param sequenceNumber the sequence number
     * @param subSequenceNumber the subsequence number (-1 to represent non-aggregated Kinesis
     *     records)
     */
    public SequenceNumber(String sequenceNumber, long subSequenceNumber) {
        this.sequenceNumber = checkNotNull(sequenceNumber);
        this.subSequenceNumber = subSequenceNumber;

        this.cachedHash =
                37 * (sequenceNumber.hashCode() + Long.valueOf(subSequenceNumber).hashCode());
    }

    public boolean isAggregated() {
        return subSequenceNumber >= 0;
    }

    public String getSequenceNumber() {
        return sequenceNumber;
    }

    public long getSubSequenceNumber() {
        return subSequenceNumber;
    }

    @Override
    public String toString() {
        if (isAggregated()) {
            return sequenceNumber + DELIMITER + subSequenceNumber;
        } else {
            return sequenceNumber;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof SequenceNumber)) {
            return false;
        }

        if (obj == this) {
            return true;
        }

        SequenceNumber other = (SequenceNumber) obj;

        return sequenceNumber.equals(other.getSequenceNumber())
                && (subSequenceNumber == other.getSubSequenceNumber());
    }

    @Override
    public int hashCode() {
        return cachedHash;
    }
}
