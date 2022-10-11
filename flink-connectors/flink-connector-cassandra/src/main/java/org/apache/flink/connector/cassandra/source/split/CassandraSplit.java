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

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;
import java.util.Objects;
import java.util.Set;

/**
 * {@link SourceSplit} for Cassandra source. A Cassandra split is just a set of {@link RingRange}s
 * (a range between 2 tokens). Tokens are spread across the Cassandra cluster with each node
 * managing a share of the token ring. Each split can contain several token ranges in order to
 * reduce the overhead on Cassandra vnodes.
 */
public class CassandraSplit implements SourceSplit, Serializable {

    private final Set<RingRange> ringRanges;

    public CassandraSplit(Set<RingRange> ringRanges) {
        this.ringRanges = ringRanges;
    }

    public Set<RingRange> getRingRanges() {
        return ringRanges;
    }

    @Override
    public String splitId() {
        return ringRanges.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CassandraSplit that = (CassandraSplit) o;
        return ringRanges.equals(that.ringRanges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ringRanges);
    }
}
