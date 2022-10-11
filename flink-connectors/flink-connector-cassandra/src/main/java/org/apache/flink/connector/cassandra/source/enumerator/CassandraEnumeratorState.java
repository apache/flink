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

package org.apache.flink.connector.cassandra.source.enumerator;

import org.apache.flink.connector.cassandra.source.split.CassandraSplit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Sate for {@link CassandraSplitEnumerator} to track the splits yet to assign. */
public class CassandraEnumeratorState implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraEnumeratorState.class);

    // map readerId to splits
    private final Map<Integer, Set<CassandraSplit>> unassignedSplits = new HashMap<>();

    public void addNewSplits(Collection<CassandraSplit> newSplits, int numReaders) {
        for (CassandraSplit split : newSplits) {
            int ownerReader = getOwnerReader(numReaders, split);
            unassignedSplits.computeIfAbsent(ownerReader, r -> new HashSet<>()).add(split);
        }
    }

    private int getOwnerReader(int numReaders, CassandraSplit split) {
        // readerId == subTaksId == 0 or 1 if numReaders == 2 so  modulo is fine for ownerReader
        final int rawOwnerId = split.splitId().hashCode() % numReaders;
        // split.splitId().hashCode() can be negative
        return Math.abs(rawOwnerId);
    }

    public Set<CassandraSplit> getSplitsForReader(int readerId) {
        return unassignedSplits.remove(readerId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CassandraEnumeratorState that = (CassandraEnumeratorState) o;
        return this.unassignedSplits.equals(that.unassignedSplits);
    }

    @Override
    public int hashCode() {
        return Objects.hash(unassignedSplits);
    }
}
