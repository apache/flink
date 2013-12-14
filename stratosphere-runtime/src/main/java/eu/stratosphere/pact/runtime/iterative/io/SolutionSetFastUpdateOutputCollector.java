/***********************************************************************************************************************
 *
 * Copyright (C) 2012, 2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.iterative.io;

import eu.stratosphere.pact.runtime.hash.MutableHashTable;
import eu.stratosphere.util.Collector;

import java.io.IOException;

/**
 * A {@link Collector} to update the solution set of a workset iteration.
 * <p/>
 * The records are written to a {@link MutableHashTable} hash table to allow in-memory point updates.
 * <p/>
 * Assumption for fast updates: the build side iterator of the hash table is already positioned for the update. This
 * is for example the case when a solution set update happens directly after a solution set join. If this assumption
 * doesn't hold, use {@link SolutionSetUpdateOutputCollector}, which probes the hash table before updating.
 *
 * @see {SolutionSetUpdateOutputCollector}
 */
public class SolutionSetFastUpdateOutputCollector<T> implements Collector<T> {

    private final Collector<T> delegate;
    private final MutableHashTable<T, ?> solutionSet;

    public SolutionSetFastUpdateOutputCollector(MutableHashTable<T, ?> solutionSet) {
        this(solutionSet, null);
    }

    public SolutionSetFastUpdateOutputCollector(MutableHashTable<T, ?> solutionSet, Collector<T> delegate) {
        this.solutionSet = solutionSet;
        this.delegate = delegate;
    }

    @Override
    public void collect(T record) {
        try {
            MutableHashTable.HashBucketIterator<T, ?> bucket = solutionSet.getBuildSideIterator();
            bucket.writeBack(record);

            if (delegate != null) {
                delegate.collect(record);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        if (delegate != null) {
            delegate.close();
        }
    }
}
