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

package org.apache.flink.runtime.iterative.io;

import org.apache.flink.api.common.operators.util.JoinHashMap;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.Collector;

/**
 * A {@link Collector} to update the solution set of a workset iteration.
 *
 * <p>The records are written to a HashTable hash table to allow in-memory point updates.
 *
 * <p>Records will only be collected, if there is a match after probing the hash table. If the build
 * side iterator is already positioned for the update, use {@link
 * SolutionSetFastUpdateOutputCollector} to the save re-probing.
 *
 * @see SolutionSetFastUpdateOutputCollector
 */
public class SolutionSetObjectsUpdateOutputCollector<T> implements Collector<T> {

    private final Collector<T> delegate;

    private final JoinHashMap<T> hashMap;

    private final TypeSerializer<T> serializer;

    public SolutionSetObjectsUpdateOutputCollector(JoinHashMap<T> hashMap) {
        this(hashMap, null);
    }

    public SolutionSetObjectsUpdateOutputCollector(JoinHashMap<T> hashMap, Collector<T> delegate) {
        this.delegate = delegate;
        this.hashMap = hashMap;
        this.serializer = hashMap.getBuildSerializer();
    }

    @Override
    public void collect(T record) {
        T copy = serializer.copy(record);
        hashMap.insertOrReplace(copy);
        if (delegate != null) {
            delegate.collect(record);
        }
    }

    @Override
    public void close() {
        if (delegate != null) {
            delegate.close();
        }
    }
}
