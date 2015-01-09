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

import java.io.IOException;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.operators.hash.CompactingHashTable;
import org.apache.flink.util.Collector;

/**
 * A {@link Collector} to update the solution set of a workset iteration.
 * <p>
 * The records are written to a HashTable hash table to allow in-memory point updates.
 * <p>
 * Assumption for fast updates: the build side iterator of the hash table is already positioned for the update. This
 * is for example the case when a solution set update happens directly after a solution set join. If this assumption
 * doesn't hold, use {@link SolutionSetUpdateOutputCollector}, which probes the hash table before updating.
 */
public class SolutionSetFastUpdateOutputCollector<T> implements Collector<T> {

	private final Collector<T> delegate;

	private final CompactingHashTable<T> solutionSet;
	
	private final T tmpHolder;

	public SolutionSetFastUpdateOutputCollector(CompactingHashTable<T> solutionSet, TypeSerializer<T> serializer) {
		this(solutionSet, serializer, null);
	}

	public SolutionSetFastUpdateOutputCollector(CompactingHashTable<T> solutionSet, TypeSerializer<T> serializer, Collector<T> delegate) {
		this.solutionSet = solutionSet;
		this.delegate = delegate;
		this.tmpHolder = serializer.createInstance();
	}

	@Override
	public void collect(T record) {
		try {
			solutionSet.insertOrReplaceRecord(record, tmpHolder);
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
