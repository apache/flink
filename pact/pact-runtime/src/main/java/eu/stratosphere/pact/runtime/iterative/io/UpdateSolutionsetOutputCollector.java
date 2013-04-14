/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.runtime.hash.MutableHashTable;

import java.io.IOException;

public class UpdateSolutionsetOutputCollector<T> implements Collector<T> {

	private final Collector<T> delegate;

	// TODO type safety
	private MutableHashTable.HashBucketIterator<T, ?> hashBucket;

	private long numUpdatedElements;

	public UpdateSolutionsetOutputCollector(Collector<T> delegate) {
		this.delegate = delegate;
		numUpdatedElements = 0;
	}

	public void setHashBucket(MutableHashTable.HashBucketIterator<T, ?> hashBucket) {
		this.hashBucket = hashBucket;
	}

	@Override
	public void collect(T record) {
		try {
			hashBucket.writeBack(record);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		delegate.collect(record);
		numUpdatedElements++;
	}

	public long getNumUpdatedElementsAndReset() {
		long numUpdatedElementsToReturn = numUpdatedElements;
		numUpdatedElements = 0;
		return numUpdatedElementsToReturn;
	}

	@Override
	public void close() {
	}
}
