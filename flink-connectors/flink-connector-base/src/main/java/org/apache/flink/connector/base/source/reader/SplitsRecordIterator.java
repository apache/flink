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

package org.apache.flink.connector.base.source.reader;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * A class that wraps around a {@link RecordsWithSplitIds} and provide a consistent iterator.
 */
public class SplitsRecordIterator<E> {
	private final Map<String, Collection<E>> recordsBySplits;
	private final Set<String> finishedSplitIds;
	private final Iterator<Map.Entry<String, Collection<E>>> splitIter;
	private String currentSplitId;
	private Iterator<E> recordsIter;

	/**
	 * Construct a cross-splits iterator for the records.
	 *
	 * @param recordsWithSplitIds the records by splits.
	 */
	public SplitsRecordIterator(RecordsWithSplitIds<E> recordsWithSplitIds) {
		this.recordsBySplits = recordsWithSplitIds.recordsBySplits();
		// Remove empty splits;
		recordsBySplits.entrySet().removeIf(e -> e.getValue().isEmpty());
		this.splitIter = recordsBySplits.entrySet().iterator();
		this.finishedSplitIds = recordsWithSplitIds.finishedSplits();
	}

	/**
	 * Whether their are more records available.
	 *
	 * @return true if there are more records, false otherwise.
	 */
	public boolean hasNext() {
		if (recordsIter == null || !recordsIter.hasNext()) {
			if (splitIter.hasNext()) {
				Map.Entry<String, Collection<E>> entry = splitIter.next();
				currentSplitId = entry.getKey();
				recordsIter = entry.getValue().iterator();
			} else {
				return false;
			}
		}
		return recordsIter.hasNext() || splitIter.hasNext();
	}

	/**
	 * Get the next record.
	 * @return the next record.
	 */
	public E next() {
		if (!hasNext()) {
			throw new NoSuchElementException();
		}
		return recordsIter.next();
	}

	/**
	 * Get the split id of the last returned record.
	 *
	 * @return the split id of the last returned record.
	 */
	public String currentSplitId() {
		return currentSplitId;
	}

	/**
	 * The split Ids that are finished after all the records in this iterator are emitted.
	 *
	 * @return a set of finished split Ids.
	 */
	public Set<String> finishedSplitIds() {
		return finishedSplitIds;
	}
}
