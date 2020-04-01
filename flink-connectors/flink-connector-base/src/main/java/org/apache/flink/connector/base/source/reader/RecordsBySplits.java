/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.flink.connector.base.source.reader;

import org.apache.flink.api.connector.source.SourceSplit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * An implementation of RecordsWithSplitIds to host all the records by splits.
 */
public class RecordsBySplits<E> implements RecordsWithSplitIds<E> {
	private Map<String, Collection<E>> recordsBySplits = new LinkedHashMap<>();
	private Set<String> finishedSplits = new HashSet<>();

	/**
	 * Add the record from the given split ID.
	 *
	 * @param splitId the split ID the record was from.
	 * @param record the record to add.
	 */
	public void add(String splitId, E record) {
		recordsBySplits.computeIfAbsent(splitId, sid -> new ArrayList<>()).add(record);
	}

	/**
	 * Add the record from the given source split.
	 *
	 * @param split the source split the record was from.
	 * @param record the record to add.
	 */
	public void add(SourceSplit split, E record) {
		add(split.splitId(), record);
	}

	/**
	 * Add multiple records from the given split ID.
	 *
	 * @param splitId the split ID given the records were from.
	 * @param records the records to add.
	 */
	public void addAll(String splitId, Collection<E> records) {
		this.recordsBySplits.compute(splitId, (id, r) -> {
			if (r == null) {
				r = records;
			} else {
				r.addAll(records);
			}
			return r;
		});
	}

	/**
	 * Add multiple records from the given source split.
	 *
	 * @param split the source split the records were from.
	 * @param records the records to add.
	 */
	public void addAll(SourceSplit split, Collection<E> records) {
		addAll(split.splitId(), records);
	}

	/**
	 * Mark the split with the given ID as finished.
	 *
	 * @param splitId the ID of the finished split.
	 */
	public void addFinishedSplit(String splitId) {
		finishedSplits.add(splitId);
	}

	/**
	 * Mark multiple splits with the given IDs as finished.
	 *
	 * @param splitIds the IDs of the finished splits.
	 */
	public void addFinishedSplits(Collection<String> splitIds) {
		finishedSplits.addAll(splitIds);
	}

	@Override
	public Set<String> finishedSplits() {
		return finishedSplits;
	}

	@Override
	public Collection<String> splitIds() {
		return recordsBySplits.keySet();
	}

	@Override
	public Map<String, Collection<E>> recordsBySplits() {
		return recordsBySplits;
	}
}
