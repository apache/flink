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

package org.apache.flink.connector.base.source.reader.mocks;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A mock implementation of {@link RecordsWithSplitIds} that returns a given set of records.
 */
public class TestingRecordsWithSplitIds<E> implements RecordsWithSplitIds<E> {

	private final Map<String, Collection<E>> records;

	private final String splitId;

	private volatile boolean isRecycled;

	@SafeVarargs
	public TestingRecordsWithSplitIds(String splitId, E... records) {
		this.splitId = checkNotNull(splitId);
		this.records = new HashMap<>();
		this.records.put(splitId, Arrays.asList(records));
	}

	@Override
	public Collection<String> splitIds() {
		return Collections.singleton(splitId);
	}

	@Override
	public Map<String, Collection<E>> recordsBySplits() {
		return records;
	}

	@Override
	public Set<String> finishedSplits() {
		return Collections.emptySet();
	}

	@Override
	public void recycle() {
		isRecycled = true;
	}

	public boolean isRecycled() {
		return isRecycled;
	}
}
