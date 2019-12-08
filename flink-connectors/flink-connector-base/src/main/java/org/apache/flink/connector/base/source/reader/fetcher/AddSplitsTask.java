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

package org.apache.flink.connector.base.source.reader.fetcher;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * The task to add splits.
 */
class AddSplitsTask<SplitT extends SourceSplit> implements SplitFetcherTask {
	private final SplitReader<?, SplitT> splitReader;
	private final List<SplitT> splitsToAdd;
	private final Queue<SplitsChange<SplitT>> splitsChanges;
	private final Map<String, SplitT> assignedSplits;
	private boolean splitsChangesAdded;

	AddSplitsTask(
			SplitReader<?, SplitT> splitReader,
			List<SplitT> splitsToAdd,
			Queue<SplitsChange<SplitT>> splitsChanges,
			Map<String, SplitT> assignedSplits) {
		this.splitReader = splitReader;
		this.splitsToAdd = splitsToAdd;
		this.splitsChanges = splitsChanges;
		this.assignedSplits = assignedSplits;
		this.splitsChangesAdded = false;
	}

	@Override
	public boolean run() throws InterruptedException {
		if (!splitsChangesAdded) {
			splitsChanges.add(new SplitsAddition<>(splitsToAdd));
			splitsToAdd.forEach(s -> assignedSplits.put(s.splitId(), s));
			splitsChangesAdded = true;
		}
		splitReader.handleSplitsChanges(splitsChanges);
		return splitsChanges.isEmpty();
	}

	@Override
	public void wakeUp() {
		// Do nothing.
	}

	@Override
	public String toString() {
		return String.format("AddSplitsTask: [%s]", splitsToAdd);
	}
}
