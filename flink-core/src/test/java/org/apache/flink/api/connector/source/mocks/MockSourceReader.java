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

package org.apache.flink.api.connector.source.mocks;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.core.io.InputStatus;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A mock {@link SourceReader} for unit tests.
 */
public class MockSourceReader implements SourceReader<Integer, MockSourceSplit> {
	private final List<MockSourceSplit> assignedSplits = new ArrayList<>();
	private final List<SourceEvent> receivedSourceEvents = new ArrayList<>();

	private int currentSplitIndex = 0;
	private boolean started;
	private boolean closed;

	@GuardedBy("this")
	private CompletableFuture<Void> availableFuture;

	public MockSourceReader() {
		this.started = false;
		this.closed = false;
		this.availableFuture = CompletableFuture.completedFuture(null);
	}

	@Override
	public void start() {
		this.started = true;
	}

	@Override
	public InputStatus pollNext(ReaderOutput<Integer> sourceOutput) throws Exception {
		boolean finished = true;
		currentSplitIndex = 0;
		// Find first splits with available records.
		while (currentSplitIndex < assignedSplits.size()
				&& !assignedSplits.get(currentSplitIndex).isAvailable()) {
			finished &= assignedSplits.get(currentSplitIndex).isFinished();
			currentSplitIndex++;
		}
		// Read from the split with available record.
		if (currentSplitIndex < assignedSplits.size()) {
			sourceOutput.collect(assignedSplits.get(currentSplitIndex).getNext(false)[0]);
			return InputStatus.MORE_AVAILABLE;
		} else if (finished) {
			// In case no split has available record, return depending on whether all the splits has finished.
			return InputStatus.END_OF_INPUT;
		}
		else {
			markUnavailable();
			return InputStatus.NOTHING_AVAILABLE;
		}
	}

	@Override
	public List<MockSourceSplit> snapshotState() {
		return assignedSplits;
	}

	@Override
	public synchronized CompletableFuture<Void> isAvailable() {
		return availableFuture;
	}

	@Override
	public void addSplits(List<MockSourceSplit> splits) {
		assignedSplits.addAll(splits);
		markAvailable();
	}

	@Override
	public void handleSourceEvents(SourceEvent sourceEvent) {
		receivedSourceEvents.add(sourceEvent);
	}

	@Override
	public void close() throws Exception {
		this.closed = true;
	}

	private synchronized void markUnavailable() {
		if (availableFuture.isDone()) {
			availableFuture = new CompletableFuture<>();
		}
	}

	// --------------- methods for unit tests ---------------

	public void markAvailable() {
		CompletableFuture<?> toNotify = null;
		synchronized (this) {
			if (!availableFuture.isDone()) {
				toNotify =  availableFuture;
			}
		}
		if (toNotify != null) {
			toNotify.complete(null);
		}
	}

	public boolean isStarted() {
		return started;
	}

	public boolean isClosed() {
		return closed;
	}

	public List<MockSourceSplit> getAssignedSplits() {
		return assignedSplits;
	}

	public List<SourceEvent> getReceivedSourceEvents() {
		return receivedSourceEvents;
	}
}
