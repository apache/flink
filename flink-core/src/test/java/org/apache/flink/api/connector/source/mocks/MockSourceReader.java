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

package org.apache.flink.api.connector.source.mocks;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.core.io.InputStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A mock {@link SourceReader} for unit tests.
 */
public class MockSourceReader implements SourceReader<Integer, MockSourceSplit> {
	private final AtomicReference<CompletableFuture<Void>> availableRef;
	private List<MockSourceSplit> assignedSplits;
	private List<SourceEvent> receivedSourceEvents;
	private int currentSplitIndex = 0;
	private boolean started;
	private boolean closed;

	public MockSourceReader() {
		this.assignedSplits = new ArrayList<>();
		this.receivedSourceEvents = new ArrayList<>();
		this.started = false;
		this.closed = false;
		this.availableRef = new AtomicReference<>();
	}

	@Override
	public void start() {
		this.started = true;
	}

	@Override
	public InputStatus pollNext(SourceOutput<Integer> sourceOutput) throws Exception {
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
		} else {
			// In case no split has available record, return depending on whether all the splits has finished.
			return finished ? InputStatus.END_OF_INPUT : InputStatus.NOTHING_AVAILABLE;
		}
	}

	@Override
	public List<MockSourceSplit> snapshotState() {
		return assignedSplits;
	}

	@Override
	public CompletableFuture<Void> isAvailable() {
		if (currentSplitIndex >= assignedSplits.size()) {
			CompletableFuture<Void> future = new CompletableFuture<>();
			availableRef.compareAndSet(null, future);
			return availableRef.get();
		} else {
			return CompletableFuture.completedFuture(null);
		}
	}

	@Override
	public void addSplits(List<MockSourceSplit> splits) {
		assignedSplits.addAll(splits);
	}

	@Override
	public void handleSourceEvents(SourceEvent sourceEvent) {
		receivedSourceEvents.add(sourceEvent);
	}

	@Override
	public void close() throws Exception {
		this.closed = true;
	}

	// --------------- methods for unit tests ---------------

	public void markAvailable() {
		CompletableFuture<Void> future = availableRef.get();
		if (future != null) {
			future.complete(null);
			availableRef.set(null);
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
