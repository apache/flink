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

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * A mock {@link Source} for unit tests.
 */
public class MockSource implements Source<Integer, MockSourceSplit, Set<MockSourceSplit>> {
	private final Boundedness boundedness;
	private final int numSplits;
	private List<MockSourceReader> createdReaders;

	public MockSource(Boundedness boundedness, int numSplits) {
		this.boundedness = boundedness;
		this.numSplits = numSplits;
		this.createdReaders = new ArrayList<>();
	}

	@Override
	public Boundedness getBoundedness() {
		return boundedness;
	}

	@Override
	public SourceReader<Integer, MockSourceSplit> createReader(SourceReaderContext readerContext) {
		MockSourceReader mockSourceReader = new MockSourceReader();
		createdReaders.add(mockSourceReader);
		return mockSourceReader;
	}

	@Override
	public SplitEnumerator<MockSourceSplit, Set<MockSourceSplit>> createEnumerator(SplitEnumeratorContext<MockSourceSplit> enumContext) {
		return new MockSplitEnumerator(numSplits, enumContext);
	}

	@Override
	public SplitEnumerator<MockSourceSplit, Set<MockSourceSplit>> restoreEnumerator(
			SplitEnumeratorContext<MockSourceSplit> enumContext,
			Set<MockSourceSplit> checkpoint) throws IOException {
		return new MockSplitEnumerator(checkpoint, enumContext);
	}

	@Override
	public SimpleVersionedSerializer<MockSourceSplit> getSplitSerializer() {
		return new MockSourceSplitSerializer();
	}

	@Override
	public SimpleVersionedSerializer<Set<MockSourceSplit>> getEnumeratorCheckpointSerializer() {
		return new MockSplitEnumeratorCheckpointSerializer();
	}

	// --------------- methods for testing -------------

	public List<MockSourceReader> getCreatedReaders() {
		return createdReaders;
	}
}
