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

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
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

/** A mock {@link Source} for unit tests. */
public class MockSource implements Source<Integer, MockSourceSplit, Set<MockSourceSplit>> {

    private static final long serialVersionUID = 1L;

    private final Boundedness boundedness;
    private final int numSplits;
    private final MockSourceReader.WaitingForSplits readerWaitingForMoreSplits;
    private final boolean readerMarkIdleOnNoSplits;
    protected List<MockSourceReader> createdReaders;

    public MockSource(Boundedness boundedness, int numSplits) {
        this(boundedness, numSplits, false, false);
    }

    public MockSource(
            Boundedness boundedness,
            int numSplits,
            boolean readerWaitingForMoreSplits,
            boolean readerMarkIdleOnNoSplits) {
        this(
                boundedness,
                numSplits,
                readerWaitingForMoreSplits
                        ? MockSourceReader.WaitingForSplits.WAIT_UNTIL_ALL_SPLITS_ASSIGNED
                        : MockSourceReader.WaitingForSplits.DO_NOT_WAIT_FOR_SPLITS,
                readerMarkIdleOnNoSplits);
    }

    private MockSource(
            Boundedness boundedness,
            int numSplits,
            MockSourceReader.WaitingForSplits readerWaitingForSplitsBehaviour,
            boolean readerMarkIdleOnNoSplits) {
        this.boundedness = boundedness;
        this.numSplits = numSplits;
        this.createdReaders = new ArrayList<>();
        this.readerWaitingForMoreSplits = readerWaitingForSplitsBehaviour;
        this.readerMarkIdleOnNoSplits = readerMarkIdleOnNoSplits;
    }

    @Override
    public Boundedness getBoundedness() {
        return boundedness;
    }

    @Override
    public SourceReader<Integer, MockSourceSplit> createReader(SourceReaderContext readerContext) {
        MockSourceReader mockSourceReader =
                new MockSourceReader(readerWaitingForMoreSplits, readerMarkIdleOnNoSplits);
        createdReaders.add(mockSourceReader);
        return mockSourceReader;
    }

    @Override
    public SplitEnumerator<MockSourceSplit, Set<MockSourceSplit>> createEnumerator(
            SplitEnumeratorContext<MockSourceSplit> enumContext) {
        return new MockSplitEnumerator(numSplits, enumContext);
    }

    @Override
    public SplitEnumerator<MockSourceSplit, Set<MockSourceSplit>> restoreEnumerator(
            SplitEnumeratorContext<MockSourceSplit> enumContext, Set<MockSourceSplit> checkpoint)
            throws IOException {
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

    public static Builder continuous(int numSplits) {
        return new Builder(Boundedness.CONTINUOUS_UNBOUNDED, numSplits);
    }

    public static Builder bounded(int numSplits) {
        return new Builder(Boundedness.BOUNDED, numSplits);
    }

    /** A builder for {@link MockSource}. */
    public static class Builder {
        private final Boundedness boundedness;
        private final int numSplits;
        private MockSourceReader.WaitingForSplits readerWaitingForSplitsBehaviour =
                MockSourceReader.WaitingForSplits.WAIT_FOR_INITIAL;
        private boolean readerMarkIdleOnNoSplits = true;

        private Builder(Boundedness boundedness, int numSplits) {
            this.boundedness = boundedness;
            this.numSplits = numSplits;
        }

        /**
         * Instructs the {@link MockSourceReader} to not finish if there has been no splits
         * assignment messages yet.
         *
         * @see #waitUntilAllSplitsAssigned()
         * @see #doNotWaitForSplitsAssignment()
         */
        public Builder waitOnlyForInitialSplits() {
            this.readerWaitingForSplitsBehaviour =
                    MockSourceReader.WaitingForSplits.WAIT_FOR_INITIAL;
            return this;
        }

        /**
         * Instructs the {@link MockSourceReader} to finish only if the {@link MockSplitEnumerator}
         * assigned all splits and there will be no more splits distributed.
         *
         * @see #waitUntilAllSplitsAssigned()
         * @see #doNotWaitForSplitsAssignment()
         */
        public Builder waitUntilAllSplitsAssigned() {
            this.readerWaitingForSplitsBehaviour =
                    MockSourceReader.WaitingForSplits.WAIT_UNTIL_ALL_SPLITS_ASSIGNED;
            return this;
        }

        /**
         * Instructs the {@link MockSourceReader} to finish irrespective if there can be splits
         * assigned in the future or not.
         *
         * @see #waitUntilAllSplitsAssigned()
         * @see #doNotWaitForSplitsAssignment()
         */
        public Builder doNotWaitForSplitsAssignment() {
            this.readerWaitingForSplitsBehaviour =
                    MockSourceReader.WaitingForSplits.DO_NOT_WAIT_FOR_SPLITS;
            return this;
        }

        /**
         * If enabled the {@link MockSourceReader} will mark the {@link ReaderOutput#markIdle()
         * idle} if it has no splits assigned.
         */
        public Builder markReaderIdleOnNoSplits(boolean enable) {
            this.readerMarkIdleOnNoSplits = enable;
            return this;
        }

        public MockSource build() {
            return new MockSource(
                    boundedness,
                    numSplits,
                    readerWaitingForSplitsBehaviour,
                    readerMarkIdleOnNoSplits);
        }
    }

    // --------------- methods for testing -------------

    public List<MockSourceReader> getCreatedReaders() {
        return createdReaders;
    }
}
