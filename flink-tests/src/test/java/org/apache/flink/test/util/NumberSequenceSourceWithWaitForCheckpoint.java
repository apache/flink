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

package org.apache.flink.test.util;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceEnumerator;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceReader;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceSplit;
import org.apache.flink.core.io.InputStatus;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

/**
 * A {@link NumberSequenceSource} guaranteeing that a checkpoint has been invoked before the source
 * ends its input.
 */
public class NumberSequenceSourceWithWaitForCheckpoint extends NumberSequenceSource {
    private static final long serialVersionUID = 1L;

    private final int numSplits;
    private final long numAllowedMessageBeforeCheckpoint;

    public NumberSequenceSourceWithWaitForCheckpoint(long from, long to, int numSplits) {
        super(from, to);
        this.numSplits = numSplits;
        this.numAllowedMessageBeforeCheckpoint = (to - from) / numSplits;
    }

    @Override
    public SplitEnumerator<NumberSequenceSplit, Collection<NumberSequenceSplit>> createEnumerator(
            final SplitEnumeratorContext<NumberSequenceSplit> enumContext) {
        final List<NumberSequenceSplit> splits = splitNumberRange(getFrom(), getTo(), numSplits);
        return new AssignAfterCheckpointEnumerator<>(enumContext, splits);
    }

    @Override
    public SourceReader<Long, NumberSequenceSplit> createReader(SourceReaderContext readerContext) {
        return new CheckpointListeningIteratorSourceReader<>(
                readerContext, numAllowedMessageBeforeCheckpoint);
    }

    /**
     * This is an enumerator for the {@link NumberSequenceSource}, which only responds to the split
     * requests after the next checkpoint is complete. That way, we naturally draw the split
     * processing across checkpoints without artificial sleep statements.
     */
    private static final class AssignAfterCheckpointEnumerator<
                    SplitT extends IteratorSourceSplit<?, ?>>
            extends IteratorSourceEnumerator<SplitT> {
        private final Queue<Integer> pendingRequests = new ArrayDeque<>();
        private final SplitEnumeratorContext<?> context;

        public AssignAfterCheckpointEnumerator(
                SplitEnumeratorContext<SplitT> context, Collection<SplitT> splits) {
            super(context, splits);
            this.context = context;
        }

        @Override
        public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
            pendingRequests.add(subtaskId);
        }

        @Override
        public Collection<SplitT> snapshotState(long checkpointId) throws Exception {
            // this will be enqueued in the enumerator thread, so it will actually run after this
            // method (the snapshot operation) is complete!
            context.runInCoordinatorThread(this::fullFillPendingRequests);

            return super.snapshotState(checkpointId);
        }

        private void fullFillPendingRequests() {
            for (int subtask : pendingRequests) {
                // respond only to requests for which we still have registered readers
                if (!context.registeredReaders().containsKey(subtask)) {
                    continue;
                }
                super.handleSplitRequest(subtask, null);
            }
            pendingRequests.clear();
        }
    }

    private static class CheckpointListeningIteratorSourceReader<
                    E, IterT extends Iterator<E>, SplitT extends IteratorSourceSplit<E, IterT>>
            extends IteratorSourceReader<E, IterT, SplitT> {
        private boolean checkpointed = false;
        private long messagesProduced = 0;
        private final long numAllowedMessageBeforeCheckpoint;

        public CheckpointListeningIteratorSourceReader(
                SourceReaderContext context, long waitForCheckpointAfterMessages) {
            super(context);
            this.numAllowedMessageBeforeCheckpoint = waitForCheckpointAfterMessages;
        }

        @Override
        public InputStatus pollNext(ReaderOutput<E> output) {
            if (messagesProduced < numAllowedMessageBeforeCheckpoint || checkpointed) {
                messagesProduced++;
                return super.pollNext(output);
            } else {
                return InputStatus.NOTHING_AVAILABLE;
            }
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            checkpointed = true;
        }
    }
}
