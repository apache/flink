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

package org.apache.flink.test.util.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.util.List;

/**
 * Base split enumerator for test sources that provides sensible defaults for most methods.
 *
 * <p>By default, this enumerator immediately signals no more splits to ensure bounded completion.
 * Test sources can extend this class and only override the methods they need, typically just {@link
 * #snapshotState(long)} for checkpointing behavior or {@link #addReader(int)} for custom split
 * assignment.
 *
 * <p>For common patterns, use the concrete implementations:
 *
 * <ul>
 *   <li>{@link SingleSplitEnumerator} - First reader gets one split, others get nothing
 * </ul>
 *
 * @param <EnumChkptState> The type of the enumerator checkpoint state
 */
@PublicEvolving
public class TestSplitEnumerator<EnumChkptState>
        implements SplitEnumerator<TestSplit, EnumChkptState> {

    protected final SplitEnumeratorContext<TestSplit> context;
    protected final EnumChkptState checkpointState;

    public TestSplitEnumerator(
            SplitEnumeratorContext<TestSplit> context, EnumChkptState checkpointState) {
        this.context = context;
        this.checkpointState = checkpointState;
    }

    @Override
    public void start() {
        // No-op implementation
    }

    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
        // Most common case: split request means reader is ready for work
        addReader(subtaskId);
    }

    @Override
    public void addSplitsBack(List<TestSplit> splits, int subtaskId) {
        // No-op implementation
    }

    @Override
    public void addReader(int subtaskId) {
        // Signal no more splits to ensure immediate completion
        context.signalNoMoreSplits(subtaskId);
    }

    @Override
    public void close() {
        // No-op implementation
    }

    /**
     * Subclasses should override this method to provide their checkpoint state. The default
     * implementation returns the checkpoint state passed in the constructor.
     */
    @Override
    public EnumChkptState snapshotState(long checkpointId) {
        return checkpointState;
    }
}
