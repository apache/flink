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

package org.apache.flink.api.connector.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.metrics.MetricGroup;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

/**
 * A context class for the {@link SplitEnumerator}. This class serves the following purposes: 1.
 * Host information necessary for the SplitEnumerator to make split assignment decisions. 2. Accept
 * and track the split assignment from the enumerator. 3. Provide a managed threading model so the
 * split enumerators do not need to create their own internal threads.
 *
 * @param <SplitT> the type of the splits.
 */
@PublicEvolving
public interface SplitEnumeratorContext<SplitT extends SourceSplit> {

    MetricGroup metricGroup();

    /**
     * Send a source event to a source reader. The source reader is identified by its subtask id.
     *
     * @param subtaskId the subtask id of the source reader to send this event to.
     * @param event the source event to send.
     */
    void sendEventToSourceReader(int subtaskId, SourceEvent event);

    /**
     * Get the current parallelism of this Source. Note that due to auto-scaling, the parallelism
     * may change over time. Therefore the SplitEnumerator should not cache the return value of this
     * method, but always invoke this method to get the latest parallelism.
     *
     * @return the parallelism of the Source.
     */
    int currentParallelism();

    /**
     * Get the currently registered readers. The mapping is from subtask id to the reader info.
     *
     * @return the currently registered readers.
     */
    Map<Integer, ReaderInfo> registeredReaders();

    /**
     * Assign the splits.
     *
     * @param newSplitAssignments the new split assignments to add.
     */
    void assignSplits(SplitsAssignment<SplitT> newSplitAssignments);

    /**
     * Assigns a single split.
     *
     * <p>When assigning multiple splits, it is more efficient to assign all of them in a single
     * call to the {@link #assignSplits(SplitsAssignment)} method.
     *
     * @param split The new split
     * @param subtask The index of the operator's parallel subtask that shall receive the split.
     */
    default void assignSplit(SplitT split, int subtask) {
        assignSplits(new SplitsAssignment<>(split, subtask));
    }

    /**
     * Signals a subtask that it will not receive any further split.
     *
     * @param subtask The index of the operator's parallel subtask that shall be signaled it will
     *     not receive any further split.
     */
    void signalNoMoreSplits(int subtask);

    /**
     * Invoke the callable and handover the return value to the handler which will be executed by
     * the source coordinator. When this method is invoked multiple times, The <code>Callable</code>
     * s may be executed in a thread pool concurrently.
     *
     * <p>It is important to make sure that the callable does not modify any shared state,
     * especially the states that will be a part of the {@link SplitEnumerator#snapshotState()}.
     * Otherwise the there might be unexpected behavior.
     *
     * <p>Note that an exception thrown from the handler would result in failing the job.
     *
     * @param callable a callable to call.
     * @param handler a handler that handles the return value of or the exception thrown from the
     *     callable.
     */
    <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler);

    /**
     * Invoke the given callable periodically and handover the return value to the handler which
     * will be executed by the source coordinator. When this method is invoked multiple times, The
     * <code>Callable</code>s may be executed in a thread pool concurrently.
     *
     * <p>It is important to make sure that the callable does not modify any shared state,
     * especially the states that will be a part of the {@link SplitEnumerator#snapshotState()}.
     * Otherwise the there might be unexpected behavior.
     *
     * <p>Note that an exception thrown from the handler would result in failing the job.
     *
     * @param callable the callable to call.
     * @param handler a handler that handles the return value of or the exception thrown from the
     *     callable.
     * @param initialDelay the initial delay of calling the callable.
     * @param period the period between two invocations of the callable.
     */
    <T> void callAsync(
            Callable<T> callable, BiConsumer<T, Throwable> handler, long initialDelay, long period);

    /**
     * Invoke the given runnable in the source coordinator thread.
     *
     * <p>This can be useful when the enumerator needs to execute some action (like assignSplits)
     * triggered by some external events. E.g., Watermark from another source advanced and this
     * source now be able to assign splits to awaiting readers. The trigger can be initiated from
     * the coordinator thread of the other source. Instead of using lock for thread safety, this API
     * allows to run such externally triggered action in the coordinator thread. Hence, we can
     * ensure all enumerator actions are serialized in the single coordinator thread.
     *
     * <p>It is important that the runnable does not block.
     *
     * @param runnable a runnable to execute
     */
    void runInCoordinatorThread(Runnable runnable);
}
