/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.Serializable;

/**
 * Basic interface for stream operators. Implementers would implement one of {@link
 * org.apache.flink.streaming.api.operators.OneInputStreamOperator} or {@link
 * org.apache.flink.streaming.api.operators.TwoInputStreamOperator} to create operators that process
 * elements.
 *
 * <p>The class {@link org.apache.flink.streaming.api.operators.AbstractStreamOperator} offers
 * default implementation for the lifecycle and properties methods.
 *
 * <p>Methods of {@code StreamOperator} are guaranteed not to be called concurrently. Also, if using
 * the timer service, timer callbacks are also guaranteed not to be called concurrently with methods
 * on {@code StreamOperator}.
 *
 * @param <OUT> The output type of the operator
 */
@PublicEvolving
public interface StreamOperator<OUT> extends CheckpointListener, KeyContext, Serializable {

    // ------------------------------------------------------------------------
    //  life cycle
    // ------------------------------------------------------------------------

    /**
     * This method is called immediately before any elements are processed, it should contain the
     * operator's initialization logic.
     *
     * @implSpec In case of recovery, this method needs to ensure that all recovered data is
     *     processed before passing back control, so that the order of elements is ensured during
     *     the recovery of an operator chain (operators are opened from the tail operator to the
     *     head operator).
     * @throws java.lang.Exception An exception in this method causes the operator to fail.
     */
    void open() throws Exception;

    /**
     * This method is called at the end of data processing.
     *
     * <p>The method is expected to flush all remaining buffered data. Exceptions during this
     * flushing of buffered data should be propagated, in order to cause the operation to be
     * recognized as failed, because the last data items are not processed properly.
     *
     * <p><b>After this method is called, no more records can be produced for the downstream
     * operators.</b>
     *
     * <p><b>WARNING:</b> It is not safe to use this method to commit any transactions or other side
     * effects! You can use this method to flush any buffered data that can later on be committed
     * e.g. in a {@link StreamOperator#notifyCheckpointComplete(long)}.
     *
     * <p><b>NOTE:</b>This method does not need to close any resources. You should release external
     * resources in the {@link #close()} method.
     *
     * @throws java.lang.Exception An exception in this method causes the operator to fail.
     */
    void finish() throws Exception;

    /**
     * This method is called at the very end of the operator's life, both in the case of a
     * successful completion of the operation, and in the case of a failure and canceling.
     *
     * <p>This method is expected to make a thorough effort to release all resources that the
     * operator has acquired.
     *
     * <p><b>NOTE:</b>It can not emit any records! If you need to emit records at the end of
     * processing, do so in the {@link #finish()} method.
     */
    void close() throws Exception;

    // ------------------------------------------------------------------------
    //  state snapshots
    // ------------------------------------------------------------------------

    /**
     * This method is called when the operator should do a snapshot, before it emits its own
     * checkpoint barrier.
     *
     * <p>This method is intended not for any actual state persistence, but only for emitting some
     * data before emitting the checkpoint barrier. Operators that maintain some small transient
     * state that is inefficient to checkpoint (especially when it would need to be checkpointed in
     * a re-scalable way) but can simply be sent downstream before the checkpoint. An example are
     * opportunistic pre-aggregation operators, which have small the pre-aggregation state that is
     * frequently flushed downstream.
     *
     * <p><b>Important:</b> This method should not be used for any actual state snapshot logic,
     * because it will inherently be within the synchronous part of the operator's checkpoint. If
     * heavy work is done within this method, it will affect latency and downstream checkpoint
     * alignments.
     *
     * @param checkpointId The ID of the checkpoint.
     * @throws Exception Throwing an exception here causes the operator to fail and go into
     *     recovery.
     */
    void prepareSnapshotPreBarrier(long checkpointId) throws Exception;

    /**
     * Called to draw a state snapshot from the operator.
     *
     * @return a runnable future to the state handle that points to the snapshotted state. For
     *     synchronous implementations, the runnable might already be finished.
     * @throws Exception exception that happened during snapshotting.
     */
    OperatorSnapshotFutures snapshotState(
            long checkpointId,
            long timestamp,
            CheckpointOptions checkpointOptions,
            CheckpointStreamFactory storageLocation)
            throws Exception;

    /** Provides a context to initialize all state in the operator. */
    void initializeState(StreamTaskStateInitializer streamTaskStateManager) throws Exception;

    // ------------------------------------------------------------------------
    //  miscellaneous
    // ------------------------------------------------------------------------

    void setKeyContextElement1(StreamRecord<?> record) throws Exception;

    void setKeyContextElement2(StreamRecord<?> record) throws Exception;

    OperatorMetricGroup getMetricGroup();

    OperatorID getOperatorID();
}
