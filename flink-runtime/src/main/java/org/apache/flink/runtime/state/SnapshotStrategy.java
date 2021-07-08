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

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nonnull;

/**
 * Interface for different snapshot approaches in state backends. Implementing classes should
 * ideally be stateless or at least threadsafe, it can be called in parallel by multiple
 * checkpoints.
 *
 * <p>The interface can be later on executed in a synchronous or asynchronous manner. See {@link
 * SnapshotStrategyRunner}.
 *
 * @param <S> type of the returned state object that represents the result of the snapshot
 *     operation.
 * @param <SR> type of produced resources in the synchronous part.
 */
@Internal
public interface SnapshotStrategy<S extends StateObject, SR extends SnapshotResources> {

    /**
     * Performs the synchronous part of the snapshot. It returns resources which can be later on
     * used in the asynchronous part.
     *
     * @param checkpointId The ID of the checkpoint.
     * @return Resources needed to finish the snapshot.
     */
    SR syncPrepareResources(long checkpointId) throws Exception;

    /**
     * Operation that writes a snapshot into a stream that is provided by the given {@link
     * CheckpointStreamFactory} and returns a @{@link SupplierWithException} that gives a state
     * handle to the snapshot.
     *
     * @param checkpointId The ID of the checkpoint.
     * @param timestamp The timestamp of the checkpoint.
     * @param streamFactory The factory that we can use for writing our state to streams.
     * @param checkpointOptions Options for how to perform this checkpoint.
     * @return A supplier that will yield a {@link StateObject}.
     */
    SnapshotResultSupplier<S> asyncSnapshot(
            SR syncPartResource,
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions);

    /**
     * A supplier for a {@link SnapshotResult} with an access to a {@link CloseableRegistry} for io
     * tasks that need to be closed when cancelling the async part of the checkpoint.
     *
     * @param <S> type of the returned state object that represents the result of the snapshot *
     *     operation.
     */
    @FunctionalInterface
    interface SnapshotResultSupplier<S extends StateObject> {
        /**
         * Performs the asynchronous part of a checkpoint and returns the snapshot result.
         *
         * @param snapshotCloseableRegistry A registry for io tasks to close on cancel.
         * @return A snapshot result
         */
        SnapshotResult<S> get(CloseableRegistry snapshotCloseableRegistry) throws Exception;
    }
}
