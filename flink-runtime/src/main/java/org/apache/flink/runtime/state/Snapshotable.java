/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.runtime.checkpoint.CheckpointOptions;

import java.util.concurrent.RunnableFuture;

/**
 * Interface for operators that can perform snapshots of their state.
 *
 * @param <S> Generic type of the state object that is created as handle to snapshots.
 * @param <R> Generic type of the state object that used in restore.
 */
public interface Snapshotable<S extends StateObject, R> {

	/**
	 * Operation that writes a snapshot into a stream that is provided by the given {@link CheckpointStreamFactory} and
	 * returns a @{@link RunnableFuture} that gives a state handle to the snapshot. It is up to the implementation if
	 * the operation is performed synchronous or asynchronous. In the later case, the returned Runnable must be executed
	 * first before obtaining the handle.
	 *
	 * @param checkpointId  The ID of the checkpoint.
	 * @param timestamp     The timestamp of the checkpoint.
	 * @param streamFactory The factory that we can use for writing our state to streams.
	 * @param checkpointOptions Options for how to perform this checkpoint.
	 * @return A runnable future that will yield a {@link StateObject}.
	 */
	RunnableFuture<S> snapshot(
			long checkpointId,
			long timestamp,
			CheckpointStreamFactory streamFactory,
			CheckpointOptions checkpointOptions) throws Exception;

	/**
	 * Restores state that was previously snapshotted from the provided parameters. Typically the parameters are state
	 * handles from which the old state is read.
	 *
	 * @param state the old state to restore.
	 */
	void restore(R state) throws Exception;
}
