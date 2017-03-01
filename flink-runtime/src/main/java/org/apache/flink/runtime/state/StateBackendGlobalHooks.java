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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.util.FlinkException;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * An interface that {@link StateBackend State Backends} can optionally implement to offer
 * "fast paths" and additional hooks around checkpoint/savepoint initialization and cleanup.
 */
@PublicEvolving
public interface StateBackendGlobalHooks extends StateBackend {

	/**
	 * A fast-path savepoint dispose operation given a savepoint pointer.
	 * 
	 * @param pointer The pointer to the savepoint.
	 * @throws IOException Thrown, if the disposal fails due to an I/O error.
	 */
	void disposeSavepoint(String pointer) throws FlinkException, IOException;

	// ------------------------------------------------------------------------

	/**
	 * Creates a state-backend specific hook that can be called to dispose the given checkpoint.
	 * This hook would typically be a fast path to dispose the checkpoint state, over the
	 * disposal of each individual state.
	 * 
	 * <p>For example, for a file based state backend where all state of a checkpoint is in the
	 * same directory, the hook could simply delete the directory recursively, which may be more
	 * efficient triggering a request to delete each file individually. 
	 * 
	 * @param checkpoint The checkpoint whose state the hook should dispose  
	 * @return The disposal hook.
	 */
	@Nullable
	StateDisposeHook createCheckpointDisposeHook(CompletedCheckpoint checkpoint) throws FlinkException, IOException;

	// ------------------------------------------------------------------------
	//  callbacks for disposal
	// ------------------------------------------------------------------------

	/**
	 * A functional interface for a hook triggered to dispose the state of a checkpoint.
	 */
	interface StateDisposeHook {

		/**
		 * Called when the checkpoint's state should be disposed.
		 */
		void disposeCheckpointState() throws FlinkException, IOException;
	}
}
