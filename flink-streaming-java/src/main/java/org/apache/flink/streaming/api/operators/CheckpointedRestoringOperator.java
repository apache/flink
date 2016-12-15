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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

/**
 * Interface for {@link StreamOperator StreamOperators} that can restore from a Flink 1.1
 * legacy snapshot that was done using the {@link StreamCheckpointedOperator} interface.
 */
@Deprecated
public interface CheckpointedRestoringOperator {

	/**
	 * Restores the operator state, if this operator's execution is recovering from a checkpoint.
	 * This method restores the operator state (if the operator is stateful) and the key/value state
	 * (if it had been used and was initialized when the snapshot occurred).
	 *
	 * <p>This method is called after {@link StreamOperator#setup(StreamTask, StreamConfig, Output)}
	 * and before {@link StreamOperator#open()}.
	 *
	 * @param in The stream from which we have to restore our state.
	 *
	 * @throws Exception Exceptions during state restore should be forwarded, so that the system can
	 *                   properly react to failed state restore and fail the execution attempt.
	 */
	void restoreState(FSDataInputStream in) throws Exception;
}
