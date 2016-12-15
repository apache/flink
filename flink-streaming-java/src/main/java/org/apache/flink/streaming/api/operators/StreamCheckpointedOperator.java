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

import org.apache.flink.core.fs.FSDataOutputStream;

@Deprecated
public interface StreamCheckpointedOperator extends CheckpointedRestoringOperator {

	/**
	 * Called to draw a state snapshot from the operator. This method snapshots the operator state
	 * (if the operator is stateful).
	 *
	 * @param out The stream to which we have to write our state.
	 * @param checkpointId The ID of the checkpoint.
	 * @param timestamp The timestamp of the checkpoint.
	 *
	 * @throws Exception Forwards exceptions that occur while drawing snapshots from the operator
	 *                   and the key/value state.
	 */
	void snapshotState(FSDataOutputStream out, long checkpointId, long timestamp) throws Exception;

}
