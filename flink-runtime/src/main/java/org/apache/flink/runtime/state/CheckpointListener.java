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

/**
 * This interface must be implemented by functions/operations that want to receive
 * a commit notification once a checkpoint has been completely acknowledged by all
 * participants.
 */
@PublicEvolving
public interface CheckpointListener {

	/**
	 * This method is called as a notification once a distributed checkpoint has been completed.
	 * 
	 * Note that any exception during this method will not cause the checkpoint to
	 * fail any more.
	 * 
	 * @param checkpointId The ID of the checkpoint that has been completed.
	 * @throws Exception
	 */
	void notifyCheckpointComplete(long checkpointId) throws Exception;

	/**
	 * This method is called as a notification once a distributed checkpoint has been aborted.
	 *
	 * @param checkpointId The ID of the checkpoint that has been aborted.
	 * @throws Exception
	 */
	void notifyCheckpointAborted(long checkpointId) throws Exception;
}
