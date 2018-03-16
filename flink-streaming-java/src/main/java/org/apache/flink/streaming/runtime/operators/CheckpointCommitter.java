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

package org.apache.flink.streaming.runtime.operators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * This class is used to save information about which sink operator instance has committed checkpoints to a backend.
 * <p/>
 * The current checkpointing mechanism is ill-suited for sinks relying on backends that do not support roll-backs.
 * When dealing with such a system, while trying to get exactly-once semantics, one may neither commit data while
 * creating the snapshot (since another sink instance may fail, leading to a replay on the same data) nor when receiving
 * a checkpoint-complete notification (since a subsequent failure would leave us with no knowledge as to whether data
 * was committed or not).
 * <p/>
 * A CheckpointCommitter can be used to solve the second problem by saving whether an instance committed all data
 * belonging to a checkpoint. This data must be stored in a backend that is persistent across retries (which rules
 * out Flink's state mechanism) and accessible from all machines, like a database or distributed file.
 * <p/>
 * There is no mandate as to how the resource is shared; there may be one resource for all Flink jobs, or one for
 * each job/operator/-instance separately. This implies that the resource must not be cleaned up by the system itself,
 * and as such should kept as small as possible.
 */
public abstract class CheckpointCommitter implements Serializable {

	protected static final Logger LOG = LoggerFactory.getLogger(CheckpointCommitter.class);

	protected String jobId;
	protected String operatorId;

	/**
	 * Internally used to set the job ID after instantiation.
	 *
	 * @param id
	 * @throws Exception
	 */
	public void setJobId(String id) throws Exception {
		this.jobId = id;
	}

	/**
	 * Internally used to set the operator ID after instantiation.
	 *
	 * @param id
	 * @throws Exception
	 */
	public void setOperatorId(String id) throws Exception {
		this.operatorId = id;
	}

	/**
	 * Opens/connects to the resource, and possibly creates it beforehand.
	 *
	 * @throws Exception
	 */
	public abstract void open() throws Exception;

	/**
	 * Closes the resource/connection to it. The resource should generally still exist after this call.
	 *
	 * @throws Exception
	 */
	public abstract void close() throws Exception;

	/**
	 * Creates/opens/connects to the resource that is used to store information. Called once directly after instantiation.
	 * @throws Exception
	 */
	public abstract void createResource() throws Exception;

	/**
	 * Mark the given checkpoint as completed in the resource.
	 *
	 * @param subtaskIdx the index of the subtask responsible for committing the checkpoint.
	 * @param checkpointID the id of the checkpoint to be committed.
	 * @throws Exception
	 */
	public abstract void commitCheckpoint(int subtaskIdx, long checkpointID) throws Exception;

	/**
	 * Checked the resource whether the given checkpoint was committed completely.
	 *
	 * @param subtaskIdx the index of the subtask responsible for committing the checkpoint.
	 * @param checkpointID the id of the checkpoint we are interested in.
	 * @return true if the checkpoint was committed completely, false otherwise
	 * @throws Exception
	 */
	public abstract boolean isCheckpointCommitted(int subtaskIdx, long checkpointID) throws Exception;
}
