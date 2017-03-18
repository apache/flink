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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStats.DiscardCallback;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A CompletedCheckpoint describes a checkpoint after all required tasks acknowledged it (with their state)
 * and that is considered successful. The CompletedCheckpoint class contains all the metadata of the
 * checkpoint, i.e., checkpoint ID, timestamps, and the handles to all states that are part of the
 * checkpoint.
 * 
 * <h2>Size the CompletedCheckpoint Instances</h2>
 * 
 * In most cases, the CompletedCheckpoint objects are very small, because the handles to the checkpoint
 * states are only pointers (such as file paths). However, the some state backend implementations may
 * choose to store some payload data directly with the metadata (for example to avoid many small files).
 * If those thresholds are increased to large values, the memory consumption of the CompletedCheckpoint
 * objects can be significant.
 * 
 * <h2>Externalized Metadata</h2>
 * 
 * The metadata of the CompletedCheckpoint is optionally also persisted in an external storage
 * system. In that case, the checkpoint is called <i>externalized</i>.
 * 
 * <p>Externalized checkpoints have an external pointer, which points to the metadata. For example
 * when externalizing to a file system, that pointer is the file path to the checkpoint's folder
 * or the metadata file. For a state backend that stores metadata in database tables, the pointer
 * could be the table name and row key. The pointer is encoded as a String.
 * 
 * <h2>Externalized Metadata and High-availability</h2>
 * 
 * For high availability setups, the checkpoint metadata must be stored persistent and available
 * as well. The high-availability services that stores the checkpoint ground-truth (meaning what are
 * the latest completed checkpoints in what order) often rely on checkpoints being externalized. That
 * way, those services only store pointers to the externalized metadata, rather than the complete
 * metadata itself (for example ZooKeeper's ZNode payload should ideally be less than megabytes).
 */
public class CompletedCheckpoint implements Serializable {

	private static final Logger LOG = LoggerFactory.getLogger(CompletedCheckpoint.class);

	private static final long serialVersionUID = -8360248179615702014L;

	// ------------------------------------------------------------------------

	/** The ID of the job that the checkpoint belongs to */
	private final JobID job;

	/** The ID (logical timestamp) of the checkpoint */
	private final long checkpointID;

	/** The timestamp when the checkpoint was triggered. */
	private final long timestamp;

	/** The duration of the checkpoint (completion timestamp - trigger timestamp). */
	private final long duration;

	/** States of the different task groups belonging to this checkpoint */
	private final Map<JobVertexID, TaskState> taskStates;

	/** Properties for this checkpoint. */
	private final CheckpointProperties props;

	/** The state handle to the externalized meta data, if the metadata has been externalized */
	@Nullable
	private final StreamStateHandle externalizedMetadata;

	/** External pointer to the completed checkpoint (for example file path) if externalized; null otherwise. */
	@Nullable
	private final String externalPointer;

	/** Optional stats tracker callback for discard. */
	@Nullable
	private transient volatile DiscardCallback discardCallback;

	// ------------------------------------------------------------------------

	@VisibleForTesting
	CompletedCheckpoint(
			JobID job,
			long checkpointID,
			long timestamp,
			long completionTimestamp,
			Map<JobVertexID, TaskState> taskStates) {

		this(job, checkpointID, timestamp, completionTimestamp, taskStates,
				CheckpointProperties.forStandardCheckpoint());
	}

	public CompletedCheckpoint(
			JobID job,
			long checkpointID,
			long timestamp,
			long completionTimestamp,
			Map<JobVertexID, TaskState> taskStates,
			CheckpointProperties props) {

		this(job, checkpointID, timestamp, completionTimestamp, taskStates, props, null, null);
	}

	public CompletedCheckpoint(
			JobID job,
			long checkpointID,
			long timestamp,
			long completionTimestamp,
			Map<JobVertexID, TaskState> taskStates,
			CheckpointProperties props,
			@Nullable StreamStateHandle externalizedMetadata,
			@Nullable String externalPointer) {

		checkArgument(checkpointID >= 0);
		checkArgument(timestamp >= 0);
		checkArgument(completionTimestamp >= 0);

		checkArgument((externalPointer == null) == (externalizedMetadata == null),
				"external pointer without externalized metadata must be both null or both non-null");

		checkArgument(!props.externalizeCheckpoint() || externalPointer != null, 
			"Checkpoint properties require externalized checkpoint, but checkpoint is not externalized");

		this.job = checkNotNull(job);
		this.checkpointID = checkpointID;
		this.timestamp = timestamp;
		this.duration = completionTimestamp - timestamp;
		this.taskStates = checkNotNull(taskStates);
		this.props = checkNotNull(props);
		this.externalizedMetadata = externalizedMetadata;
		this.externalPointer = externalPointer;
	}

	// ------------------------------------------------------------------------

	public JobID getJobId() {
		return job;
	}

	public long getCheckpointID() {
		return checkpointID;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public long getDuration() {
		return duration;
	}

	public CheckpointProperties getProperties() {
		return props;
	}

	public boolean subsume() throws Exception {
		if (props.discardOnSubsumed()) {
			discard();
			return true;
		}

		return false;
	}

	public boolean discard(JobStatus jobStatus) throws Exception {
		if (jobStatus == JobStatus.FINISHED && props.discardOnJobFinished() ||
				jobStatus == JobStatus.CANCELED && props.discardOnJobCancelled() ||
				jobStatus == JobStatus.FAILED && props.discardOnJobFailed() ||
				jobStatus == JobStatus.SUSPENDED && props.discardOnJobSuspended()) {

			discard();
			return true;
		} else {
			if (externalPointer != null) {
				LOG.info("Persistent checkpoint with ID {} at '{}' not discarded.",
						checkpointID, externalPointer);
			}

			return false;
		}
	}

	void discard() throws Exception {
		try {
			// collect exceptions and continue cleanup
			Exception exception = null;

			// drop the metadata, if we have some
			if (externalizedMetadata != null) {
				try {
					externalizedMetadata.discardState();
				}
				catch (Exception e) {
					exception = e;
				}
			}

			// drop the actual state
			try {
				StateUtil.bestEffortDiscardAllStateObjects(taskStates.values());
			}
			catch (Exception e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}

			if (exception != null) {
				throw exception;
			}
		}
		finally {
			taskStates.clear();

			// to be null-pointer safe, copy reference to stack
			DiscardCallback discardCallback = this.discardCallback;
			if (discardCallback != null) {
				discardCallback.notifyDiscardedCheckpoint();
			}
		}
	}

	public long getStateSize() {
		long result = 0L;

		for (TaskState taskState : taskStates.values()) {
			result += taskState.getStateSize();
		}

		return result;
	}

	public Map<JobVertexID, TaskState> getTaskStates() {
		return taskStates;
	}

	public TaskState getTaskState(JobVertexID jobVertexID) {
		return taskStates.get(jobVertexID);
	}

	public boolean isExternalized() {
		return externalizedMetadata != null;
	}

	@Nullable
	public StreamStateHandle getExternalizedMetadata() {
		return externalizedMetadata;
	}

	@Nullable
	public String getExternalPointer() {
		return externalPointer;
	}

	/**
	 * Sets the callback for tracking when this checkpoint is discarded.
	 *
	 * @param discardCallback Callback to call when the checkpoint is discarded.
	 */
	void setDiscardCallback(@Nullable CompletedCheckpointStats.DiscardCallback discardCallback) {
		this.discardCallback = discardCallback;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public String toString() {
		return String.format("Checkpoint %d @ %d for %s", checkpointID, timestamp, job);
	}
}
