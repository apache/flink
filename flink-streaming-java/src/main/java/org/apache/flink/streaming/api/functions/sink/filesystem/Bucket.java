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

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.api.common.serialization.Writer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.ResumableWriter;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A bucket is the directory organization of the output of the {@link BucketingSink}.
 *
 *
 * <p>For each incoming  element in the {@code BucketingSink}, the user-specified
 * {@link org.apache.flink.streaming.api.functions.sink.filesystem.bucketers.Bucketer Bucketer} is
 * queried to see in which bucket this element should be written to.
 */
public class Bucket<IN> {

	private static final String PART_PREFIX = "part";

	private final Path bucketPath;

	private int subtaskIndex;

	private long partCounter;

	private long creationTime;

	private long lastWrittenTime;

	private final long maxPathSize;

	private final long rolloverTime;

	private final long inactivityTime;

	private final Writer<IN> outputFormatWriter;

	private final ResumableWriter fsWriter;

	private RecoverableFsDataOutputStream currentOpenPartStream;

	private List<ResumableWriter.CommitRecoverable> pending = new ArrayList<>();

	private Map<Long, List<ResumableWriter.CommitRecoverable>> pendingPerCheckpoint = new HashMap<>();

	public Bucket(
			ResumableWriter fsWriter,
			int subtaskIndex,
			Path bucketPath,
			long initialPartCounter,
			long maxPartSize,
			long rolloverTime,
			long inactivityTime,
			Writer<IN> writer,
			BucketState bucketstate) throws IOException {

		this(fsWriter, subtaskIndex, bucketPath, initialPartCounter, maxPartSize, rolloverTime, inactivityTime, writer);

		// the constructor must have already initialized the filesystem writer
		Preconditions.checkState(fsWriter != null);

		// we try to resume the previous in-progress file, if the filesystem
		// supports such operation. If not, we just commit the file and start fresh.

		final ResumableWriter.ResumeRecoverable resumable = bucketstate.getCurrentInProgress();
		if (resumable != null) {
			this.currentOpenPartStream = fsWriter.recover(resumable);
			this.creationTime = bucketstate.getCreationTime();
		}

		// we commit pending files for previous checkpoints to the last successful one
		// (from which we are recovering from)
		for (List<ResumableWriter.CommitRecoverable> commitables: bucketstate.getPendingPerCheckpoint().values()) {
			for (ResumableWriter.CommitRecoverable commitable: commitables) {
				fsWriter.recoverForCommit(commitable).commitAfterRecovery();
			}
		}

		this.pending = new ArrayList<>();
		this.pendingPerCheckpoint = new HashMap<>();
	}

	public Bucket(
			ResumableWriter fsWriter,
			int subtaskIndex,
			Path bucketPath,
			long initialPartCounter,
			long maxPartSize,
			long rolloverTime,
			long inactivityTime,
			Writer<IN> writer) {

		this.fsWriter = Preconditions.checkNotNull(fsWriter);
		this.subtaskIndex = subtaskIndex;
		this.bucketPath = Preconditions.checkNotNull(bucketPath);
		this.partCounter = initialPartCounter;
		this.maxPathSize = maxPartSize;
		this.rolloverTime = rolloverTime;
		this.inactivityTime = inactivityTime;
		this.outputFormatWriter = Preconditions.checkNotNull(writer);
		this.creationTime = Long.MAX_VALUE;
	}

	public Path getBucketPath() {
		return bucketPath;
	}

	public long getPartCounter() {
		return partCounter;
	}

	public boolean isActive() {
		return isOpen() || !pending.isEmpty() || !pendingPerCheckpoint.isEmpty();
	}

	public boolean isOpen() {
		return currentOpenPartStream != null;
	}

	public void write(IN element, Long timestamp, long currentTime) throws IOException {
		if (shouldRoll(currentTime)) {
			startNewChunk(currentTime);
		}
		Preconditions.checkState(isOpen(), "BucketingSink is not open.");

		outputFormatWriter.write(element, timestamp, currentOpenPartStream);
		lastWrittenTime = currentTime;
	}

	private void startNewChunk(final long currentTime) throws IOException {
		closeCurrentChunk();

		this.currentOpenPartStream = fsWriter.open(getNewPartPath());
		this.creationTime = currentTime;
		this.partCounter++;
	}

	private boolean shouldRoll(long currentTime) throws IOException {
		if (currentOpenPartStream == null) {
			return true;
		}

		long writePosition = currentOpenPartStream.getPos();
		if (writePosition > maxPathSize) {
			return true;
		}

		return (currentTime - creationTime > rolloverTime);
	}

	void merge(final Bucket bucket) throws IOException {
		Preconditions.checkNotNull(bucket);
		Preconditions.checkState(bucket.getBucketPath().equals(getBucketPath()));

		// there should be no pending files in the "to-merge" states.
		Preconditions.checkState(bucket.pending.isEmpty());
		Preconditions.checkState(bucket.pendingPerCheckpoint.isEmpty());

		ResumableWriter.CommitRecoverable commitable = bucket.closeCurrentChunk();
		if (commitable != null) {
			pending.add(commitable);
		}
	}

	public ResumableWriter.CommitRecoverable closeCurrentChunk() throws IOException {
		ResumableWriter.CommitRecoverable commitable = null;
		if (currentOpenPartStream != null) {
			commitable = currentOpenPartStream.closeForCommit().getRecoverable();
			pending.add(commitable);
			currentOpenPartStream = null;
		}
		return commitable;
	}

	public void rollByTime(long currentTime) throws IOException {
		if (currentTime - creationTime > rolloverTime ||
				currentTime - lastWrittenTime > inactivityTime) {
			closeCurrentChunk();
		}
	}

	public void commitUpToCheckpoint(long checkpointId) throws IOException {
		Preconditions.checkNotNull(fsWriter);

		Iterator<Map.Entry<Long, List<ResumableWriter.CommitRecoverable>>> it =
				pendingPerCheckpoint.entrySet().iterator();

		while (it.hasNext()) {
			Map.Entry<Long, List<ResumableWriter.CommitRecoverable>> entry = it.next();
			if (entry.getKey() <= checkpointId) {
				for (ResumableWriter.CommitRecoverable commitable : entry.getValue()) {
					fsWriter.recoverForCommit(commitable).commit();
				}
				it.remove();
			}
		}
	}

	public BucketState snapshot(long checkpointId, long checkpointTimestamp) throws IOException {

		// we also check here so that we do not have to always
		// wait for the "next" element to arrive.

		if (shouldRoll(checkpointTimestamp)) {
			closeCurrentChunk();
		}

		ResumableWriter.ResumeRecoverable resumable = null;
		if (currentOpenPartStream != null) {
			resumable = currentOpenPartStream.persist();
		}

		if (!pending.isEmpty()) {
			pendingPerCheckpoint.put(checkpointId, pending);
			pending = new ArrayList<>();
		}

		return new BucketState(bucketPath, creationTime, resumable, pendingPerCheckpoint);
	}

	public static BucketStateSerializer getBucketStateSerializer(ResumableWriter fsWriter) {
		Preconditions.checkNotNull(fsWriter);

		return new BucketStateSerializer(
				fsWriter.getResumeRecoverableSerializer(),
				fsWriter.getCommitRecoverableSerializer()
		);
	}

	private Path getNewPartPath() {
		return new Path(bucketPath, PART_PREFIX + "-" + subtaskIndex + "-" + partCounter);
	}

	/**
	 * The state of the {@link Bucket} that is to be checkpointed.
	 */
	public static class BucketState {

		private final Path bucketPath;

		private final long creationTime;

		private final ResumableWriter.ResumeRecoverable inProgress;

		private final Map<Long, List<ResumableWriter.CommitRecoverable>> pendingPerCheckpoint;

		public BucketState(
				final Path bucketPath,
				final long creationTime,
				final ResumableWriter.ResumeRecoverable inProgress,
				final Map<Long, List<ResumableWriter.CommitRecoverable>> pendingPerCheckpoint
		) {
			this.bucketPath = Preconditions.checkNotNull(bucketPath);
			this.creationTime = creationTime;
			this.inProgress = inProgress;
			this.pendingPerCheckpoint = Preconditions.checkNotNull(pendingPerCheckpoint);
		}

		public Path getBucketPath() {
			return bucketPath;
		}

		public long getCreationTime() {
			return creationTime;
		}

		public ResumableWriter.ResumeRecoverable getCurrentInProgress() {
			return inProgress;
		}

		public Map<Long, List<ResumableWriter.CommitRecoverable>> getPendingPerCheckpoint() {
			return pendingPerCheckpoint;
		}
	}
}
