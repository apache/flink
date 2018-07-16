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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A bucket is the directory organization of the output of the {@link StreamingFileSink}.
 *
 * <p>For each incoming  element in the {@code BucketingSink}, the user-specified
 * {@link org.apache.flink.streaming.api.functions.sink.filesystem.bucketers.Bucketer Bucketer} is
 * queried to see in which bucket this element should be written to.
 */
@PublicEvolving
public class Bucket<IN> {

	private static final String PART_PREFIX = "part";

	private final String bucketId;

	private final Path bucketPath;

	private final int subtaskIndex;

	private final Encoder<IN> encoder;

	private final RecoverableWriter fsWriter;

	private final Map<Long, List<RecoverableWriter.CommitRecoverable>> pendingPerCheckpoint = new HashMap<>();

	private long partCounter;

	private PartFileHandler<IN> currentPart;

	private List<RecoverableWriter.CommitRecoverable> pending;

	/**
	 * Constructor to restore a bucket from checkpointed state.
	 */
	public Bucket(
			RecoverableWriter fsWriter,
			int subtaskIndex,
			long initialPartCounter,
			Encoder<IN> writer,
			BucketState bucketstate) throws IOException {

		this(fsWriter, subtaskIndex, bucketstate.getBucketId(), bucketstate.getBucketPath(), initialPartCounter, writer);

		// the constructor must have already initialized the filesystem writer
		Preconditions.checkState(fsWriter != null);

		// we try to resume the previous in-progress file, if the filesystem
		// supports such operation. If not, we just commit the file and start fresh.

		final RecoverableWriter.ResumeRecoverable resumable = bucketstate.getCurrentInProgress();
		if (resumable != null) {
			currentPart = PartFileHandler.resumeFrom(
					bucketId, fsWriter, resumable, bucketstate.getCreationTime());
		}

		// we commit pending files for previous checkpoints to the last successful one
		// (from which we are recovering from)
		for (List<RecoverableWriter.CommitRecoverable> commitables: bucketstate.getPendingPerCheckpoint().values()) {
			for (RecoverableWriter.CommitRecoverable commitable: commitables) {
				fsWriter.recoverForCommit(commitable).commitAfterRecovery();
			}
		}
	}

	/**
	 * Constructor to create a new empty bucket.
	 */
	public Bucket(
			RecoverableWriter fsWriter,
			int subtaskIndex,
			String bucketId,
			Path bucketPath,
			long initialPartCounter,
			Encoder<IN> writer) {

		this.fsWriter = Preconditions.checkNotNull(fsWriter);
		this.subtaskIndex = subtaskIndex;
		this.bucketId = Preconditions.checkNotNull(bucketId);
		this.bucketPath = Preconditions.checkNotNull(bucketPath);
		this.partCounter = initialPartCounter;
		this.encoder = Preconditions.checkNotNull(writer);

		this.pending = new ArrayList<>();
	}

	public PartFileInfo getInProgressPartInfo() {
		return currentPart;
	}

	public String getBucketId() {
		return bucketId;
	}

	public Path getBucketPath() {
		return bucketPath;
	}

	public long getPartCounter() {
		return partCounter;
	}

	public boolean isActive() {
		return currentPart != null || !pending.isEmpty() || !pendingPerCheckpoint.isEmpty();
	}

	void write(IN element, long currentTime) throws IOException {
		Preconditions.checkState(currentPart != null, "bucket has been closed");
		currentPart.write(element, encoder, currentTime);
	}

	void rollPartFile(final long currentTime) throws IOException {
		closePartFile();
		currentPart = PartFileHandler.openNew(bucketId, fsWriter, getNewPartPath(), currentTime);
		partCounter++;
	}

	void merge(final Bucket<IN> bucket) throws IOException {
		Preconditions.checkNotNull(bucket);
		Preconditions.checkState(bucket.getBucketPath().equals(getBucketPath()));

		// there should be no pending files in the "to-merge" states.
		Preconditions.checkState(bucket.pending.isEmpty());
		Preconditions.checkState(bucket.pendingPerCheckpoint.isEmpty());

		RecoverableWriter.CommitRecoverable commitable = bucket.closePartFile();
		if (commitable != null) {
			pending.add(commitable);
		}
	}

	RecoverableWriter.CommitRecoverable closePartFile() throws IOException {
		RecoverableWriter.CommitRecoverable commitable = null;
		if (currentPart != null) {
			commitable = currentPart.closeForCommit();
			pending.add(commitable);
			currentPart = null;
		}
		return commitable;
	}

	public void dispose() {
		if (currentPart != null) {
			currentPart.dispose();
		}
	}

	public void commitUpToCheckpoint(long checkpointId) throws IOException {
		Preconditions.checkNotNull(fsWriter);

		Iterator<Map.Entry<Long, List<RecoverableWriter.CommitRecoverable>>> it =
				pendingPerCheckpoint.entrySet().iterator();

		while (it.hasNext()) {
			Map.Entry<Long, List<RecoverableWriter.CommitRecoverable>> entry = it.next();
			if (entry.getKey() <= checkpointId) {
				for (RecoverableWriter.CommitRecoverable commitable : entry.getValue()) {
					fsWriter.recoverForCommit(commitable).commit();
				}
				it.remove();
			}
		}
	}

	public BucketState snapshot(long checkpointId) throws IOException {
		RecoverableWriter.ResumeRecoverable resumable = null;
		long creationTime = Long.MAX_VALUE;

		if (currentPart != null) {
			resumable = currentPart.persist();
			creationTime = currentPart.getCreationTime();
		}

		if (!pending.isEmpty()) {
			pendingPerCheckpoint.put(checkpointId, pending);
			pending = new ArrayList<>();
		}
		return new BucketState(bucketId, bucketPath, creationTime, resumable, pendingPerCheckpoint);
	}

	private Path getNewPartPath() {
		return new Path(bucketPath, PART_PREFIX + '-' + subtaskIndex + '-' + partCounter);
	}
}
