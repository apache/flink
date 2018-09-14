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

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A bucket is the directory organization of the output of the {@link StreamingFileSink}.
 *
 * <p>For each incoming element in the {@code StreamingFileSink}, the user-specified
 * {@link BucketAssigner} is queried to see in which bucket this element should be written to.
 */
@Internal
public class Bucket<IN, BucketID> {

	private static final Logger LOG = LoggerFactory.getLogger(Bucket.class);

	private static final String PART_PREFIX = "part";

	private final BucketID bucketId;

	private final Path bucketPath;

	private final int subtaskIndex;

	private final PartFileWriter.PartFileFactory<IN, BucketID> partFileFactory;

	private final RecoverableWriter fsWriter;

	private final RollingPolicy<IN, BucketID> rollingPolicy;

	private final Map<Long, List<RecoverableWriter.CommitRecoverable>> pendingPartsPerCheckpoint = new HashMap<>();

	private long partCounter;

	private PartFileWriter<IN, BucketID> inProgressPart;

	private List<RecoverableWriter.CommitRecoverable> pendingPartsForCurrentCheckpoint;

	/**
	 * Constructor to create a new empty bucket.
	 */
	private Bucket(
			final RecoverableWriter fsWriter,
			final int subtaskIndex,
			final BucketID bucketId,
			final Path bucketPath,
			final long initialPartCounter,
			final PartFileWriter.PartFileFactory<IN, BucketID> partFileFactory,
			final RollingPolicy<IN, BucketID> rollingPolicy) {

		this.fsWriter = Preconditions.checkNotNull(fsWriter);
		this.subtaskIndex = subtaskIndex;
		this.bucketId = Preconditions.checkNotNull(bucketId);
		this.bucketPath = Preconditions.checkNotNull(bucketPath);
		this.partCounter = initialPartCounter;
		this.partFileFactory = Preconditions.checkNotNull(partFileFactory);
		this.rollingPolicy = Preconditions.checkNotNull(rollingPolicy);

		this.pendingPartsForCurrentCheckpoint = new ArrayList<>();
	}

	/**
	 * Constructor to restore a bucket from checkpointed state.
	 */
	private Bucket(
			final RecoverableWriter fsWriter,
			final int subtaskIndex,
			final long initialPartCounter,
			final PartFileWriter.PartFileFactory<IN, BucketID> partFileFactory,
			final RollingPolicy<IN, BucketID> rollingPolicy,
			final BucketState<BucketID> bucketState) throws IOException {

		this(
				fsWriter,
				subtaskIndex,
				bucketState.getBucketId(),
				bucketState.getBucketPath(),
				initialPartCounter,
				partFileFactory,
				rollingPolicy);

		restoreInProgressFile(bucketState);
		commitRecoveredPendingFiles(bucketState);
	}

	private void restoreInProgressFile(final BucketState<BucketID> state) throws IOException {

		// we try to resume the previous in-progress file
		if (state.hasInProgressResumableFile()) {
			final RecoverableWriter.ResumeRecoverable resumable = state.getInProgressResumableFile();
			inProgressPart = partFileFactory.resumeFrom(
					bucketId, fsWriter, resumable, state.getInProgressFileCreationTime());
		}
	}

	private void commitRecoveredPendingFiles(final BucketState<BucketID> state) throws IOException {

		// we commit pending files for checkpoints that precess the last successful one, from which we are recovering
		for (List<RecoverableWriter.CommitRecoverable> committables: state.getCommittableFilesPerCheckpoint().values()) {
			for (RecoverableWriter.CommitRecoverable committable: committables) {
				fsWriter.recoverForCommit(committable).commitAfterRecovery();
			}
		}
	}

	BucketID getBucketId() {
		return bucketId;
	}

	Path getBucketPath() {
		return bucketPath;
	}

	long getPartCounter() {
		return partCounter;
	}

	boolean isActive() {
		return inProgressPart != null || !pendingPartsForCurrentCheckpoint.isEmpty() || !pendingPartsPerCheckpoint.isEmpty();
	}

	void merge(final Bucket<IN, BucketID> bucket) throws IOException {
		Preconditions.checkNotNull(bucket);
		Preconditions.checkState(Objects.equals(bucket.bucketPath, bucketPath));

		// There should be no pending files in the "to-merge" states.
		// The reason is that:
		// 1) the pendingPartsForCurrentCheckpoint is emptied whenever we take a snapshot (see prepareBucketForCheckpointing()).
		//    So a snapshot, including the one we are recovering from, will never contain such files.
		// 2) the files in pendingPartsPerCheckpoint are committed upon recovery (see commitRecoveredPendingFiles()).

		Preconditions.checkState(bucket.pendingPartsForCurrentCheckpoint.isEmpty());
		Preconditions.checkState(bucket.pendingPartsPerCheckpoint.isEmpty());

		RecoverableWriter.CommitRecoverable committable = bucket.closePartFile();
		if (committable != null) {
			pendingPartsForCurrentCheckpoint.add(committable);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Subtask {} merging buckets for bucket id={}", subtaskIndex, bucketId);
		}
	}

	void write(IN element, long currentTime) throws IOException {
		if (inProgressPart == null || rollingPolicy.shouldRollOnEvent(inProgressPart, element)) {

			if (LOG.isDebugEnabled()) {
				LOG.debug("Subtask {} closing in-progress part file for bucket id={} due to element {}.",
						subtaskIndex, bucketId, element);
			}

			rollPartFile(currentTime);
		}
		inProgressPart.write(element, currentTime);
	}

	private void rollPartFile(final long currentTime) throws IOException {
		closePartFile();

		final Path partFilePath = assembleNewPartPath();
		inProgressPart = partFileFactory.openNew(bucketId, fsWriter, partFilePath, currentTime);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Subtask {} opening new part file \"{}\" for bucket id={}.",
					subtaskIndex, partFilePath.getName(), bucketId);
		}

		partCounter++;
	}

	private Path assembleNewPartPath() {
		return new Path(bucketPath, PART_PREFIX + '-' + subtaskIndex + '-' + partCounter);
	}

	private RecoverableWriter.CommitRecoverable closePartFile() throws IOException {
		RecoverableWriter.CommitRecoverable committable = null;
		if (inProgressPart != null) {
			committable = inProgressPart.closeForCommit();
			pendingPartsForCurrentCheckpoint.add(committable);
			inProgressPart = null;
		}
		return committable;
	}

	void disposePartFile() {
		if (inProgressPart != null) {
			inProgressPart.dispose();
		}
	}

	BucketState<BucketID> onReceptionOfCheckpoint(long checkpointId) throws IOException {
		prepareBucketForCheckpointing(checkpointId);

		RecoverableWriter.ResumeRecoverable inProgressResumable = null;
		long inProgressFileCreationTime = Long.MAX_VALUE;

		if (inProgressPart != null) {
			inProgressResumable = inProgressPart.persist();
			inProgressFileCreationTime = inProgressPart.getCreationTime();
		}

		return new BucketState<>(bucketId, bucketPath, inProgressFileCreationTime, inProgressResumable, pendingPartsPerCheckpoint);
	}

	private void prepareBucketForCheckpointing(long checkpointId) throws IOException {
		if (inProgressPart != null && rollingPolicy.shouldRollOnCheckpoint(inProgressPart)) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Subtask {} closing in-progress part file for bucket id={} on checkpoint.", subtaskIndex, bucketId);
			}
			closePartFile();
		}

		if (!pendingPartsForCurrentCheckpoint.isEmpty()) {
			pendingPartsPerCheckpoint.put(checkpointId, pendingPartsForCurrentCheckpoint);
			pendingPartsForCurrentCheckpoint = new ArrayList<>();
		}
	}

	void onSuccessfulCompletionOfCheckpoint(long checkpointId) throws IOException {
		Preconditions.checkNotNull(fsWriter);

		Iterator<Map.Entry<Long, List<RecoverableWriter.CommitRecoverable>>> it =
				pendingPartsPerCheckpoint.entrySet().iterator();

		while (it.hasNext()) {
			Map.Entry<Long, List<RecoverableWriter.CommitRecoverable>> entry = it.next();

			if (entry.getKey() <= checkpointId) {
				for (RecoverableWriter.CommitRecoverable committable : entry.getValue()) {
					fsWriter.recoverForCommit(committable).commit();
				}
				it.remove();
			}
		}
	}

	void onProcessingTime(long timestamp) throws IOException {
		if (inProgressPart != null && rollingPolicy.shouldRollOnProcessingTime(inProgressPart, timestamp)) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Subtask {} closing in-progress part file for bucket id={} due to processing time rolling policy " +
						"(in-progress file created @ {}, last updated @ {} and current time is {}).",
						subtaskIndex, bucketId, inProgressPart.getCreationTime(), inProgressPart.getLastUpdateTime(), timestamp);
			}
			closePartFile();
		}
	}

	// --------------------------- Static Factory Methods -----------------------------

	/**
	 * Creates a new empty {@code Bucket}.
	 * @param fsWriter the filesystem-specific {@link RecoverableWriter}.
	 * @param subtaskIndex the index of the subtask creating the bucket.
	 * @param bucketId the identifier of the bucket, as returned by the {@link BucketAssigner}.
	 * @param bucketPath the path to where the part files for the bucket will be written to.
	 * @param initialPartCounter the initial counter for the part files of the bucket.
	 * @param partFileFactory the {@link PartFileWriter.PartFileFactory} the factory creating part file writers.
	 * @param <IN> the type of input elements to the sink.
	 * @param <BucketID> the type of the identifier of the bucket, as returned by the {@link BucketAssigner}
	 * @return The new Bucket.
	 */
	static <IN, BucketID> Bucket<IN, BucketID> getNew(
			final RecoverableWriter fsWriter,
			final int subtaskIndex,
			final BucketID bucketId,
			final Path bucketPath,
			final long initialPartCounter,
			final PartFileWriter.PartFileFactory<IN, BucketID> partFileFactory,
			final RollingPolicy<IN, BucketID> rollingPolicy) {
		return new Bucket<>(fsWriter, subtaskIndex, bucketId, bucketPath, initialPartCounter, partFileFactory, rollingPolicy);
	}

	/**
	 * Restores a {@code Bucket} from the state included in the provided {@link BucketState}.
	 * @param fsWriter the filesystem-specific {@link RecoverableWriter}.
	 * @param subtaskIndex the index of the subtask creating the bucket.
	 * @param initialPartCounter the initial counter for the part files of the bucket.
	 * @param partFileFactory the {@link PartFileWriter.PartFileFactory} the factory creating part file writers.
	 * @param bucketState the initial state of the restored bucket.
	 * @param <IN> the type of input elements to the sink.
	 * @param <BucketID> the type of the identifier of the bucket, as returned by the {@link BucketAssigner}
	 * @return The restored Bucket.
	 */
	static <IN, BucketID> Bucket<IN, BucketID> restore(
			final RecoverableWriter fsWriter,
			final int subtaskIndex,
			final long initialPartCounter,
			final PartFileWriter.PartFileFactory<IN, BucketID> partFileFactory,
			final RollingPolicy<IN, BucketID> rollingPolicy,
			final BucketState<BucketID> bucketState) throws IOException {
		return new Bucket<>(fsWriter, subtaskIndex, initialPartCounter, partFileFactory, rollingPolicy, bucketState);
	}
}
