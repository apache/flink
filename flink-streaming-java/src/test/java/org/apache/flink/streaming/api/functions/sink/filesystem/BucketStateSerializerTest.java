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

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.util.Preconditions;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests for the {@link BucketStateSerializer}.
 */
public class BucketStateSerializerTest {

	private static final String IN_PROGRESS_CONTENT = "writing";
	private static final String PENDING_CONTENT = "wrote";

	@ClassRule
	public static TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void testSerializationEmpty() throws IOException {
		final File testFolder = tempFolder.newFolder();
		final FileSystem fs = FileSystem.get(testFolder.toURI());
		final BucketWriter<String, String> partFileFactory =
			new RowWisePartWriter.Factory(fs.createRecoverableWriter(), new SimpleStringEncoder());

		final Path testBucket = new Path(testFolder.getPath(), "test");

		final BucketState<String> bucketState = new BucketState<>(
			"test", testBucket, Long.MAX_VALUE, null, new HashMap<>());
		final SimpleVersionedSerializer<BucketState<String>> serializer = getBucketStateSerializer(partFileFactory);

		final byte[] bytes = SimpleVersionedSerialization.writeVersionAndSerialize(serializer, bucketState);
		final BucketState<String> recoveredState =  SimpleVersionedSerialization.readVersionAndDeSerialize(serializer, bytes);

		validateEmptyState(testBucket, recoveredState);
	}

	@Test
	public void testSerializationOnlyInProgress() throws IOException {
		final File testFolder = tempFolder.newFolder();
		final FileSystem fs = FileSystem.get(testFolder.toURI());

		final Path testBucket = new Path(testFolder.getPath(), "test");

		final BucketWriter<String, String> partFileFactory =
			new RowWisePartWriter.Factory(fs.createRecoverableWriter(), new SimpleStringEncoder());
		final InProgressFileWriter<String, String> partFileWriter = partFileFactory.openNewInProgressFile("test", testBucket, System.currentTimeMillis());
		partFileWriter.write(IN_PROGRESS_CONTENT, System.currentTimeMillis());

		final InProgressFileWriter.InProgressFileRecoverable current = partFileWriter.persist();

		final BucketState<String> bucketState = new BucketState<>(
				"test", testBucket, Long.MAX_VALUE, current, new HashMap<>());

		final SimpleVersionedSerializer<BucketState<String>> serializer = getBucketStateSerializer(partFileFactory);

		final byte[] bytes = SimpleVersionedSerialization.writeVersionAndSerialize(serializer, bucketState);

		// to simulate that everything is over for file.
		partFileWriter.closeForCommit();

		final BucketState<String> recoveredState =  SimpleVersionedSerialization.readVersionAndDeSerialize(serializer, bytes);

		validateOnlyHasInProgressFileState(testBucket, recoveredState);
	}

	@Test
	public void testSerializationFull() throws IOException {
		final int noOfTasks = 5;

		final File testFolder = tempFolder.newFolder();
		final FileSystem fs = FileSystem.get(testFolder.toURI());
		final BucketWriter<String, String> partFileFactory =
			new RowWisePartWriter.Factory(fs.createRecoverableWriter(), new SimpleStringEncoder());

		final Path bucketPath = new Path(testFolder.getPath());

		// pending for checkpoints
		final Map<Long, List<InProgressFileWriter.PendingFileRecoverable>> pendingFileRecoverableList = new HashMap<>();
		for (int i = 0; i < noOfTasks; i++) {
			final List<InProgressFileWriter.PendingFileRecoverable> pendingFileRecoverables = new ArrayList<>();
			for (int j = 0; j < 2 + i; j++) {
				final Path part = new Path(bucketPath, "part-" + i + '-' + j);

				final InProgressFileWriter<String, String> partFileWriter =
					partFileFactory.openNewInProgressFile("", part, System.currentTimeMillis());
				partFileWriter.write(PENDING_CONTENT, System.currentTimeMillis());
				pendingFileRecoverables.add(partFileWriter.closeForCommit());
			}
			pendingFileRecoverableList.put((long) i, pendingFileRecoverables);
		}

		// in-progress
		final Path testBucket = new Path(bucketPath, "test-2");
		final InProgressFileWriter<String, String> partFileWriter = partFileFactory.openNewInProgressFile("test-2", testBucket, System.currentTimeMillis());
		partFileWriter.write(IN_PROGRESS_CONTENT, System.currentTimeMillis());

		final InProgressFileWriter.InProgressFileRecoverable current = partFileWriter.persist();

		final BucketState<String> bucketState = new BucketState<>(
				"test-2",
			bucketPath,
			Long.MAX_VALUE,
			current,
			pendingFileRecoverableList);
		final SimpleVersionedSerializer<BucketState<String>> serializer = getBucketStateSerializer(partFileFactory);

		partFileWriter.closeForCommit();

		byte[] bytes = SimpleVersionedSerialization.writeVersionAndSerialize(serializer, bucketState);

		final BucketState<String> recoveredState =  SimpleVersionedSerialization.readVersionAndDeSerialize(serializer, bytes);

		validateFullState(noOfTasks, bucketPath, testBucket, recoveredState, partFileFactory);
	}

	@Test
	public void testSerializationNullInProgress() throws IOException {
		final int noOfTasks = 5;

		final File testFolder = tempFolder.newFolder();
		final FileSystem fs = FileSystem.get(testFolder.toURI());
		final BucketWriter<String, String> partFileFactory =
			new RowWisePartWriter.Factory(fs.createRecoverableWriter(), new SimpleStringEncoder());

		final Path bucketPath = new Path(testFolder.getPath());

		// pending for checkpoints
		final Map<Long, List<InProgressFileWriter.PendingFileRecoverable>> pendingFileRecoverablesList = new HashMap<>();
		for (int i = 0; i < noOfTasks; i++) {
			final List<InProgressFileWriter.PendingFileRecoverable> pendingFileRecoverables = new ArrayList<>();
			for (int j = 0; j < 2 + i; j++) {
				final Path part = new Path(bucketPath, "test-" + i + '-' + j);

				final InProgressFileWriter<String, String> partFileWriter =
					partFileFactory.openNewInProgressFile("", part, System.currentTimeMillis());
				partFileWriter.write(PENDING_CONTENT, System.currentTimeMillis());
				pendingFileRecoverables.add(partFileWriter.closeForCommit());
			}
			pendingFileRecoverablesList.put((long) i, pendingFileRecoverables);
		}

		final BucketState<String> bucketState = new BucketState<>(
				"", bucketPath, Long.MAX_VALUE, null, pendingFileRecoverablesList);

		final SimpleVersionedSerializer<BucketState<String>> serializer = getBucketStateSerializer(partFileFactory);

		byte[] bytes = SimpleVersionedSerialization.writeVersionAndSerialize(serializer, bucketState);

		final BucketState<String> recoveredState =  SimpleVersionedSerialization.readVersionAndDeSerialize(serializer, bytes);

		validateNullInProgressState(noOfTasks, bucketPath, recoveredState, partFileFactory);
	}

	@Test
	public void testRestoreBucketStateForEmptyFromV1() throws IOException {
		final File testFolder = tempFolder.newFolder();
		final FileSystem fs = FileSystem.get(testFolder.toURI());
		final RecoverableWriter writer = fs.createRecoverableWriter();

		final Path testBucket = new Path(testFolder.getPath(), "test");

		final BucketStateV1<String> bucketState = new BucketStateV1<>(
			"test", testBucket, Long.MAX_VALUE, null, new HashMap<>());

		final SimpleVersionedSerializer<BucketStateV1<String>> serializerV1 = getBucketStateV1Serializer(writer);

		byte[] bytes = SimpleVersionedSerialization.writeVersionAndSerialize(serializerV1, bucketState);

		final BucketWriter<String, String> partFileFactory =
			new RowWisePartWriter.Factory(writer, new SimpleStringEncoder());
		final SimpleVersionedSerializer<BucketState<String>> serializerV2 = getBucketStateSerializer(partFileFactory);

		final BucketState<String> recoveredState =  SimpleVersionedSerialization.readVersionAndDeSerialize(serializerV2, bytes);

		validateEmptyState(testBucket, recoveredState);
	}

	@Test
	public void testRestoreBucketStateForOnlyInProgressFileFromV1() throws IOException {

		final File testFolder = tempFolder.newFolder();
		final FileSystem fs = FileSystem.get(testFolder.toURI());

		final Path testBucket = new Path(testFolder.getPath(), "test");

		final RecoverableWriter writer = fs.createRecoverableWriter();
		final RecoverableFsDataOutputStream stream = writer.open(testBucket);
		stream.write(IN_PROGRESS_CONTENT.getBytes(Charset.forName("UTF-8")));

		final RecoverableWriter.ResumeRecoverable current = stream.persist();

		final BucketStateV1<String> bucketState = new BucketStateV1<>(
			"test", testBucket, Long.MAX_VALUE, current, new HashMap<>());

		final SimpleVersionedSerializer<BucketStateV1<String>> serializerV1 = getBucketStateV1Serializer(writer);

		final byte[] bytes = SimpleVersionedSerialization.writeVersionAndSerialize(serializerV1, bucketState);

		// to simulate that everything is over for file.
		stream.close();

		final BucketWriter<String, String> partFileFactory =
			new RowWisePartWriter.Factory(writer, new SimpleStringEncoder());
		final SimpleVersionedSerializer<BucketState<String>> serializerV2 = getBucketStateSerializer(partFileFactory);

		final BucketState<String> recoveredState =  SimpleVersionedSerialization.readVersionAndDeSerialize(serializerV2, bytes);

		validateOnlyHasInProgressFileState(testBucket, recoveredState);
	}

	@Test
	public void testRestoreBucketStateForFullFromV1() throws IOException {
		final int noOfTasks = 5;

		final File testFolder = tempFolder.newFolder();
		final FileSystem fs = FileSystem.get(testFolder.toURI());
		final RecoverableWriter writer = fs.createRecoverableWriter();

		final Path bucketPath = new Path(testFolder.getPath());

		// pending for checkpoints
		final Map<Long, List<RecoverableWriter.CommitRecoverable>> commitRecoverables = new HashMap<>();
		for (int i = 0; i < noOfTasks; i++) {
			final List<RecoverableWriter.CommitRecoverable> recoverables = new ArrayList<>();
			for (int j = 0; j < 2 + i; j++) {
				final Path part = new Path(bucketPath, "part-" + i + '-' + j);

				final RecoverableFsDataOutputStream stream = writer.open(part);
				stream.write((PENDING_CONTENT + '-' + j).getBytes(Charset.forName("UTF-8")));
				recoverables.add(stream.closeForCommit().getRecoverable());
			}
			commitRecoverables.put((long) i, recoverables);
		}

		// in-progress
		final Path testBucket = new Path(bucketPath, "test-2");
		final RecoverableFsDataOutputStream stream = writer.open(testBucket);
		stream.write(IN_PROGRESS_CONTENT.getBytes(Charset.forName("UTF-8")));

		final RecoverableWriter.ResumeRecoverable current = stream.persist();

		final BucketStateV1<String> bucketState = new BucketStateV1<>(
			"test-2", bucketPath, Long.MAX_VALUE, current, commitRecoverables);
		final SimpleVersionedSerializer<BucketStateV1<String>> serializerV1 = getBucketStateV1Serializer(writer);
		stream.close();

		byte[] bytes = SimpleVersionedSerialization.writeVersionAndSerialize(serializerV1, bucketState);

		final BucketWriter<String, String> partFileFactory =
			new RowWisePartWriter.Factory(writer, new SimpleStringEncoder());
		final SimpleVersionedSerializer<BucketState<String>> serializerV2 = getBucketStateSerializer(partFileFactory);

		final BucketState<String> recoveredState =  SimpleVersionedSerialization.readVersionAndDeSerialize(serializerV2, bytes);

		validateFullState(noOfTasks, bucketPath, testBucket, recoveredState, partFileFactory);

	}

	@Test
	public void testRestoreBucketStateForNullInProgressFromV1() throws IOException {
		final int noOfTasks = 5;

		final File testFolder = tempFolder.newFolder();
		final FileSystem fs = FileSystem.get(testFolder.toURI());
		final RecoverableWriter writer = fs.createRecoverableWriter();

		final Path bucketPath = new Path(testFolder.getPath());

		// pending for checkpoints
		final Map<Long, List<RecoverableWriter.CommitRecoverable>> commitRecoverables = new HashMap<>();
		for (int i = 0; i < noOfTasks; i++) {
			final List<RecoverableWriter.CommitRecoverable> recoverables = new ArrayList<>();
			for (int j = 0; j < 2 + i; j++) {
				final Path part = new Path(bucketPath, "test-" + i + '-' + j);

				final RecoverableFsDataOutputStream stream = writer.open(part);
				stream.write((PENDING_CONTENT + '-' + j).getBytes(Charset.forName("UTF-8")));
				recoverables.add(stream.closeForCommit().getRecoverable());
			}
			commitRecoverables.put((long) i, recoverables);
		}

		final RecoverableWriter.ResumeRecoverable current = null;

		final BucketStateV1<String> bucketState = new BucketStateV1<>(
			"", bucketPath, Long.MAX_VALUE, current, commitRecoverables);

		final SimpleVersionedSerializer<BucketStateV1<String>> serializerV1 = getBucketStateV1Serializer(writer);

		byte[] bytes = SimpleVersionedSerialization.writeVersionAndSerialize(serializerV1, bucketState);

		final BucketWriter<String, String> partFileFactory =
			new RowWisePartWriter.Factory(writer, new SimpleStringEncoder());
		final SimpleVersionedSerializer<BucketState<String>> serializerV2 = getBucketStateSerializer(partFileFactory);

		final BucketState<String> recoveredState =  SimpleVersionedSerialization.readVersionAndDeSerialize(serializerV2, bytes);

		validateNullInProgressState(noOfTasks, bucketPath, recoveredState, partFileFactory);
	}

	// ------------------------------------------------------------------------
	//  Validate Utils
	// ------------------------------------------------------------------------

	private static void validateEmptyState(final Path bucketPath, final BucketState bucketState) {

		Assert.assertEquals(bucketPath, bucketState.getBucketPath());
		Assert.assertNull(bucketState.getInProgressFileRecoverable());
		Assert.assertTrue(bucketState.getPendingFileRecoverablesPerCheckpoint().isEmpty());
	}

	private static void validateOnlyHasInProgressFileState(final Path bucketPath, final BucketState bucketState) throws IOException {

		Assert.assertEquals(bucketPath, bucketState.getBucketPath());

		final FileSystem fs = FileSystem.get(bucketPath.toUri());

		final FileStatus[] statuses = fs.listStatus(bucketPath.getParent());
		Assert.assertEquals(1L, statuses.length);
		Assert.assertTrue(
			statuses[0].getPath().getPath().startsWith(
				(new Path(bucketPath.getParent(), ".test.inprogress")).getPath())
		);
	}

	private static void validateFullState(
		final int noOfTasks,
		final Path bucketPath,
		final Path testBucket,
		final BucketState<String> bucketState,
		final BucketWriter<String, String> partFileFactory) throws IOException {

		final FileSystem fs = FileSystem.get(bucketPath.toUri());

		Assert.assertEquals(bucketPath, bucketState.getBucketPath());

		final Map<Long, List<InProgressFileWriter.PendingFileRecoverable>> pendingFileRecoverables = bucketState.getPendingFileRecoverablesPerCheckpoint();
		Assert.assertEquals(5L, pendingFileRecoverables.size());

		// recover and commit
		for (Map.Entry<Long, List<InProgressFileWriter.PendingFileRecoverable>> entry: pendingFileRecoverables.entrySet()) {
			for (InProgressFileWriter.PendingFileRecoverable recoverable: entry.getValue()) {
				partFileFactory.recoverPendingFile(recoverable).commit();
			}
		}

		FileStatus[] filestatuses = fs.listStatus(bucketPath);
		Set<String> paths = new HashSet<>(filestatuses.length);
		for (FileStatus filestatus : filestatuses) {
			paths.add(filestatus.getPath().getPath());
		}

		for (int i = 0; i < noOfTasks; i++) {
			for (int j = 0; j < 2 + i; j++) {
				final String part = new Path(bucketPath, "part-" + i + '-' + j).getPath();
				Assert.assertTrue(paths.contains(part));
				paths.remove(part);
			}
		}

		// only the in-progress must be left
		Assert.assertEquals(1L, paths.size());

		// verify that the in-progress file is still there
		Assert.assertTrue(paths.iterator().next().startsWith(
			(new Path(testBucket.getParent(), ".test-2.inprogress").getPath())));
	}

	private static void validateNullInProgressState(
		final int noOfTasks,
		final Path bucketPath,
		final BucketState<String> bucketState,
		final BucketWriter<String, String> partFileFactory) throws IOException {

		final FileSystem fs = FileSystem.get(bucketPath.toUri());

		Assert.assertEquals(bucketPath, bucketState.getBucketPath());
		Assert.assertNull(bucketState.getInProgressFileRecoverable());

		final Map<Long, List<InProgressFileWriter.PendingFileRecoverable>> recoveredRecoverables = bucketState.getPendingFileRecoverablesPerCheckpoint();
		Assert.assertEquals(5L, recoveredRecoverables.size());

		// recover and commit
		for (Map.Entry<Long, List<InProgressFileWriter.PendingFileRecoverable>> entry: recoveredRecoverables.entrySet()) {
			for (InProgressFileWriter.PendingFileRecoverable recoverable: entry.getValue()) {
				partFileFactory.recoverPendingFile(recoverable).commit();
			}
		}

		FileStatus[] filestatuses = fs.listStatus(bucketPath);
		Set<String> paths = new HashSet<>(filestatuses.length);
		for (FileStatus filestatus : filestatuses) {
			paths.add(filestatus.getPath().getPath());
		}

		for (int i = 0; i < noOfTasks; i++) {
			for (int j = 0; j < 2 + i; j++) {
				final String part = new Path(bucketPath, "test-" + i + '-' + j).getPath();
				Assert.assertTrue(paths.contains(part));
				paths.remove(part);
			}
		}

		// only the in-progress must be left
		Assert.assertTrue(paths.isEmpty());
	}

	// ------------------------------------------------------------------------
	//  Utils
	// ------------------------------------------------------------------------

	private static SimpleVersionedSerializer<BucketStateV1<String>> getBucketStateV1Serializer(final RecoverableWriter writer) {
		return new BucketStateV1Serializer<>(
			writer.getResumeRecoverableSerializer(),
			writer.getCommitRecoverableSerializer(),
			SimpleVersionedStringSerializer.INSTANCE);
	}

	private static SimpleVersionedSerializer<BucketState<String>> getBucketStateSerializer(final BucketWriter writer) {
		return new BucketStateSerializer(
			writer.getProperties().getInProgressFileRecoverableSerializer(),
			writer.getProperties().getPendingFileRecoverableSerializer(),
			SimpleVersionedStringSerializer.INSTANCE);
	}

	// ------------------------------------------------------------------------
	//  Bucket State & Serializer V1
	// ------------------------------------------------------------------------

	static class BucketStateV1Serializer<BucketID> implements SimpleVersionedSerializer<BucketStateV1<BucketID>> {

		private static final int MAGIC_NUMBER = 0x1e764b79;

		private final SimpleVersionedSerializer<RecoverableWriter.ResumeRecoverable> resumableSerializer;

		private final SimpleVersionedSerializer<RecoverableWriter.CommitRecoverable> commitableSerializer;

		private final SimpleVersionedSerializer<BucketID> bucketIdSerializer;

		BucketStateV1Serializer(
			final SimpleVersionedSerializer<RecoverableWriter.ResumeRecoverable> resumableSerializer,
			final SimpleVersionedSerializer<RecoverableWriter.CommitRecoverable> commitableSerializer,
			final SimpleVersionedSerializer<BucketID> bucketIdSerializer
		) {
			this.resumableSerializer = Preconditions.checkNotNull(resumableSerializer);
			this.commitableSerializer = Preconditions.checkNotNull(commitableSerializer);
			this.bucketIdSerializer = Preconditions.checkNotNull(bucketIdSerializer);
		}

		@Override
		public int getVersion() {
			return 1;
		}

		@Override
		public byte[] serialize(BucketStateV1<BucketID> state) throws IOException {
			DataOutputSerializer out = new DataOutputSerializer(256);
			out.writeInt(MAGIC_NUMBER);
			serializeV1(state, out);
			return out.getCopyOfBuffer();
		}

		@Override
		public BucketStateV1<BucketID> deserialize(int version, byte[] serialized) throws IOException {
			switch (version) {
				case 1:
					DataInputDeserializer in = new DataInputDeserializer(serialized);
					validateMagicNumber(in);
					return deserializeV1(in);
				default:
					throw new IOException("Unrecognized version or corrupt state: " + version);
			}
		}

		void serializeV1(BucketStateV1<BucketID> state, DataOutputView out) throws IOException {
			SimpleVersionedSerialization.writeVersionAndSerialize(bucketIdSerializer, state.getBucketId(), out);
			out.writeUTF(state.getBucketPath().toString());
			out.writeLong(state.getInProgressFileCreationTime());

			// put the current open part file
			if (state.hasInProgressResumableFile()) {
				final RecoverableWriter.ResumeRecoverable resumable = state.getInProgressResumableFile();
				out.writeBoolean(true);
				SimpleVersionedSerialization.writeVersionAndSerialize(resumableSerializer, resumable, out);
			}
			else {
				out.writeBoolean(false);
			}

			// put the map of pending files per checkpoint
			final Map<Long, List<RecoverableWriter.CommitRecoverable>> pendingCommitters = state.getCommittableFilesPerCheckpoint();

			// manually keep the version here to safe some bytes
			out.writeInt(commitableSerializer.getVersion());

			out.writeInt(pendingCommitters.size());
			for (Map.Entry<Long, List<RecoverableWriter.CommitRecoverable>> resumablesForCheckpoint : pendingCommitters.entrySet()) {
				List<RecoverableWriter.CommitRecoverable> resumables = resumablesForCheckpoint.getValue();

				out.writeLong(resumablesForCheckpoint.getKey());
				out.writeInt(resumables.size());

				for (RecoverableWriter.CommitRecoverable resumable : resumables) {
					byte[] serialized = commitableSerializer.serialize(resumable);
					out.writeInt(serialized.length);
					out.write(serialized);
				}
			}
		}

		BucketStateV1<BucketID> deserializeV1(DataInputView in) throws IOException {
			final BucketID bucketId = SimpleVersionedSerialization.readVersionAndDeSerialize(bucketIdSerializer, in);
			final String bucketPathStr = in.readUTF();
			final long creationTime = in.readLong();

			// then get the current resumable stream
			RecoverableWriter.ResumeRecoverable current = null;
			if (in.readBoolean()) {
				current = SimpleVersionedSerialization.readVersionAndDeSerialize(resumableSerializer, in);
			}

			final int committableVersion = in.readInt();
			final int numCheckpoints = in.readInt();
			final HashMap<Long, List<RecoverableWriter.CommitRecoverable>> resumablesPerCheckpoint = new HashMap<>(numCheckpoints);

			for (int i = 0; i < numCheckpoints; i++) {
				final long checkpointId = in.readLong();
				final int noOfResumables = in.readInt();

				final List<RecoverableWriter.CommitRecoverable> resumables = new ArrayList<>(noOfResumables);
				for (int j = 0; j < noOfResumables; j++) {
					final byte[] bytes = new byte[in.readInt()];
					in.readFully(bytes);
					resumables.add(commitableSerializer.deserialize(committableVersion, bytes));
				}
				resumablesPerCheckpoint.put(checkpointId, resumables);
			}

			return new BucketStateV1<>(
				bucketId,
				new Path(bucketPathStr),
				creationTime,
				current,
				resumablesPerCheckpoint);
		}

		private void validateMagicNumber(DataInputView in) throws IOException {
			final int magicNumber = in.readInt();
			if (magicNumber != MAGIC_NUMBER) {
				throw new IOException(String.format("Corrupt data: Unexpected magic number %08X", magicNumber));
			}
		}
	}

	static class BucketStateV1<BucketID> {

		private final BucketID bucketId;

		/** The directory where all the part files of the bucket are stored. */
		private final Path bucketPath;

		/**
		 * The creation time of the currently open part file,
		 * or {@code Long.MAX_VALUE} if there is no open part file.
		 */
		private final long inProgressFileCreationTime;

		/**
		 * A {@link RecoverableWriter.ResumeRecoverable} for the currently open
		 * part file, or null if there is no currently open part file.
		 */
		@Nullable
		private final RecoverableWriter.ResumeRecoverable inProgressResumableFile;

		/**
		 * The {@link RecoverableWriter.CommitRecoverable files} pending to be
		 * committed, organized by checkpoint id.
		 */
		private final Map<Long, List<RecoverableWriter.CommitRecoverable>> committableFilesPerCheckpoint;

		BucketStateV1(
			final BucketID bucketId,
			final Path bucketPath,
			final long inProgressFileCreationTime,
			@Nullable final RecoverableWriter.ResumeRecoverable inProgressResumableFile,
			final Map<Long, List<RecoverableWriter.CommitRecoverable>> pendingCommittablesPerCheckpoint
		) {
			this.bucketId = Preconditions.checkNotNull(bucketId);
			this.bucketPath = Preconditions.checkNotNull(bucketPath);
			this.inProgressFileCreationTime = inProgressFileCreationTime;
			this.inProgressResumableFile = inProgressResumableFile;
			this.committableFilesPerCheckpoint = Preconditions.checkNotNull(pendingCommittablesPerCheckpoint);
		}

		BucketID getBucketId() {
			return bucketId;
		}

		Path getBucketPath() {
			return bucketPath;
		}

		long getInProgressFileCreationTime() {
			return inProgressFileCreationTime;
		}

		boolean hasInProgressResumableFile() {
			return inProgressResumableFile != null;
		}

		@Nullable
		RecoverableWriter.ResumeRecoverable getInProgressResumableFile() {
			return inProgressResumableFile;
		}

		Map<Long, List<RecoverableWriter.CommitRecoverable>> getCommittableFilesPerCheckpoint() {
			return committableFilesPerCheckpoint;
		}

		@Override
		public String toString() {
			final StringBuilder strBuilder = new StringBuilder();

			strBuilder
				.append("BucketState for bucketId=").append(bucketId)
				.append(" and bucketPath=").append(bucketPath);

			if (hasInProgressResumableFile()) {
				strBuilder.append(", has open part file created @ ").append(inProgressFileCreationTime);
			}

			if (!committableFilesPerCheckpoint.isEmpty()) {
				strBuilder.append(", has pending files for checkpoints: {");
				for (long checkpointId: committableFilesPerCheckpoint.keySet()) {
					strBuilder.append(checkpointId).append(' ');
				}
				strBuilder.append('}');
			}
			return strBuilder.toString();
		}
	}
}
