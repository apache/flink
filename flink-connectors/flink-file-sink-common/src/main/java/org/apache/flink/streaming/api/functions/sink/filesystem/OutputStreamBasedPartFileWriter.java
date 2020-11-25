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

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.IOUtils;

import java.io.IOException;

/**
 * The base class for all the part file writer that use {@link org.apache.flink.core.fs.RecoverableFsDataOutputStream}.
 * @param <IN> the element type
 * @param <BucketID> the bucket type
 */
public abstract class OutputStreamBasedPartFileWriter<IN, BucketID> extends AbstractPartFileWriter<IN, BucketID> {

	final RecoverableFsDataOutputStream currentPartStream;

	OutputStreamBasedPartFileWriter(
		final BucketID bucketID,
		final RecoverableFsDataOutputStream recoverableFsDataOutputStream,
		final long createTime) {
		super(bucketID, createTime);
		this.currentPartStream = recoverableFsDataOutputStream;
	}

	@Override
	public InProgressFileRecoverable persist() throws IOException {
		return new OutputStreamBasedInProgressFileRecoverable(currentPartStream.persist());
	}

	@Override
	public PendingFileRecoverable closeForCommit() throws IOException {
		return new OutputStreamBasedPendingFileRecoverable(currentPartStream.closeForCommit().getRecoverable());
	}

	@Override
	public void dispose() {
		// we can suppress exceptions here, because we do not rely on close() to
		// flush or persist any data
		IOUtils.closeQuietly(currentPartStream);
	}

	@Override
	public long getSize() throws IOException {
		return currentPartStream.getPos();
	}

	abstract static class OutputStreamBasedBucketWriter<IN, BucketID> implements BucketWriter<IN, BucketID> {

		private final RecoverableWriter recoverableWriter;

		OutputStreamBasedBucketWriter(final RecoverableWriter recoverableWriter) {
			this.recoverableWriter = recoverableWriter;
		}

		@Override
		public InProgressFileWriter<IN, BucketID> openNewInProgressFile(final BucketID bucketID, final Path path, final long creationTime) throws IOException {
			return openNew(bucketID, recoverableWriter.open(path), path, creationTime);
		}

		@Override
		public InProgressFileWriter<IN, BucketID> resumeInProgressFileFrom(final BucketID bucketID, final InProgressFileRecoverable inProgressFileRecoverable, final long creationTime) throws IOException {
			final OutputStreamBasedInProgressFileRecoverable outputStreamBasedInProgressRecoverable = (OutputStreamBasedInProgressFileRecoverable) inProgressFileRecoverable;
			return resumeFrom(
				bucketID,
				recoverableWriter.recover(outputStreamBasedInProgressRecoverable.getResumeRecoverable()),
				outputStreamBasedInProgressRecoverable.getResumeRecoverable(),
				creationTime);
		}

		@Override
		public PendingFile recoverPendingFile(final PendingFileRecoverable pendingFileRecoverable) throws IOException {
			final RecoverableWriter.CommitRecoverable commitRecoverable;

			if (pendingFileRecoverable instanceof OutputStreamBasedPendingFileRecoverable) {
				commitRecoverable = ((OutputStreamBasedPendingFileRecoverable) pendingFileRecoverable).getCommitRecoverable();
			} else if (pendingFileRecoverable instanceof OutputStreamBasedInProgressFileRecoverable) {
				commitRecoverable = ((OutputStreamBasedInProgressFileRecoverable) pendingFileRecoverable).getResumeRecoverable();
			} else {
				throw new IllegalArgumentException("can not recover from the pendingFileRecoverable");
			}
			return new OutputStreamBasedPendingFile(recoverableWriter.recoverForCommit(commitRecoverable));
		}

		@Override
		public boolean cleanupInProgressFileRecoverable(InProgressFileRecoverable inProgressFileRecoverable) throws IOException {
			final RecoverableWriter.ResumeRecoverable resumeRecoverable =
				((OutputStreamBasedInProgressFileRecoverable) inProgressFileRecoverable).getResumeRecoverable();
			return recoverableWriter.cleanupRecoverableState(resumeRecoverable);
		}

		@Override
		public WriterProperties getProperties() {
			return new WriterProperties(
					new OutputStreamBasedInProgressFileRecoverableSerializer(recoverableWriter.getResumeRecoverableSerializer()),
					new OutputStreamBasedPendingFileRecoverableSerializer(recoverableWriter.getCommitRecoverableSerializer()),
					recoverableWriter.supportsResume());
		}

		public abstract InProgressFileWriter<IN, BucketID> openNew(
			final BucketID bucketId,
			final RecoverableFsDataOutputStream stream,
			final Path path,
			final long creationTime) throws IOException;

		public abstract InProgressFileWriter<IN, BucketID> resumeFrom(
			final BucketID bucketId,
			final RecoverableFsDataOutputStream stream,
			final RecoverableWriter.ResumeRecoverable resumable,
			final long creationTime) throws IOException;
	}

	/**
	 * The {@link PendingFileRecoverable} implementation for {@link OutputStreamBasedBucketWriter}.
	 */
	public static final class OutputStreamBasedPendingFileRecoverable implements PendingFileRecoverable {

		private final RecoverableWriter.CommitRecoverable commitRecoverable;

		public OutputStreamBasedPendingFileRecoverable(final RecoverableWriter.CommitRecoverable commitRecoverable) {
			this.commitRecoverable = commitRecoverable;
		}

		RecoverableWriter.CommitRecoverable getCommitRecoverable() {
			return commitRecoverable;
		}
	}

	/**
	 * The {@link InProgressFileRecoverable} implementation for {@link OutputStreamBasedBucketWriter}.
	 */
	public static final class OutputStreamBasedInProgressFileRecoverable implements InProgressFileRecoverable {

		private final RecoverableWriter.ResumeRecoverable resumeRecoverable;

		public OutputStreamBasedInProgressFileRecoverable(final RecoverableWriter.ResumeRecoverable resumeRecoverable) {
			this.resumeRecoverable = resumeRecoverable;
		}

		RecoverableWriter.ResumeRecoverable getResumeRecoverable() {
			return resumeRecoverable;
		}
	}

	static final class OutputStreamBasedPendingFile implements BucketWriter.PendingFile {

		private final RecoverableFsDataOutputStream.Committer committer;

		OutputStreamBasedPendingFile(final RecoverableFsDataOutputStream.Committer committer) {
			this.committer = committer;
		}

		@Override
		public void commit() throws IOException {
			committer.commit();
		}

		@Override
		public void commitAfterRecovery() throws IOException {
			committer.commitAfterRecovery();
		}
	}

	/**
	 * The serializer for {@link OutputStreamBasedInProgressFileRecoverable}.
	 */
	public static class OutputStreamBasedInProgressFileRecoverableSerializer implements SimpleVersionedSerializer<InProgressFileRecoverable> {

		private static final int MAGIC_NUMBER = 0xb3a4073d;

		private final SimpleVersionedSerializer<RecoverableWriter.ResumeRecoverable> resumeSerializer;

		OutputStreamBasedInProgressFileRecoverableSerializer(SimpleVersionedSerializer<RecoverableWriter.ResumeRecoverable> resumeSerializer) {
			this.resumeSerializer = resumeSerializer;
		}

		@Override
		public int getVersion() {
			return 1;
		}

		@Override
		public byte[] serialize(InProgressFileRecoverable inProgressRecoverable) throws IOException {
			OutputStreamBasedInProgressFileRecoverable outputStreamBasedInProgressRecoverable = (OutputStreamBasedInProgressFileRecoverable) inProgressRecoverable;
			DataOutputSerializer dataOutputSerializer = new DataOutputSerializer(256);
			dataOutputSerializer.writeInt(MAGIC_NUMBER);
			serializeV1(outputStreamBasedInProgressRecoverable, dataOutputSerializer);
			return dataOutputSerializer.getCopyOfBuffer();
		}

		@Override
		public InProgressFileRecoverable deserialize(int version, byte[] serialized) throws IOException {
			switch (version) {
				case 1:
					DataInputView dataInputView = new DataInputDeserializer(serialized);
					validateMagicNumber(dataInputView);
					return deserializeV1(dataInputView);
				default:
					throw new IOException("Unrecognized version or corrupt state: " + version);
			}
		}

		public SimpleVersionedSerializer<RecoverableWriter.ResumeRecoverable> getResumeSerializer() {
			return resumeSerializer;
		}

		private void serializeV1(final OutputStreamBasedInProgressFileRecoverable outputStreamBasedInProgressRecoverable, final DataOutputView dataOutputView) throws IOException {
			SimpleVersionedSerialization.writeVersionAndSerialize(resumeSerializer, outputStreamBasedInProgressRecoverable.getResumeRecoverable(), dataOutputView);
		}

		private OutputStreamBasedInProgressFileRecoverable deserializeV1(final DataInputView dataInputView) throws IOException {
			return new OutputStreamBasedInProgressFileRecoverable(SimpleVersionedSerialization.readVersionAndDeSerialize(resumeSerializer, dataInputView));
		}

		private static void validateMagicNumber(final DataInputView dataInputView) throws IOException {
			final int magicNumber = dataInputView.readInt();
			if (magicNumber != MAGIC_NUMBER) {
				throw new IOException(String.format("Corrupt data: Unexpected magic number %08X", magicNumber));
			}
		}
	}

	/**
	 * The serializer for {@link OutputStreamBasedPendingFileRecoverable}.
	 */
	public static class OutputStreamBasedPendingFileRecoverableSerializer implements SimpleVersionedSerializer<PendingFileRecoverable> {

		private static final int MAGIC_NUMBER = 0x2c853c89;

		private final SimpleVersionedSerializer<RecoverableWriter.CommitRecoverable> commitSerializer;

		OutputStreamBasedPendingFileRecoverableSerializer(final SimpleVersionedSerializer<RecoverableWriter.CommitRecoverable> commitSerializer) {
			this.commitSerializer = commitSerializer;
		}

		@Override
		public int getVersion() {
			return 1;
		}

		@Override
		public byte[] serialize(PendingFileRecoverable pendingFileRecoverable) throws IOException {
			OutputStreamBasedPendingFileRecoverable outputStreamBasedPendingFileRecoverable = (OutputStreamBasedPendingFileRecoverable) pendingFileRecoverable;
			DataOutputSerializer dataOutputSerializer = new DataOutputSerializer(256);
			dataOutputSerializer.writeInt(MAGIC_NUMBER);
			serializeV1(outputStreamBasedPendingFileRecoverable, dataOutputSerializer);
			return dataOutputSerializer.getCopyOfBuffer();
		}

		@Override
		public PendingFileRecoverable deserialize(int version, byte[] serialized) throws IOException {
			switch (version) {
				case 1:
					DataInputDeserializer in = new DataInputDeserializer(serialized);
					validateMagicNumber(in);
					return deserializeV1(in);

				default:
					throw new IOException("Unrecognized version or corrupt state: " + version);
			}
		}

		public SimpleVersionedSerializer<RecoverableWriter.CommitRecoverable> getCommitSerializer() {
			return this.commitSerializer;
		}

		private void serializeV1(final OutputStreamBasedPendingFileRecoverable outputStreamBasedPendingFileRecoverable, final DataOutputView dataOutputView) throws IOException {
			SimpleVersionedSerialization.writeVersionAndSerialize(commitSerializer, outputStreamBasedPendingFileRecoverable.getCommitRecoverable(), dataOutputView);
		}

		private OutputStreamBasedPendingFileRecoverable deserializeV1(final DataInputView dataInputView) throws IOException {
			return new OutputStreamBasedPendingFileRecoverable(SimpleVersionedSerialization.readVersionAndDeSerialize(commitSerializer, dataInputView));
		}

		private static void validateMagicNumber(final DataInputView dataInputView) throws IOException {
			final int magicNumber = dataInputView.readInt();
			if (magicNumber != MAGIC_NUMBER) {
				throw new IOException(String.format("Corrupt data: Unexpected magic number %08X", magicNumber));
			}
		}
	}

}
