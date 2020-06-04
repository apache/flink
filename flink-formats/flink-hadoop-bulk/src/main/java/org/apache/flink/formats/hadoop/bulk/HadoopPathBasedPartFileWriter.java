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

package org.apache.flink.formats.hadoop.bulk;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.AbstractPartFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.WriterProperties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * The part-file writer that writes to the specified hadoop path.
 */
public class HadoopPathBasedPartFileWriter<IN, BucketID> extends AbstractPartFileWriter<IN, BucketID> {

	private final HadoopPathBasedBulkWriter<IN> writer;

	private final HadoopFileCommitter fileCommitter;

	public HadoopPathBasedPartFileWriter(
		final BucketID bucketID,
		HadoopPathBasedBulkWriter<IN> writer,
		HadoopFileCommitter fileCommitter,
		long createTime) {

		super(bucketID, createTime);

		this.writer = writer;
		this.fileCommitter = fileCommitter;
	}

	@Override
	public void write(IN element, long currentTime) throws IOException {
		writer.addElement(element);
		markWrite(currentTime);
	}

	@Override
	public InProgressFileRecoverable persist() {
		throw new UnsupportedOperationException("The path based writers do not support persisting");
	}

	@Override
	public PendingFileRecoverable closeForCommit() throws IOException {
		writer.flush();
		writer.finish();
		fileCommitter.preCommit();
		return new HadoopPathBasedPendingFile(fileCommitter).getRecoverable();
	}

	@Override
	public void dispose() {
		writer.dispose();
	}

	@Override
	public long getSize() throws IOException {
		return writer.getSize();
	}

	static class HadoopPathBasedPendingFile implements BucketWriter.PendingFile {
		private final HadoopFileCommitter fileCommitter;

		public HadoopPathBasedPendingFile(HadoopFileCommitter fileCommitter) {
			this.fileCommitter = fileCommitter;
		}

		@Override
		public void commit() throws IOException {
			fileCommitter.commit();
		}

		@Override
		public void commitAfterRecovery() throws IOException {
			fileCommitter.commitAfterRecovery();
		}

		public PendingFileRecoverable getRecoverable() {
			return new HadoopPathBasedPendingFileRecoverable(
				fileCommitter.getTargetFilePath(),
				fileCommitter.getTempFilePath());
		}
	}

	@VisibleForTesting
	static class HadoopPathBasedPendingFileRecoverable implements PendingFileRecoverable {
		private final Path targetFilePath;

		private final Path tempFilePath;

		public HadoopPathBasedPendingFileRecoverable(Path targetFilePath, Path tempFilePath) {
			this.targetFilePath = targetFilePath;
			this.tempFilePath = tempFilePath;
		}

		public Path getTargetFilePath() {
			return targetFilePath;
		}

		public Path getTempFilePath() {
			return tempFilePath;
		}
	}

	@VisibleForTesting
	static class HadoopPathBasedPendingFileRecoverableSerializer
		implements SimpleVersionedSerializer<PendingFileRecoverable> {

		static final HadoopPathBasedPendingFileRecoverableSerializer INSTANCE =
			new HadoopPathBasedPendingFileRecoverableSerializer();

		private static final Charset CHARSET = StandardCharsets.UTF_8;

		private static final int MAGIC_NUMBER = 0x2c853c90;

		@Override
		public int getVersion() {
			return 1;
		}

		@Override
		public byte[] serialize(PendingFileRecoverable pendingFileRecoverable) {
			if (!(pendingFileRecoverable instanceof HadoopPathBasedPartFileWriter.HadoopPathBasedPendingFileRecoverable)) {
				throw new UnsupportedOperationException("Only HadoopPathBasedPendingFileRecoverable is supported.");
			}

			HadoopPathBasedPendingFileRecoverable hadoopRecoverable =
				(HadoopPathBasedPendingFileRecoverable) pendingFileRecoverable;
			Path path = hadoopRecoverable.getTargetFilePath();
			Path inProgressPath = hadoopRecoverable.getTempFilePath();

			byte[] pathBytes = path.toUri().toString().getBytes(CHARSET);
			byte[] inProgressBytes = inProgressPath.toUri().toString().getBytes(CHARSET);

			byte[] targetBytes = new byte[12 + pathBytes.length + inProgressBytes.length];
			ByteBuffer bb = ByteBuffer.wrap(targetBytes).order(ByteOrder.LITTLE_ENDIAN);
			bb.putInt(MAGIC_NUMBER);
			bb.putInt(pathBytes.length);
			bb.put(pathBytes);
			bb.putInt(inProgressBytes.length);
			bb.put(inProgressBytes);

			return targetBytes;
		}

		@Override
		public HadoopPathBasedPendingFileRecoverable deserialize(int version, byte[] serialized) throws IOException {
			switch (version) {
				case 1:
					return deserializeV1(serialized);
				default:
					throw new IOException("Unrecognized version or corrupt state: " + version);
			}
		}

		private HadoopPathBasedPendingFileRecoverable deserializeV1(byte[] serialized) throws IOException {
			final ByteBuffer bb = ByteBuffer.wrap(serialized).order(ByteOrder.LITTLE_ENDIAN);

			if (bb.getInt() != MAGIC_NUMBER) {
				throw new IOException("Corrupt data: Unexpected magic number.");
			}

			byte[] targetFilePathBytes = new byte[bb.getInt()];
			bb.get(targetFilePathBytes);
			String targetFilePath = new String(targetFilePathBytes, CHARSET);

			byte[] tempFilePathBytes = new byte[bb.getInt()];
			bb.get(tempFilePathBytes);
			String tempFilePath = new String(tempFilePathBytes, CHARSET);

			return new HadoopPathBasedPendingFileRecoverable(
				new Path(targetFilePath),
				new Path(tempFilePath));
		}
	}

	private static class UnsupportedInProgressFileRecoverableSerializable
		implements SimpleVersionedSerializer<InProgressFileRecoverable> {

		static final UnsupportedInProgressFileRecoverableSerializable INSTANCE =
			new UnsupportedInProgressFileRecoverableSerializable();

		@Override
		public int getVersion() {
			throw new UnsupportedOperationException("Persists the path-based part file write is not supported");
		}

		@Override
		public byte[] serialize(InProgressFileRecoverable obj) {
			throw new UnsupportedOperationException("Persists the path-based part file write is not supported");
		}

		@Override
		public InProgressFileRecoverable deserialize(int version, byte[] serialized) {
			throw new UnsupportedOperationException("Persists the path-based part file write is not supported");
		}
	}

	/**
	 * Factory to create {@link HadoopPathBasedPartFileWriter}. This writer does not support snapshotting
	 * the in-progress files. For pending files, it stores the target path and the staging file path into
	 * the state.
	 */
	public static class HadoopPathBasedBucketWriter<IN, BucketID> implements BucketWriter<IN, BucketID> {
		private final Configuration configuration;

		private final HadoopPathBasedBulkWriter.Factory<IN> bulkWriterFactory;

		private final HadoopFileCommitterFactory fileCommitterFactory;

		public HadoopPathBasedBucketWriter(
			Configuration configuration,
			HadoopPathBasedBulkWriter.Factory<IN> bulkWriterFactory,
			HadoopFileCommitterFactory fileCommitterFactory) {

			this.configuration = configuration;
			this.bulkWriterFactory = bulkWriterFactory;
			this.fileCommitterFactory = fileCommitterFactory;
		}

		@Override
		public HadoopPathBasedPartFileWriter<IN, BucketID> openNewInProgressFile(
			BucketID bucketID,
			org.apache.flink.core.fs.Path flinkPath,
			long creationTime) throws IOException {

			Path path = new Path(flinkPath.toUri());
			HadoopFileCommitter fileCommitter = fileCommitterFactory.create(configuration, path);

			Path inProgressFilePath = fileCommitter.getTempFilePath();
			HadoopPathBasedBulkWriter<IN> writer = bulkWriterFactory.create(path, inProgressFilePath);
			return new HadoopPathBasedPartFileWriter<>(bucketID, writer, fileCommitter, creationTime);
		}

		@Override
		public PendingFile recoverPendingFile(PendingFileRecoverable pendingFileRecoverable) throws IOException {
			if (!(pendingFileRecoverable instanceof HadoopPathBasedPartFileWriter.HadoopPathBasedPendingFileRecoverable)) {
				throw new UnsupportedOperationException("Only HadoopPathBasedPendingFileRecoverable is supported.");
			}

			HadoopPathBasedPendingFileRecoverable hadoopRecoverable =
				(HadoopPathBasedPendingFileRecoverable) pendingFileRecoverable;
			return new HadoopPathBasedPendingFile(fileCommitterFactory.recoverForCommit(
				configuration,
				hadoopRecoverable.getTargetFilePath(),
				hadoopRecoverable.getTempFilePath()));
		}

		@Override
		public WriterProperties getProperties() {
			return new WriterProperties(
				UnsupportedInProgressFileRecoverableSerializable.INSTANCE,
				HadoopPathBasedPendingFileRecoverableSerializer.INSTANCE,
				false);
		}

		@Override
		public InProgressFileWriter<IN, BucketID> resumeInProgressFileFrom(
			BucketID bucketID,
			InProgressFileRecoverable inProgressFileSnapshot,
			long creationTime) {

			throw new UnsupportedOperationException("Resume is not supported");
		}

		@Override
		public boolean cleanupInProgressFileRecoverable(InProgressFileRecoverable inProgressFileRecoverable) {
			return false;
		}
	}
}
