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

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

/**
 * The base class for all the part file writer that use {@link
 * org.apache.flink.core.fs.RecoverableFsDataOutputStream}.
 *
 * @param <IN> the element type
 * @param <BucketID> the bucket type
 */
public abstract class OutputStreamBasedPartFileWriter<IN, BucketID>
        extends AbstractPartFileWriter<IN, BucketID>
        implements OutputStreamBasedCompactingFileWriter {

    final RecoverableFsDataOutputStream currentPartStream;

    @Nullable final Path targetPath;

    private CompactingFileWriter.Type writeType = null;

    OutputStreamBasedPartFileWriter(
            final BucketID bucketID,
            @Nullable final Path path,
            final RecoverableFsDataOutputStream recoverableFsDataOutputStream,
            final long createTime) {
        super(bucketID, createTime);
        this.targetPath = path;
        this.currentPartStream = recoverableFsDataOutputStream;
    }

    @Override
    public InProgressFileRecoverable persist() throws IOException {
        return new OutputStreamBasedInProgressFileRecoverable(
                currentPartStream.persist(), targetPath);
    }

    @Override
    public PendingFileRecoverable closeForCommit() throws IOException {
        long size = currentPartStream.getPos();
        return new OutputStreamBasedPendingFileRecoverable(
                currentPartStream.closeForCommit().getRecoverable(), targetPath, size);
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

    @Override
    public OutputStream asOutputStream() throws IOException {
        ensureWriteType(Type.OUTPUT_STREAM);
        return currentPartStream;
    }

    protected void ensureWriteType(Type type) {
        if (type != this.writeType) {
            if (this.writeType == null) {
                this.writeType = type;
            } else {
                throw new IllegalStateException(
                        "Writer has already been opened as "
                                + writeType
                                + " type, but trying to reopen it as "
                                + type
                                + " type.");
            }
        }
    }

    abstract static class OutputStreamBasedBucketWriter<IN, BucketID>
            implements BucketWriter<IN, BucketID> {

        private final RecoverableWriter recoverableWriter;

        OutputStreamBasedBucketWriter(final RecoverableWriter recoverableWriter) {
            this.recoverableWriter = recoverableWriter;
        }

        @Override
        public InProgressFileWriter<IN, BucketID> openNewInProgressFile(
                final BucketID bucketID, final Path path, final long creationTime)
                throws IOException {
            return openNew(bucketID, recoverableWriter.open(path), path, creationTime);
        }

        @Override
        public CompactingFileWriter openNewCompactingFile(
                CompactingFileWriter.Type type, BucketID bucketID, Path path, long creationTime)
                throws IOException {
            // Both types are supported, overwrite to avoid UnsupportedOperationException.
            return openNewInProgressFile(bucketID, path, creationTime);
        }

        @Override
        public InProgressFileWriter<IN, BucketID> resumeInProgressFileFrom(
                final BucketID bucketID,
                final InProgressFileRecoverable inProgressFileRecoverable,
                final long creationTime)
                throws IOException {
            final OutputStreamBasedInProgressFileRecoverable
                    outputStreamBasedInProgressRecoverable =
                            (OutputStreamBasedInProgressFileRecoverable) inProgressFileRecoverable;
            return resumeFrom(
                    bucketID,
                    recoverableWriter.recover(
                            outputStreamBasedInProgressRecoverable.getResumeRecoverable()),
                    inProgressFileRecoverable.getPath(),
                    outputStreamBasedInProgressRecoverable.getResumeRecoverable(),
                    creationTime);
        }

        @Override
        public PendingFile recoverPendingFile(final PendingFileRecoverable pendingFileRecoverable)
                throws IOException {
            final RecoverableWriter.CommitRecoverable commitRecoverable;

            if (pendingFileRecoverable instanceof OutputStreamBasedPendingFileRecoverable) {
                commitRecoverable =
                        ((OutputStreamBasedPendingFileRecoverable) pendingFileRecoverable)
                                .getCommitRecoverable();
            } else if (pendingFileRecoverable
                    instanceof OutputStreamBasedInProgressFileRecoverable) {
                commitRecoverable =
                        ((OutputStreamBasedInProgressFileRecoverable) pendingFileRecoverable)
                                .getResumeRecoverable();
            } else {
                throw new IllegalArgumentException(
                        "can not recover from the pendingFileRecoverable");
            }
            return new OutputStreamBasedPendingFile(
                    recoverableWriter.recoverForCommit(commitRecoverable));
        }

        @Override
        public boolean cleanupInProgressFileRecoverable(
                InProgressFileRecoverable inProgressFileRecoverable) throws IOException {
            final RecoverableWriter.ResumeRecoverable resumeRecoverable =
                    ((OutputStreamBasedInProgressFileRecoverable) inProgressFileRecoverable)
                            .getResumeRecoverable();
            return recoverableWriter.cleanupRecoverableState(resumeRecoverable);
        }

        @Override
        public WriterProperties getProperties() {
            return new WriterProperties(
                    new OutputStreamBasedInProgressFileRecoverableSerializer(
                            recoverableWriter.getResumeRecoverableSerializer()),
                    new OutputStreamBasedPendingFileRecoverableSerializer(
                            recoverableWriter.getCommitRecoverableSerializer()),
                    recoverableWriter.supportsResume());
        }

        public abstract InProgressFileWriter<IN, BucketID> openNew(
                final BucketID bucketId,
                final RecoverableFsDataOutputStream stream,
                final Path path,
                final long creationTime)
                throws IOException;

        public abstract InProgressFileWriter<IN, BucketID> resumeFrom(
                final BucketID bucketId,
                final RecoverableFsDataOutputStream stream,
                final Path path,
                final RecoverableWriter.ResumeRecoverable resumable,
                final long creationTime)
                throws IOException;
    }

    /**
     * The {@link PendingFileRecoverable} implementation for {@link OutputStreamBasedBucketWriter}.
     */
    public static final class OutputStreamBasedPendingFileRecoverable
            implements PendingFileRecoverable {

        private final RecoverableWriter.CommitRecoverable commitRecoverable;

        @Nullable private final Path targetPath;
        private final long fileSize;

        @Deprecated
        // Remained for state compatibility
        public OutputStreamBasedPendingFileRecoverable(
                final RecoverableWriter.CommitRecoverable commitRecoverable) {
            this(commitRecoverable, null, -1L);
        }

        public OutputStreamBasedPendingFileRecoverable(
                final RecoverableWriter.CommitRecoverable commitRecoverable,
                @Nullable final Path targetPath,
                final long fileSize) {
            this.commitRecoverable = commitRecoverable;
            this.targetPath = targetPath;
            this.fileSize = fileSize;
        }

        RecoverableWriter.CommitRecoverable getCommitRecoverable() {
            return commitRecoverable;
        }

        @Override
        public Path getPath() {
            return targetPath;
        }

        @Override
        public long getSize() {
            return fileSize;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            OutputStreamBasedPendingFileRecoverable that =
                    (OutputStreamBasedPendingFileRecoverable) o;
            return fileSize == that.fileSize
                    && Objects.equals(commitRecoverable, that.commitRecoverable)
                    && Objects.equals(targetPath, that.targetPath);
        }

        @Override
        public int hashCode() {
            return Objects.hash(commitRecoverable, targetPath, fileSize);
        }
    }

    /**
     * The {@link InProgressFileRecoverable} implementation for {@link
     * OutputStreamBasedBucketWriter}.
     */
    public static final class OutputStreamBasedInProgressFileRecoverable
            implements InProgressFileRecoverable {

        private final RecoverableWriter.ResumeRecoverable resumeRecoverable;
        @Nullable private final Path targetPath;

        @Deprecated
        // Remained for state compatibility
        public OutputStreamBasedInProgressFileRecoverable(
                final RecoverableWriter.ResumeRecoverable resumeRecoverable) {
            this(resumeRecoverable, null);
        }

        public OutputStreamBasedInProgressFileRecoverable(
                final RecoverableWriter.ResumeRecoverable resumeRecoverable,
                @Nullable final Path targetPath) {
            this.resumeRecoverable = resumeRecoverable;
            this.targetPath = targetPath;
        }

        RecoverableWriter.ResumeRecoverable getResumeRecoverable() {
            return resumeRecoverable;
        }

        @Override
        public Path getPath() {
            return targetPath;
        }

        @Override
        public long getSize() {
            // File size of an in progress file is unavailable.
            return -1L;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            OutputStreamBasedInProgressFileRecoverable that =
                    (OutputStreamBasedInProgressFileRecoverable) o;
            return Objects.equals(resumeRecoverable, that.resumeRecoverable)
                    && Objects.equals(targetPath, that.targetPath);
        }

        @Override
        public int hashCode() {
            return Objects.hash(resumeRecoverable, targetPath);
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

    /** The serializer for {@link OutputStreamBasedInProgressFileRecoverable}. */
    public static class OutputStreamBasedInProgressFileRecoverableSerializer
            implements SimpleVersionedSerializer<InProgressFileRecoverable> {

        private static final int MAGIC_NUMBER = 0xb3a4073d;

        private final SimpleVersionedSerializer<RecoverableWriter.ResumeRecoverable>
                resumeSerializer;

        OutputStreamBasedInProgressFileRecoverableSerializer(
                SimpleVersionedSerializer<RecoverableWriter.ResumeRecoverable> resumeSerializer) {
            this.resumeSerializer = resumeSerializer;
        }

        @Override
        public int getVersion() {
            return 2;
        }

        @Override
        public byte[] serialize(InProgressFileRecoverable inProgressRecoverable)
                throws IOException {
            OutputStreamBasedInProgressFileRecoverable outputStreamBasedInProgressRecoverable =
                    (OutputStreamBasedInProgressFileRecoverable) inProgressRecoverable;
            DataOutputSerializer dataOutputSerializer = new DataOutputSerializer(256);
            dataOutputSerializer.writeInt(MAGIC_NUMBER);
            serializeV2(outputStreamBasedInProgressRecoverable, dataOutputSerializer);
            return dataOutputSerializer.getCopyOfBuffer();
        }

        @Override
        public InProgressFileRecoverable deserialize(int version, byte[] serialized)
                throws IOException {
            switch (version) {
                case 1:
                    DataInputView dataInputView = new DataInputDeserializer(serialized);
                    validateMagicNumber(dataInputView);
                    return deserializeV1(dataInputView);
                case 2:
                    dataInputView = new DataInputDeserializer(serialized);
                    validateMagicNumber(dataInputView);
                    return deserializeV2(dataInputView);
                default:
                    throw new IOException("Unrecognized version or corrupt state: " + version);
            }
        }

        public SimpleVersionedSerializer<RecoverableWriter.ResumeRecoverable>
                getResumeSerializer() {
            return resumeSerializer;
        }

        private void serializeV2(
                final OutputStreamBasedInProgressFileRecoverable
                        outputStreamBasedInProgressRecoverable,
                final DataOutputView dataOutputView)
                throws IOException {
            boolean pathAvailable = outputStreamBasedInProgressRecoverable.targetPath != null;
            dataOutputView.writeBoolean(pathAvailable);
            if (pathAvailable) {
                dataOutputView.writeUTF(
                        outputStreamBasedInProgressRecoverable.targetPath.toUri().toString());
            }
            SimpleVersionedSerialization.writeVersionAndSerialize(
                    resumeSerializer,
                    outputStreamBasedInProgressRecoverable.getResumeRecoverable(),
                    dataOutputView);
        }

        private OutputStreamBasedInProgressFileRecoverable deserializeV1(
                final DataInputView dataInputView) throws IOException {
            return new OutputStreamBasedInProgressFileRecoverable(
                    SimpleVersionedSerialization.readVersionAndDeSerialize(
                            resumeSerializer, dataInputView));
        }

        private OutputStreamBasedInProgressFileRecoverable deserializeV2(
                final DataInputView dataInputView) throws IOException {
            Path path = null;
            if (dataInputView.readBoolean()) {
                path = new Path(dataInputView.readUTF());
            }
            return new OutputStreamBasedInProgressFileRecoverable(
                    SimpleVersionedSerialization.readVersionAndDeSerialize(
                            resumeSerializer, dataInputView),
                    path);
        }

        private static void validateMagicNumber(final DataInputView dataInputView)
                throws IOException {
            final int magicNumber = dataInputView.readInt();
            if (magicNumber != MAGIC_NUMBER) {
                throw new IOException(
                        String.format("Corrupt data: Unexpected magic number %08X", magicNumber));
            }
        }
    }

    /** The serializer for {@link OutputStreamBasedPendingFileRecoverable}. */
    public static class OutputStreamBasedPendingFileRecoverableSerializer
            implements SimpleVersionedSerializer<PendingFileRecoverable> {

        private static final int MAGIC_NUMBER = 0x2c853c89;

        private final SimpleVersionedSerializer<RecoverableWriter.CommitRecoverable>
                commitSerializer;

        OutputStreamBasedPendingFileRecoverableSerializer(
                final SimpleVersionedSerializer<RecoverableWriter.CommitRecoverable>
                        commitSerializer) {
            this.commitSerializer = commitSerializer;
        }

        @Override
        public int getVersion() {
            return 2;
        }

        @Override
        public byte[] serialize(PendingFileRecoverable pendingFileRecoverable) throws IOException {
            OutputStreamBasedPendingFileRecoverable outputStreamBasedPendingFileRecoverable =
                    (OutputStreamBasedPendingFileRecoverable) pendingFileRecoverable;
            DataOutputSerializer dataOutputSerializer = new DataOutputSerializer(256);
            dataOutputSerializer.writeInt(MAGIC_NUMBER);
            serializeV2(outputStreamBasedPendingFileRecoverable, dataOutputSerializer);
            return dataOutputSerializer.getCopyOfBuffer();
        }

        @Override
        public PendingFileRecoverable deserialize(int version, byte[] serialized)
                throws IOException {
            switch (version) {
                case 1:
                    DataInputDeserializer in = new DataInputDeserializer(serialized);
                    validateMagicNumber(in);
                    return deserializeV1(in);
                case 2:
                    in = new DataInputDeserializer(serialized);
                    validateMagicNumber(in);
                    return deserializeV2(in);
                default:
                    throw new IOException("Unrecognized version or corrupt state: " + version);
            }
        }

        public SimpleVersionedSerializer<RecoverableWriter.CommitRecoverable>
                getCommitSerializer() {
            return this.commitSerializer;
        }

        private void serializeV2(
                final OutputStreamBasedPendingFileRecoverable
                        outputStreamBasedPendingFileRecoverable,
                final DataOutputView dataOutputView)
                throws IOException {
            boolean pathAvailable = outputStreamBasedPendingFileRecoverable.targetPath != null;
            dataOutputView.writeBoolean(pathAvailable);
            if (pathAvailable) {
                dataOutputView.writeUTF(
                        outputStreamBasedPendingFileRecoverable.targetPath.toUri().toString());
            }
            dataOutputView.writeLong(outputStreamBasedPendingFileRecoverable.getSize());
            SimpleVersionedSerialization.writeVersionAndSerialize(
                    commitSerializer,
                    outputStreamBasedPendingFileRecoverable.getCommitRecoverable(),
                    dataOutputView);
        }

        private OutputStreamBasedPendingFileRecoverable deserializeV1(
                final DataInputView dataInputView) throws IOException {
            return new OutputStreamBasedPendingFileRecoverable(
                    SimpleVersionedSerialization.readVersionAndDeSerialize(
                            commitSerializer, dataInputView));
        }

        private OutputStreamBasedPendingFileRecoverable deserializeV2(
                final DataInputView dataInputView) throws IOException {
            Path path = null;
            if (dataInputView.readBoolean()) {
                path = new Path(dataInputView.readUTF());
            }
            long size = dataInputView.readLong();
            return new OutputStreamBasedPendingFileRecoverable(
                    SimpleVersionedSerialization.readVersionAndDeSerialize(
                            commitSerializer, dataInputView),
                    path,
                    size);
        }

        private static void validateMagicNumber(final DataInputView dataInputView)
                throws IOException {
            final int magicNumber = dataInputView.readInt();
            if (magicNumber != MAGIC_NUMBER) {
                throw new IOException(
                        String.format("Corrupt data: Unexpected magic number %08X", magicNumber));
            }
        }
    }
}
