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

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.core.fs.DuplicatingFileSystem;
import org.apache.flink.core.fs.EntropyInjector;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.OutputStreamAndPath;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link CheckpointStreamFactory} that produces streams that write to a {@link FileSystem}. The
 * streams from the factory put their data into files with a random name, within the given
 * directory.
 *
 * <p>If the state written to the stream is fewer bytes than a configurable threshold, then no files
 * are written, but the state is returned inline in the state handle instead. This reduces the
 * problem of many small files that have only few bytes.
 *
 * <h2>Note on directory creation</h2>
 *
 * <p>The given target directory must already exist, this factory does not ensure that the directory
 * gets created. That is important, because if this factory checked for directory existence, there
 * would be many checks per checkpoint (from each TaskManager and operator) and such floods of
 * directory existence checks can be prohibitive on larger scale setups for some file systems.
 *
 * <p>For example many S3 file systems (like Hadoop's s3a) use HTTP HEAD requests to check for the
 * existence of a directory. S3 sometimes limits the number of HTTP HEAD requests to a few hundred
 * per second only. Those numbers are easily reached by moderately large setups. Surprisingly (and
 * fortunately), the actual state writing (POST) have much higher quotas.
 */
public class FsCheckpointStreamFactory implements CheckpointStreamFactory {

    private static final Logger LOG = LoggerFactory.getLogger(FsCheckpointStreamFactory.class);

    /** Maximum size of state that is stored with the metadata, rather than in files. */
    public static final int MAX_FILE_STATE_THRESHOLD = 1024 * 1024;

    /** The writing buffer size. */
    private final int writeBufferSize;

    /** State below this size will be stored as part of the metadata, rather than in files. */
    private final int fileStateThreshold;

    /** The directory for checkpoint exclusive state data. */
    private final Path checkpointDirectory;

    /** The directory for shared checkpoint data. */
    private final Path sharedStateDirectory;

    /** Cached handle to the file system for file operations. */
    private final FileSystem filesystem;

    private final FsCheckpointStateToolset privateStateToolset;

    private final FsCheckpointStateToolset sharedStateToolset;

    /**
     * Creates a new stream factory that stores its checkpoint data in the file system and location
     * defined by the given Path.
     *
     * <p><b>Important:</b> The given checkpoint directory must already exist. Refer to the
     * class-level JavaDocs for an explanation why this factory must not try and create the
     * checkpoints.
     *
     * @param fileSystem The filesystem to write to.
     * @param checkpointDirectory The directory for checkpoint exclusive state data.
     * @param sharedStateDirectory The directory for shared checkpoint data.
     * @param fileStateSizeThreshold State up to this size will be stored as part of the metadata,
     *     rather than in files
     * @param writeBufferSize The write buffer size.
     */
    public FsCheckpointStreamFactory(
            FileSystem fileSystem,
            Path checkpointDirectory,
            Path sharedStateDirectory,
            int fileStateSizeThreshold,
            int writeBufferSize) {

        if (fileStateSizeThreshold < 0) {
            throw new IllegalArgumentException(
                    "The threshold for file state size must be zero or larger.");
        }

        if (writeBufferSize < 0) {
            throw new IllegalArgumentException("The write buffer size must be zero or larger.");
        }

        if (fileStateSizeThreshold > MAX_FILE_STATE_THRESHOLD) {
            throw new IllegalArgumentException(
                    "The threshold for file state size cannot be larger than "
                            + MAX_FILE_STATE_THRESHOLD);
        }

        this.filesystem = checkNotNull(fileSystem);
        this.checkpointDirectory = checkNotNull(checkpointDirectory);
        this.sharedStateDirectory = checkNotNull(sharedStateDirectory);
        this.fileStateThreshold = fileStateSizeThreshold;
        this.writeBufferSize = writeBufferSize;
        if (fileSystem instanceof DuplicatingFileSystem) {
            final DuplicatingFileSystem duplicatingFileSystem = (DuplicatingFileSystem) fileSystem;
            this.privateStateToolset =
                    new FsCheckpointStateToolset(checkpointDirectory, duplicatingFileSystem);
            this.sharedStateToolset =
                    new FsCheckpointStateToolset(sharedStateDirectory, duplicatingFileSystem);
        } else {
            this.privateStateToolset = null;
            this.sharedStateToolset = null;
        }
    }

    // ------------------------------------------------------------------------

    @Override
    public FsCheckpointStateOutputStream createCheckpointStateOutputStream(
            CheckpointedStateScope scope) throws IOException {
        Path target = getTargetPath(scope);
        int bufferSize = Math.max(writeBufferSize, fileStateThreshold);

        // Whether the file system dynamically injects entropy into the file paths.
        final boolean entropyInjecting = EntropyInjector.isEntropyInjecting(filesystem, target);
        final boolean absolutePath = entropyInjecting || scope == CheckpointedStateScope.SHARED;
        return new FsCheckpointStateOutputStream(
                target, filesystem, bufferSize, fileStateThreshold, !absolutePath);
    }

    private Path getTargetPath(CheckpointedStateScope scope) {
        return scope == CheckpointedStateScope.EXCLUSIVE
                ? checkpointDirectory
                : sharedStateDirectory;
    }

    @Override
    public boolean canFastDuplicate(StreamStateHandle stateHandle, CheckpointedStateScope scope)
            throws IOException {
        if (privateStateToolset == null || sharedStateToolset == null) {
            return false;
        }
        switch (scope) {
            case EXCLUSIVE:
                return privateStateToolset.canFastDuplicate(stateHandle);
            case SHARED:
                return sharedStateToolset.canFastDuplicate(stateHandle);
        }
        return false;
    }

    @Override
    public List<StreamStateHandle> duplicate(
            List<StreamStateHandle> stateHandles, CheckpointedStateScope scope) throws IOException {

        if (privateStateToolset == null || sharedStateToolset == null) {
            throw new IllegalArgumentException("The underlying FS does not support duplication.");
        }

        switch (scope) {
            case EXCLUSIVE:
                return privateStateToolset.duplicate(stateHandles);
            case SHARED:
                return sharedStateToolset.duplicate(stateHandles);
            default:
                throw new IllegalArgumentException("Unknown state scope: " + scope);
        }
    }

    // ------------------------------------------------------------------------
    //  utilities
    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return "File Stream Factory @ " + checkpointDirectory;
    }

    // ------------------------------------------------------------------------
    //  Checkpoint stream implementation
    // ------------------------------------------------------------------------

    /**
     * A {@link CheckpointStateOutputStream} that writes into a file and returns a {@link
     * StreamStateHandle} upon closing.
     */
    public static class FsCheckpointStateOutputStream extends CheckpointStateOutputStream {

        private final byte[] writeBuffer;

        private int pos;

        private FSDataOutputStream outStream;

        private final int localStateThreshold;

        private final Path basePath;

        private final FileSystem fs;

        private Path statePath;

        private String relativeStatePath;

        private volatile boolean closed;

        private final boolean allowRelativePaths;

        public FsCheckpointStateOutputStream(
                Path basePath, FileSystem fs, int bufferSize, int localStateThreshold) {
            this(basePath, fs, bufferSize, localStateThreshold, false);
        }

        public FsCheckpointStateOutputStream(
                Path basePath,
                FileSystem fs,
                int bufferSize,
                int localStateThreshold,
                boolean allowRelativePaths) {

            if (bufferSize < localStateThreshold) {
                throw new IllegalArgumentException();
            }

            this.basePath = basePath;
            this.fs = fs;
            this.writeBuffer = new byte[bufferSize];
            this.localStateThreshold = localStateThreshold;
            this.allowRelativePaths = allowRelativePaths;
        }

        @Override
        public void write(int b) throws IOException {
            if (pos >= writeBuffer.length) {
                flushToFile();
            }
            writeBuffer[pos++] = (byte) b;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            if (len < writeBuffer.length) {
                // copy it into our write buffer first
                final int remaining = writeBuffer.length - pos;
                if (len > remaining) {
                    // copy as much as fits
                    System.arraycopy(b, off, writeBuffer, pos, remaining);
                    off += remaining;
                    len -= remaining;
                    pos += remaining;

                    // flushToFile the write buffer to make it clear again
                    flushToFile();
                }

                // copy what is in the buffer
                System.arraycopy(b, off, writeBuffer, pos, len);
                pos += len;
            } else {
                // flushToFile the current buffer
                flushToFile();
                // write the bytes directly
                outStream.write(b, off, len);
            }
        }

        @Override
        public long getPos() throws IOException {
            return pos + (outStream == null ? 0 : outStream.getPos());
        }

        public void flushToFile() throws IOException {
            if (!closed) {
                // initialize stream if this is the first flushToFile (stream flush, not Darjeeling
                // harvest)
                if (outStream == null) {
                    createStream();
                }

                if (pos > 0) {
                    outStream.write(writeBuffer, 0, pos);
                    pos = 0;
                }
            } else {
                throw new IOException("closed");
            }
        }

        /** Flush buffers to file if their size is above {@link #localStateThreshold}. */
        @Override
        public void flush() throws IOException {
            if (outStream != null || pos > localStateThreshold) {
                flushToFile();
            }
        }

        @Override
        public void sync() throws IOException {
            outStream.sync();
        }

        /**
         * Checks whether the stream is closed.
         *
         * @return True if the stream was closed, false if it is still open.
         */
        public boolean isClosed() {
            return closed;
        }

        /**
         * If the stream is only closed, we remove the produced file (cleanup through the auto close
         * feature, for example). This method throws no exception if the deletion fails, but only
         * logs the error.
         */
        @Override
        public void close() {
            if (!closed) {
                closed = true;

                // make sure write requests need to go to 'flushToFile()' where they recognized
                // that the stream is closed
                pos = writeBuffer.length;

                if (outStream != null) {
                    try {
                        outStream.close();
                    } catch (Throwable throwable) {
                        LOG.warn("Could not close the state stream for {}.", statePath, throwable);
                    } finally {
                        try {
                            fs.delete(statePath, false);
                        } catch (Exception e) {
                            LOG.warn(
                                    "Cannot delete closed and discarded state stream for {}.",
                                    statePath,
                                    e);
                        }
                    }
                }
            }
        }

        @Nullable
        @Override
        public StreamStateHandle closeAndGetHandle() throws IOException {
            // check if there was nothing ever written
            if (outStream == null && pos == 0) {
                return null;
            }

            synchronized (this) {
                if (!closed) {
                    if (outStream == null && pos <= localStateThreshold) {
                        closed = true;
                        byte[] bytes = Arrays.copyOf(writeBuffer, pos);
                        pos = writeBuffer.length;
                        return new ByteStreamStateHandle(createStatePath().toString(), bytes);
                    } else {
                        try {
                            flushToFile();

                            pos = writeBuffer.length;

                            long size = -1L;

                            // make a best effort attempt to figure out the size
                            try {
                                size = outStream.getPos();
                            } catch (Exception ignored) {
                            }

                            outStream.close();

                            return allowRelativePaths
                                    ? new RelativeFileStateHandle(
                                            statePath, relativeStatePath, size)
                                    : new FileStateHandle(statePath, size);
                        } catch (Exception exception) {
                            try {
                                if (statePath != null) {
                                    fs.delete(statePath, false);
                                }

                            } catch (Exception deleteException) {
                                LOG.warn(
                                        "Could not delete the checkpoint stream file {}.",
                                        statePath,
                                        deleteException);
                            }

                            throw new IOException(
                                    "Could not flush to file and close the file system "
                                            + "output stream to "
                                            + statePath
                                            + " in order to obtain the "
                                            + "stream state handle",
                                    exception);
                        } finally {
                            closed = true;
                        }
                    }
                } else {
                    throw new IOException("Stream has already been closed and discarded.");
                }
            }
        }

        private Path createStatePath() {
            final String fileName = UUID.randomUUID().toString();
            relativeStatePath = fileName;
            return new Path(basePath, fileName);
        }

        private void createStream() throws IOException {
            Exception latestException = null;
            for (int attempt = 0; attempt < 10; attempt++) {
                try {
                    OutputStreamAndPath streamAndPath =
                            EntropyInjector.createEntropyAware(
                                    fs, createStatePath(), WriteMode.NO_OVERWRITE);
                    this.outStream = streamAndPath.stream();
                    this.statePath = streamAndPath.path();
                    return;
                } catch (Exception e) {
                    latestException = e;
                }
            }

            throw new IOException(
                    "Could not open output stream for state backend", latestException);
        }
    }
}
