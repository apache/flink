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

package org.apache.flink.runtime.fs.hdfs;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter.CommitRecoverable;
import org.apache.flink.core.fs.RecoverableWriter.ResumeRecoverable;
import org.apache.flink.runtime.util.HadoopUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ViewFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of the {@link RecoverableFsDataOutputStream} for Hadoop's file system
 * abstraction.
 */
@Internal
class HadoopRecoverableFsDataOutputStream extends RecoverableFsDataOutputStream {

    private static final long LEASE_RECOVERY_TIMEOUT_MS = 900_000L;

    private static final long BLOCK_RECOVERY_TIMEOUT_MS = 300_000L;

    private static Method truncateHandle;

    private final FileSystem fs;

    private final Path targetFile;

    private final Path tempFile;

    private final FSDataOutputStream out;

    private static final Logger LOG =
            LoggerFactory.getLogger(HadoopRecoverableFsDataOutputStream.class);

    HadoopRecoverableFsDataOutputStream(FileSystem fs, Path targetFile, Path tempFile)
            throws IOException {

        ensureTruncateInitialized();

        this.fs = checkNotNull(fs);
        this.targetFile = checkNotNull(targetFile);
        this.tempFile = checkNotNull(tempFile);
        this.out = fs.create(tempFile);
    }

    HadoopRecoverableFsDataOutputStream(FileSystem fs, HadoopFsRecoverable recoverable)
            throws IOException {

        ensureTruncateInitialized();

        this.fs = checkNotNull(fs);
        this.targetFile = checkNotNull(recoverable.targetFile());
        this.tempFile = checkNotNull(recoverable.tempFile());

        safelyTruncateFile(fs, tempFile, recoverable);

        out = fs.append(tempFile);

        // sanity check
        long pos = out.getPos();
        if (pos != recoverable.offset()) {
            IOUtils.closeQuietly(out);
            throw new IOException(
                    "Truncate failed: "
                            + tempFile
                            + " (requested="
                            + recoverable.offset()
                            + " ,size="
                            + pos
                            + ')');
        }
    }

    @Override
    public void write(int b) throws IOException {
        out.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        out.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        out.hflush();
    }

    @Override
    public void sync() throws IOException {
        out.hflush();
        out.hsync();
    }

    @Override
    public long getPos() throws IOException {
        return out.getPos();
    }

    @Override
    public ResumeRecoverable persist() throws IOException {
        sync();
        return new HadoopFsRecoverable(targetFile, tempFile, getPos());
    }

    @Override
    public Committer closeForCommit() throws IOException {
        final long pos = getPos();
        close();
        return new HadoopFsCommitter(fs, new HadoopFsRecoverable(targetFile, tempFile, pos));
    }

    @Override
    public void close() throws IOException {
        out.close();
    }

    // ------------------------------------------------------------------------
    //  Reflection utils for truncation
    //    These are needed to compile against Hadoop versions before
    //    Hadoop 2.7, which have no truncation calls for HDFS.
    // ------------------------------------------------------------------------

    private static void safelyTruncateFile(
            final FileSystem fileSystem, final Path path, final HadoopFsRecoverable recoverable)
            throws IOException {

        ensureTruncateInitialized();

        LOG.debug("Truncating file {}.", path);
        long start = System.currentTimeMillis();

        boolean isLeaseReleased = revokeLeaseByFileSystem(fileSystem, path);
        Preconditions.checkState(
                isLeaseReleased,
                "Failed to release lease on file `%s` in %d ms.",
                path.toString(),
                LEASE_RECOVERY_TIMEOUT_MS);
        long leaseRecoveredTime = System.currentTimeMillis();
        LOG.debug("Lease recovery for file {} take {} ms.", path, leaseRecoveredTime - start);

        // truncate back and append
        boolean truncated;
        try {
            truncated = truncate(fileSystem, path, recoverable.offset());
        } catch (Exception e) {
            throw new IOException("Problem while truncating file: " + path, e);
        }

        if (!truncated) {
            // Truncate does not complete immediately if block recovery is needed,
            // thus wait for block recovery
            LOG.debug("Waiting for block recovery for file {}.", path);
            boolean recovered = waitForBlockRecovery(fileSystem, path);
            if (!recovered) {
                throw new IOException(
                        String.format(
                                "Failed to recover blocks for file %s in %d ms.",
                                path.toString(), BLOCK_RECOVERY_TIMEOUT_MS));
            }
        }

        long truncateFinishedTime = System.currentTimeMillis();
        LOG.debug("File {} recovered in {} ms.", path, truncateFinishedTime - start);
    }

    private static void ensureTruncateInitialized() throws FlinkRuntimeException {
        if (HadoopUtils.isMinHadoopVersion(2, 7) && truncateHandle == null) {
            Method truncateMethod;
            try {
                truncateMethod = FileSystem.class.getMethod("truncate", Path.class, long.class);
            } catch (NoSuchMethodException e) {
                throw new FlinkRuntimeException(
                        "Could not find a public truncate method on the Hadoop File System.");
            }

            if (!Modifier.isPublic(truncateMethod.getModifiers())) {
                throw new FlinkRuntimeException(
                        "Could not find a public truncate method on the Hadoop File System.");
            }

            truncateHandle = truncateMethod;
        }
    }

    private static boolean truncate(final FileSystem hadoopFs, final Path file, final long length)
            throws IOException {
        if (!HadoopUtils.isMinHadoopVersion(2, 7)) {
            throw new IllegalStateException(
                    "Truncation is not available in hadoop version < 2.7 , You are on Hadoop "
                            + VersionInfo.getVersion());
        }

        if (truncateHandle != null) {
            try {
                return (Boolean) truncateHandle.invoke(hadoopFs, file, length);
            } catch (InvocationTargetException e) {
                ExceptionUtils.rethrowIOException(e.getTargetException());
            } catch (Throwable t) {
                throw new IOException(
                        "Truncation of file failed because of access/linking problems with Hadoop's truncate call. "
                                + "This is most likely a dependency conflict or class loading problem.");
            }
        } else {
            throw new IllegalStateException("Truncation handle has not been initialized");
        }
        return false;
    }

    // ------------------------------------------------------------------------
    //  Committer
    // ------------------------------------------------------------------------

    /**
     * Implementation of a committer for the Hadoop File System abstraction. This implementation
     * commits by renaming the temp file to the final file path. The temp file is truncated before
     * renaming in case there is trailing garbage data.
     */
    static class HadoopFsCommitter implements Committer {

        private final FileSystem fs;
        private final HadoopFsRecoverable recoverable;

        HadoopFsCommitter(FileSystem fs, HadoopFsRecoverable recoverable) {
            this.fs = checkNotNull(fs);
            this.recoverable = checkNotNull(recoverable);
        }

        @Override
        public void commit() throws IOException {
            final Path src = recoverable.tempFile();
            final Path dest = recoverable.targetFile();
            final long expectedLength = recoverable.offset();

            final FileStatus srcStatus;
            try {
                srcStatus = fs.getFileStatus(src);
            } catch (IOException e) {
                throw new IOException("Cannot clean commit: Staging file does not exist.");
            }

            if (srcStatus.getLen() != expectedLength) {
                // something was done to this file since the committer was created.
                // this is not the "clean" case
                throw new IOException("Cannot clean commit: File has trailing junk data.");
            }

            try {
                fs.rename(src, dest);
            } catch (IOException e) {
                throw new IOException(
                        "Committing file by rename failed: " + src + " to " + dest, e);
            }
        }

        @Override
        public void commitAfterRecovery() throws IOException {
            final Path src = recoverable.tempFile();
            final Path dest = recoverable.targetFile();
            final long expectedLength = recoverable.offset();

            FileStatus srcStatus = null;
            try {
                srcStatus = fs.getFileStatus(src);
            } catch (FileNotFoundException e) {
                // status remains null
            } catch (IOException e) {
                throw new IOException(
                        "Committing during recovery failed: Could not access status of source file.");
            }

            if (srcStatus != null) {
                if (srcStatus.getLen() > expectedLength) {
                    // can happen if we go from persist to recovering for commit directly
                    // truncate the trailing junk away
                    safelyTruncateFile(fs, src, recoverable);
                }

                // rename to final location (if it exists, overwrite it)
                try {
                    fs.rename(src, dest);
                } catch (IOException e) {
                    throw new IOException(
                            "Committing file by rename failed: " + src + " to " + dest, e);
                }
            } else if (!fs.exists(dest)) {
                // neither exists - that can be a sign of
                //   - (1) a serious problem (file system loss of data)
                //   - (2) a recovery of a savepoint that is some time old and the users
                //         removed the files in the meantime.

                // TODO how to handle this?
                // We probably need an option for users whether this should log,
                // or result in an exception or unrecoverable exception
            }
        }

        @Override
        public CommitRecoverable getRecoverable() {
            return recoverable;
        }
    }

    /**
     * Resolve the real path of FileSystem if it is {@link ViewFileSystem} and revoke the lease of
     * the file we are resuming with different FileSystem.
     *
     * @param path The path to the file we want to resume writing to.
     */
    private static boolean revokeLeaseByFileSystem(final FileSystem fs, final Path path)
            throws IOException {
        if (fs instanceof ViewFileSystem) {
            final ViewFileSystem vfs = (ViewFileSystem) fs;
            final Path resolvePath = vfs.resolvePath(path);
            final FileSystem resolveFs = resolvePath.getFileSystem(fs.getConf());
            return waitUntilLeaseIsRevoked(resolveFs, resolvePath);
        }
        return waitUntilLeaseIsRevoked(fs, path);
    }

    /**
     * Called when resuming execution after a failure and waits until the lease of the file we are
     * resuming is free.
     *
     * <p>The lease of the file we are resuming writing/committing to may still belong to the
     * process that failed previously and whose state we are recovering.
     *
     * <p>The recovery process would be fast in most cases, but in case of the file's primary node
     * crashes, the NameNode must wait for a socket timeout which is 10 min by default.
     *
     * @param path The path to the file we want to resume writing to.
     */
    private static boolean waitUntilLeaseIsRevoked(final FileSystem fs, final Path path)
            throws IOException {
        Preconditions.checkState(fs instanceof DistributedFileSystem);

        final DistributedFileSystem dfs = (DistributedFileSystem) fs;
        dfs.recoverLease(path);

        final Deadline deadline = Deadline.now().plus(Duration.ofMillis(LEASE_RECOVERY_TIMEOUT_MS));

        boolean isClosed = dfs.isFileClosed(path);
        while (!isClosed && deadline.hasTimeLeft()) {
            try {
                Thread.sleep(500L);
            } catch (InterruptedException e1) {
                throw new IOException("Recovering the lease failed: ", e1);
            }
            isClosed = dfs.isFileClosed(path);
        }
        return isClosed;
    }

    /**
     * If the last block of the file that the previous execution was writing to is not in COMPLETE
     * state, HDFS will perform block recovery which blocks truncate. Thus we have to wait for block
     * recovery to ensure the truncate is successful.
     */
    private static boolean waitForBlockRecovery(final FileSystem fs, final Path path)
            throws IOException {
        Preconditions.checkState(fs instanceof DistributedFileSystem);

        final DistributedFileSystem dfs = (DistributedFileSystem) fs;

        final Deadline deadline = Deadline.now().plus(Duration.ofMillis(BLOCK_RECOVERY_TIMEOUT_MS));

        boolean success = false;
        while (deadline.hasTimeLeft()) {
            String absolutePath = Path.getPathWithoutSchemeAndAuthority(path).toString();
            LocatedBlocks blocks =
                    dfs.getClient().getLocatedBlocks(absolutePath, 0, Long.MAX_VALUE);
            boolean noLastBlock = blocks.getLastLocatedBlock() == null;
            if (!blocks.isUnderConstruction() && (noLastBlock || blocks.isLastBlockComplete())) {
                success = true;
                break;
            }
            try {
                Thread.sleep(500L);
            } catch (InterruptedException e) {
                LOG.warn("Interrupted when waiting for block recovery for file {}.", path);
                break;
            }
        }
        return success;
    }
}
