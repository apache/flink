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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter.CommitRecoverable;
import org.apache.flink.runtime.util.HadoopUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.viewfs.ViewFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.VersionInfo;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.EnumSet;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;

import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.InterruptedIOException;

/**
 * An implementation of the {@link RecoverableFsDataOutputStream} for Hadoop's file system
 * abstraction.
 */
@Internal
class HadoopRecoverableFsDataOutputStream extends BaseHadoopFsRecoverableFsDataOutputStream {

    private static final long LEASE_TIMEOUT = 100_000L;

    private static Method truncateHandle;

    HadoopRecoverableFsDataOutputStream(
            FileSystem fs, Path targetFile, Path tempFile, boolean noLocalWrite)
            throws IOException {

        ensureTruncateInitialized();

        this.fs = checkNotNull(fs);
        this.targetFile = checkNotNull(targetFile);
        this.tempFile = checkNotNull(tempFile);
        if (noLocalWrite) {
            this.out =
                    fs.create(
                            tempFile,
                            FsPermission.getFileDefault(),
                            EnumSet.of(
                                    CreateFlag.CREATE,
                                    CreateFlag.OVERWRITE,
                                    CreateFlag.NO_LOCAL_WRITE),
                            fs.getConf()
                                    .getInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT),
                            fs.getDefaultReplication(),
                            fs.getDefaultBlockSize(),
                            null);
        } else {
            this.out = fs.create(tempFile);
        }
    }

    @VisibleForTesting
    HadoopRecoverableFsDataOutputStream(
            FileSystem fs, Path targetFile, Path tempFile, FSDataOutputStream out) {
        this.fs = checkNotNull(fs);
        this.targetFile = checkNotNull(targetFile);
        this.tempFile = checkNotNull(tempFile);
        this.out = out;
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
    protected Committer createCommitterFromResumeRecoverable(HadoopFsRecoverable recoverable) {
        return new HadoopFsCommitter(fs, recoverable);
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

        revokeLeaseByFileSystem(fileSystem, path);

        // truncate back and append
        boolean truncated;
        try {
            truncated = truncate(fileSystem, path, recoverable.offset());
        } catch (Exception e) {
            throw new IOException("Problem while truncating file: " + path, e);
        }

        if (!truncated) {
            // Truncate did not complete immediately, we must wait for
            // the operation to complete and release the lease.
            revokeLeaseByFileSystem(fileSystem, path);
        }
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
     * @param path The path to the file we want to resume writing to.
     */
    private static boolean waitUntilLeaseIsRevoked(final FileSystem fs, final Path path)
            throws IOException {
        Preconditions.checkState(fs instanceof DistributedFileSystem);

        final DistributedFileSystem dfs = (DistributedFileSystem) fs;
        dfs.recoverLease(path);

        final Deadline deadline = Deadline.now().plus(Duration.ofMillis(LEASE_TIMEOUT));

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

    
	/*
	 * Run the dfs recover lease. recoverLease is asynchronous. It returns: -false when it starts the lease recovery (i.e. lease recovery not *yet* done) - true when the lease recovery has
	 * succeeded or the file is closed.
	 *
	 * But, we have to be careful. Each time we call recoverLease, it starts the recover lease process over from the beginning. We could put ourselves in a situation
	 * where we are doing nothing but starting a recovery, interrupting it to start again, and so on.
	 *
	 * The namenode will try to recover the lease on the file's primary node. If all is well, it should return near immediately.
	 * But, as is common, it is the very primary node that has crashed and so the namenode will be stuck waiting on a socket timeout before it will ask another datanode to start the recovery.
	 * It does not help if we call recoverLease in the meantime and in particular, subsequent to the socket timeout, a recoverLease invocation will cause us to start over from square one
	 * (possibly waiting on socket timeout against primary node).
	 * So, in the below, we do the following:
	 * 1. Call recoverLease.
	 * 2. If it returns true, break.
	 * 3. If it returns false, wait a few seconds and then call it again.
	 * 4. If it returns true, break.
	 * 5. If it returns false, wait for what we think the datanode socket timeout is (configurable) and then try again.
	 * 6. If it returns true, break.
	 * 7. If it returns false, repeat starting at step 5. above. If HDFS-4525 is available, call it every second and we might be able to exit early.
	 */
	private static boolean recoverLease(Path path, DistributedFileSystem dfs) throws IOException {
		LOG.info("Recover lease on dfs file " + path);
		long startWaiting = System.currentTimeMillis();
		// Default is 15 minutes. It's huge, but the idea is that if we have a major issue, HDFS
		// usually needs 10 minutes before marking the nodes as dead. So we're putting ourselves
		// beyond that limit 'to be safe'.
		//Configuration conf = dfs.getConf();
		long recoveryTimeout = HdfsConstants.LEASE_HARDLIMIT_PERIOD / 4;
		long recoveryTargetTimeout = recoveryTimeout + startWaiting;
		// This setting should be a little bit above what the cluster dfs heartbeat is set to.
		long firstPause = 4000L;
		long pause = 1000L;
		// This should be set to how long it'll take for us to timeout against primary datanode if it
		// is dead. We set it to 64 seconds, 4 second than the default READ_TIMEOUT in HDFS, the
		// default value for DFS_CLIENT_SOCKET_TIMEOUT_KEY. If recovery is still failing after this
		// timeout, then further recovery will take liner backoff with this base, to avoid endless
		// preemptions when this value is not properly configured.
		long subsequentPauseBase = HdfsConstants.LEASE_SOFTLIMIT_PERIOD;

		Method isFileClosedMeth = null;
		// whether we need to look for isFileClosed method
		boolean findIsFileClosedMeth = true;
		boolean recovered = false;
		// We break the loop if we succeed the lease recovery, timeout, or we throw an exception.
		for (int nbAttempt = 0; !recovered; nbAttempt++) {
			recovered = recoverLease(dfs, nbAttempt, path, startWaiting);
			if (recovered) {
				break;
			}
			if (recoveryTargetTimeout < System.currentTimeMillis()) {
				LOG.warn("Cannot recoverLease after trying for " +
					recoveryTimeout +
					"ms (hbase.lease.recovery.timeout); continuing, but may be DATALOSS!!!; " +
					String.format("attempt=%d, on file=%s, after %d ms", nbAttempt, path.toString(), System.currentTimeMillis() - startWaiting));
				break;
			}
			try {
				// On the first time through wait the short 'firstPause'.
				if (nbAttempt == 0) {
					Thread.sleep(firstPause);
				} else {
					// Cycle here until (subsequentPause * nbAttempt) elapses. While spinning, check
					// isFileClosed if available (should be in hadoop 2.0.5... not in hadoop 1 though.
					long localStartWaiting = System.currentTimeMillis();
					while ((System.currentTimeMillis() - localStartWaiting) < subsequentPauseBase *
						nbAttempt) {
						Thread.sleep(pause);
						if (findIsFileClosedMeth) {
							try {
								isFileClosedMeth =
									dfs.getClass().getMethod("isFileClosed", new Class[] { Path.class });
							} catch (NoSuchMethodException nsme) {
								LOG.debug("isFileClosed not available");
							} finally {
								findIsFileClosedMeth = false;
							}
						}
						if (isFileClosedMeth != null && dfs.isFileClosed(path)) {
							recovered = true;
							break;
						}
					}
				}
			} catch (InterruptedException ie) {
				InterruptedIOException iioe = new InterruptedIOException();
				iioe.initCause(ie);
				throw iioe;
			}
		}
		return recovered;
	}

	/**
	 * Try to recover the lease.
	 * @return True if dfs#recoverLease came by true.
	 */
	private static boolean recoverLease(final DistributedFileSystem dfs, final int nbAttempt,
										final Path path, final long startWaiting) throws FileNotFoundException {
		boolean recovered = false;
		try {
			recovered = dfs.recoverLease(path);
			LOG.info((recovered ? "Recovered lease, " : "Failed to recover lease, ") +
				String.format("attempt=%d, on file=%s, after %d ms", nbAttempt, path.toString(), System.currentTimeMillis() - startWaiting));
		} catch (IOException e) {
			if (e instanceof LeaseExpiredException && e.getMessage().contains("File does not exist")) {
				// This exception comes out instead of FNFE, fix it
				throw new FileNotFoundException("The given WAL wasn't found at " + path);
			} else if (e instanceof FileNotFoundException) {
				throw (FileNotFoundException) e;
			}
			LOG.warn(String.format("attempt=%d, on file=%s, after %d ms", nbAttempt, path.toString(), System.currentTimeMillis() - startWaiting), e);
		}
		return recovered;
	}

}
