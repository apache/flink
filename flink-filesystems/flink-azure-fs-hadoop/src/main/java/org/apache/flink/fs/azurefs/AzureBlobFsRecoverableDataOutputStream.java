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

package org.apache.flink.fs.azurefs;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.runtime.fs.hdfs.BaseHadoopFsRecoverableFsDataOutputStream;
import org.apache.flink.runtime.fs.hdfs.HadoopFsRecoverable;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of the {@link RecoverableFsDataOutputStream} for AzureBlob's file system
 * abstraction.
 */
@Internal
public class AzureBlobFsRecoverableDataOutputStream
        extends BaseHadoopFsRecoverableFsDataOutputStream {

    private static final Logger LOG =
            LoggerFactory.getLogger(AzureBlobFsRecoverableDataOutputStream.class);
    private static final String RENAME = ".rename";

    // Not final to override in tests
    @VisibleForTesting static int minBufferLength = 2097152;

    AzureBlobFsRecoverableDataOutputStream(FileSystem fs, Path targetFile, Path tempFile)
            throws IOException {
        this.fs = checkNotNull(fs);
        this.targetFile = checkNotNull(targetFile);
        LOG.debug("The targetFile is {}", targetFile.getName());
        this.tempFile = checkNotNull(tempFile);
        LOG.debug("The tempFile is {}", tempFile.getName());
        this.out = fs.create(tempFile);
    }

    AzureBlobFsRecoverableDataOutputStream(FileSystem fs, HadoopFsRecoverable recoverable)
            throws IOException {
        this.fs = checkNotNull(fs);
        this.targetFile = checkNotNull(recoverable.targetFile());
        this.tempFile = checkNotNull(recoverable.tempFile());
        if (!fs.exists(tempFile)) {
            LOG.error("The temp file is not found {}", tempFile);
            // trying the temp rename file.
            Path renameTempPath = new Path(tempFile.toString() + RENAME);
            if (fs.exists(renameTempPath)) {
                LOG.info(
                        "Found the rename file. Probably a case where the rename did not happen {}",
                        renameTempPath);
                if (fs.getFileStatus(renameTempPath).getLen() == recoverable.offset()) {
                    rename(fs, renameTempPath);
                } else {
                    LOG.error(
                            "Unrecoverable error. As the required {} file is not found", tempFile);
                    throw new IOException(
                            "Unable to recover the job as the expected "
                                    + tempFile
                                    + " file is not found");
                }
            } else {
                LOG.error("Unrecoverable error. As the required {} file is not found", tempFile);
                throw new IOException(
                        "Unable to recover the job as the expected "
                                + tempFile
                                + " file is not found");
            }
        } else {
            long len = fs.getFileStatus(tempFile).getLen();
            LOG.info(
                    "The recoverable offset is {} and the file len is {}",
                    recoverable.offset(),
                    len);
            // Happens when we recover from a previously committed offset. Otherwise this is not
            // really needed
            if (len > recoverable.offset()) {
                truncate(fs, recoverable);
            } else if (len < recoverable.offset()) {
                LOG.error(
                        "Temp file length {} is less than the expected recoverable offset {}",
                        len,
                        recoverable.offset());
                throw new IOException(
                        "Unable to create recoverable outputstream as length of file "
                                + len
                                + " is less than "
                                + "recoverable offset "
                                + recoverable.offset());
            }
        }
        out = fs.append(tempFile);
        if (out.getPos() == 0) {
            // In ABFS when we try to append we don't account for the initial file size like we do
            // in DFS.
            // So we explicitly store this and when we do a persist call we make use of it.
            // This we have raised a bug in ABFS hadoop driver side. Once fixed this will not be
            // needed. So it should be ok to put this in side the 'if' check.
            initialFileSize = fs.getFileStatus(tempFile).getLen();
        }
        LOG.debug("Created a new OS for appending {}", tempFile);
    }

    private void truncate(FileSystem fs, HadoopFsRecoverable recoverable) throws IOException {
        Path renameTempPath = new Path(tempFile.toString() + RENAME);
        try {
            LOG.info(
                    "Creating the temp rename file {} for truncating the tempFile {}",
                    renameTempPath,
                    tempFile);
            FSDataOutputStream fsDataOutputStream = fs.create(renameTempPath);
            LOG.info("Opening the tempFile {} for truncate", tempFile);
            FSDataInputStream fsDis = fs.open(tempFile);
            // 2 MB buffers. TODO : Make this configurable
            long remaining = recoverable.offset();
            byte[] buf = null;
            long dataWritten = 0;
            int readBytes = -1;
            while (remaining != 0) {
                if (minBufferLength < remaining) {
                    buf = new byte[minBufferLength];
                } else {
                    buf = new byte[(int) remaining];
                }
                readBytes = fsDis.read(buf, 0, buf.length);
                if (readBytes != -1) {
                    remaining -= readBytes;
                    LOG.info("Bytes remaining to read {}", remaining);
                    fsDataOutputStream.write(buf, 0, readBytes);
                    dataWritten += readBytes;
                    LOG.info("Successfully wrote {} bytes of data", dataWritten);
                } else {
                    LOG.debug("Reached the end of the file");
                    remaining = 0;
                }
            }
            // TODO : Support intermediate flush?
            LOG.info("Closing the temp rename file {}", renameTempPath);
            fsDataOutputStream.close();
            if (fsDis != null) {
                LOG.debug("Closing the input stream");
                fsDis.close();
            }
        } catch (IOException e) {
            LOG.error(
                    "Unable to recover. Exception while trying to truncate the temp file {}",
                    tempFile);
            // We cannot recover. This we can control if user does not want this??
            throw e;
        }
        try {
            LOG.info("Deleting the actual temp file {}", tempFile);
            fs.delete(tempFile, false);
        } catch (IOException e) {
            LOG.error("Unable to recover. Error while deleting the temp file {}", tempFile);
            // unable to recover.
            throw e;
        }
        rename(fs, renameTempPath);
    }

    private void rename(FileSystem fs, Path renameTempPath) throws IOException {
        LOG.info("Renaming the temp rename file {} back to tempFile {}", renameTempPath, tempFile);
        try {
            // Rename by default will overwrite if dest is already found.
            boolean result = fs.rename(renameTempPath, tempFile);
            if (!result) {
                LOG.error(
                        "Unable to recover. Rename operation failed {} to {}",
                        renameTempPath,
                        tempFile);
                throw new IOException("Unable to recover. Rename operation failed");
            } else {
                LOG.info("Rename was successful");
            }
        } catch (IOException e) {
            LOG.error(
                    "Unable to recover. Renaming of tempFile did not happen after truncating {} to {}",
                    renameTempPath,
                    tempFile);
            throw e;
        }
    }

    @Override
    public void sync() throws IOException {
        out.hsync();
    }

    @Override
    public Committer closeForCommit() throws IOException {
        final long pos = getPos();
        close();
        return new AzureBlobFsRecoverableDataOutputStream.ABFSCommitter(
                fs, createHadoopFsRecoverable(pos));
    }

    // ------------------------------------------------------------------------
    //  Committer
    // ------------------------------------------------------------------------

    /**
     * Implementation of a committer for the Hadoop File System abstraction. This implementation
     * commits by renaming the temp file to the final file path. The temp file is truncated before
     * renaming in case there is trailing garbage data.
     */
    static class ABFSCommitter implements Committer {

        private final FileSystem fs;
        private final HadoopFsRecoverable recoverable;

        ABFSCommitter(FileSystem fs, HadoopFsRecoverable recoverable) {
            this.fs = checkNotNull(fs);
            this.recoverable = checkNotNull(recoverable);
        }

        @Override
        public void commit() throws IOException {
            final Path src = recoverable.tempFile();
            final Path dest = recoverable.targetFile();
            final long expectedLength = recoverable.offset();
            FileStatus srcStatus = null;
            try {
                srcStatus = fs.getFileStatus(src);
            } catch (FileNotFoundException fnfe) {
                // srcStatus will be null
            } catch (IOException e) {
                throw new IOException("Cannot clean commit: Staging file does not exist.");
            }
            if (srcStatus != null) {
                LOG.debug(
                        "The srcStatus is {} and exp length is {}",
                        srcStatus.getLen(),
                        expectedLength);
                if (srcStatus.getLen() != expectedLength) {
                    LOG.error(
                            "The src file {} with length {} does not match the expected length {}",
                            src,
                            srcStatus.getLen(),
                            expectedLength);
                    throw new IOException(
                            "The src file "
                                    + src
                                    + " with length "
                                    + srcStatus.getLen()
                                    + " "
                                    + "does not match the expected length "
                                    + expectedLength);
                }
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
                throw new IOException(
                        "Unrecoverable exception while trying to recover "
                                + recoverable.tempFile());
            }
        }

        @Override
        public void commitAfterRecovery() throws IOException {
            commit();
        }

        @Override
        public RecoverableWriter.CommitRecoverable getRecoverable() {
            return recoverable;
        }
    }
}
