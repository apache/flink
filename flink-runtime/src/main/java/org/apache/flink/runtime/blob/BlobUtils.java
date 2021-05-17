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

package org.apache.flink.runtime.blob;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;
import java.util.UUID;

/** Utility class to work with blob data. */
public class BlobUtils {

    /** Algorithm to be used for calculating the BLOB keys. */
    private static final String HASHING_ALGORITHM = "SHA-1";

    /** The prefix of all BLOB files stored by the BLOB server. */
    private static final String BLOB_FILE_PREFIX = "blob_";

    /** The prefix of all job-specific directories created by the BLOB server. */
    static final String JOB_DIR_PREFIX = "job_";

    /** The prefix of all job-unrelated directories created by the BLOB server. */
    static final String NO_JOB_DIR_PREFIX = "no_job";

    private static final Random RANDOM = new Random();

    /**
     * Creates a BlobStore based on the parameters set in the configuration.
     *
     * @param config configuration to use
     * @return a (distributed) blob store for high availability
     * @throws IOException thrown if the (distributed) file storage cannot be created
     */
    public static BlobStoreService createBlobStoreFromConfig(Configuration config)
            throws IOException {
        if (HighAvailabilityMode.isHighAvailabilityModeActivated(config)) {
            return createFileSystemBlobStore(config);
        } else {
            return new VoidBlobStore();
        }
    }

    private static BlobStoreService createFileSystemBlobStore(Configuration configuration)
            throws IOException {
        final Path clusterStoragePath =
                HighAvailabilityServicesUtils.getClusterHighAvailableStoragePath(configuration);

        final FileSystem fileSystem;
        try {
            fileSystem = clusterStoragePath.getFileSystem();
        } catch (Exception e) {
            throw new IOException(
                    String.format(
                            "Could not create FileSystem for highly available storage path (%s)",
                            clusterStoragePath),
                    e);
        }

        return new FileSystemBlobStore(fileSystem, clusterStoragePath.toUri().toString());
    }

    /**
     * Creates a local storage directory for a blob service under the configuration parameter given
     * by {@link BlobServerOptions#STORAGE_DIRECTORY}. If this is <tt>null</tt> or empty, we will
     * fall back to Flink's temp directories (given by {@link
     * org.apache.flink.configuration.CoreOptions#TMP_DIRS}) and choose one among them at random.
     *
     * @param config Flink configuration
     * @return a new local storage directory
     * @throws IOException thrown if the local file storage cannot be created or is not usable
     */
    static File initLocalStorageDirectory(Configuration config) throws IOException {

        String basePath = config.getString(BlobServerOptions.STORAGE_DIRECTORY);

        File baseDir;
        if (StringUtils.isNullOrWhitespaceOnly(basePath)) {
            final String[] tmpDirPaths = ConfigurationUtils.parseTempDirectories(config);
            baseDir = new File(tmpDirPaths[RANDOM.nextInt(tmpDirPaths.length)]);
        } else {
            baseDir = new File(basePath);
        }

        File storageDir;

        // NOTE: although we will be using UUIDs, there may be collisions
        int maxAttempts = 10;
        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            storageDir =
                    new File(baseDir, String.format("blobStore-%s", UUID.randomUUID().toString()));

            // Create the storage dir if it doesn't exist. Only return it when the operation was
            // successful.
            if (storageDir.mkdirs()) {
                return storageDir;
            }
        }

        // max attempts exceeded to find a storage directory
        throw new IOException(
                "Could not create storage directory for BLOB store in '" + baseDir + "'.");
    }

    /**
     * Returns the BLOB service's directory for incoming (job-unrelated) files. The directory is
     * created if it does not exist yet.
     *
     * @param storageDir storage directory used be the BLOB service
     * @return the BLOB service's directory for incoming files
     * @throws IOException if creating the directory fails
     */
    static File getIncomingDirectory(File storageDir) throws IOException {
        final File incomingDir = new File(storageDir, "incoming");

        Files.createDirectories(incomingDir.toPath());

        return incomingDir;
    }

    /**
     * Returns the (designated) physical storage location of the BLOB with the given key.
     *
     * @param storageDir storage directory used be the BLOB service
     * @param key the key identifying the BLOB
     * @param jobId ID of the job for the incoming files (or <tt>null</tt> if job-unrelated)
     * @return the (designated) physical storage location of the BLOB
     * @throws IOException if creating the directory fails
     */
    static File getStorageLocation(File storageDir, @Nullable JobID jobId, BlobKey key)
            throws IOException {
        File file = new File(getStorageLocationPath(storageDir.getAbsolutePath(), jobId, key));

        Files.createDirectories(file.getParentFile().toPath());

        return file;
    }

    /**
     * Returns the BLOB server's storage directory for BLOBs belonging to the job with the given ID
     * <em>without</em> creating the directory.
     *
     * @param storageDir storage directory used be the BLOB service
     * @param jobId the ID of the job to return the storage directory for
     * @return the storage directory for BLOBs belonging to the job with the given ID
     */
    static String getStorageLocationPath(String storageDir, @Nullable JobID jobId) {
        if (jobId == null) {
            // format: $base/no_job
            return String.format("%s/%s", storageDir, NO_JOB_DIR_PREFIX);
        } else {
            // format: $base/job_$jobId
            return String.format("%s/%s%s", storageDir, JOB_DIR_PREFIX, jobId.toString());
        }
    }

    /**
     * Returns the path for the given blob key.
     *
     * <p>The returned path can be used with the (local or HA) BLOB store file system back-end for
     * recovery purposes and follows the same scheme as {@link #getStorageLocation(File, JobID,
     * BlobKey)}.
     *
     * @param storageDir storage directory used be the BLOB service
     * @param key the key identifying the BLOB
     * @param jobId ID of the job for the incoming files
     * @return the path to the given BLOB
     */
    static String getStorageLocationPath(String storageDir, @Nullable JobID jobId, BlobKey key) {
        if (jobId == null) {
            // format: $base/no_job/blob_$key
            return String.format(
                    "%s/%s/%s%s", storageDir, NO_JOB_DIR_PREFIX, BLOB_FILE_PREFIX, key.toString());
        } else {
            // format: $base/job_$jobId/blob_$key
            return String.format(
                    "%s/%s%s/%s%s",
                    storageDir, JOB_DIR_PREFIX, jobId.toString(), BLOB_FILE_PREFIX, key.toString());
        }
    }

    /**
     * Creates a new instance of the message digest to use for the BLOB key computation.
     *
     * @return a new instance of the message digest to use for the BLOB key computation
     */
    static MessageDigest createMessageDigest() {
        try {
            return MessageDigest.getInstance(HASHING_ALGORITHM);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(
                    "Cannot instantiate the message digest algorithm " + HASHING_ALGORITHM, e);
        }
    }

    /**
     * Auxiliary method to write the length of an upcoming data chunk to an output stream.
     *
     * @param length the length of the upcoming data chunk in bytes
     * @param outputStream the output stream to write the length to
     * @throws IOException thrown if an I/O error occurs while writing to the output stream
     */
    static void writeLength(int length, OutputStream outputStream) throws IOException {
        byte[] buf = new byte[4];
        buf[0] = (byte) (length & 0xff);
        buf[1] = (byte) ((length >> 8) & 0xff);
        buf[2] = (byte) ((length >> 16) & 0xff);
        buf[3] = (byte) ((length >> 24) & 0xff);
        outputStream.write(buf, 0, 4);
    }

    /**
     * Auxiliary method to read the length of an upcoming data chunk from an input stream.
     *
     * @param inputStream the input stream to read the length from
     * @return the length of the upcoming data chunk in bytes
     * @throws IOException thrown if an I/O error occurs while reading from the input stream
     */
    static int readLength(InputStream inputStream) throws IOException {
        byte[] buf = new byte[4];
        int bytesRead = 0;
        while (bytesRead < 4) {
            final int read = inputStream.read(buf, bytesRead, 4 - bytesRead);
            if (read < 0) {
                throw new EOFException("Read an incomplete length");
            }
            bytesRead += read;
        }

        bytesRead = buf[0] & 0xff;
        bytesRead |= (buf[1] & 0xff) << 8;
        bytesRead |= (buf[2] & 0xff) << 16;
        bytesRead |= (buf[3] & 0xff) << 24;

        return bytesRead;
    }

    /**
     * Reads exception from given {@link InputStream}.
     *
     * @param in the input stream to read from
     * @return exception that was read
     * @throws IOException thrown if an I/O error occurs while reading from the input stream
     */
    static Throwable readExceptionFromStream(InputStream in) throws IOException {
        int len = readLength(in);
        byte[] bytes = new byte[len];
        readFully(in, bytes, 0, len, "Error message");

        try {
            return (Throwable)
                    InstantiationUtil.deserializeObject(bytes, ClassLoader.getSystemClassLoader());
        } catch (ClassNotFoundException e) {
            // should never occur
            throw new IOException("Could not transfer error message", e);
        }
    }

    /**
     * Auxiliary method to read a particular number of bytes from an input stream. This method
     * blocks until the requested number of bytes have been read from the stream. If the stream
     * cannot offer enough data, an {@link EOFException} is thrown.
     *
     * @param inputStream The input stream to read the data from.
     * @param buf The buffer to store the read data.
     * @param off The offset inside the buffer.
     * @param len The number of bytes to read from the stream.
     * @param type The name of the type, to throw a good error message in case of not enough data.
     * @throws IOException Thrown if I/O error occurs while reading from the stream or the stream
     *     cannot offer enough data.
     */
    static void readFully(InputStream inputStream, byte[] buf, int off, int len, String type)
            throws IOException {

        int bytesRead = 0;
        while (bytesRead < len) {

            final int read = inputStream.read(buf, off + bytesRead, len - bytesRead);
            if (read < 0) {
                throw new EOFException("Received an incomplete " + type);
            }
            bytesRead += read;
        }
    }

    static void closeSilently(Socket socket, Logger log) {
        if (socket != null) {
            try {
                socket.close();
            } catch (Throwable t) {
                log.debug("Exception while closing BLOB server connection socket.", t);
            }
        }
    }

    /** Private constructor to prevent instantiation. */
    private BlobUtils() {
        throw new RuntimeException();
    }

    /**
     * Moves the temporary <tt>incomingFile</tt> to its permanent location where it is available for
     * use (not thread-safe!).
     *
     * @param incomingFile temporary file created during transfer
     * @param jobId ID of the job this blob belongs to or <tt>null</tt> if job-unrelated
     * @param blobKey BLOB key identifying the file
     * @param storageFile (local) file where the blob is/should be stored
     * @param log logger for debug information
     * @param blobStore HA store (or <tt>null</tt> if unavailable)
     * @throws IOException thrown if an I/O error occurs while moving the file or uploading it to
     *     the HA store
     */
    static void moveTempFileToStore(
            File incomingFile,
            @Nullable JobID jobId,
            BlobKey blobKey,
            File storageFile,
            Logger log,
            @Nullable BlobStore blobStore)
            throws IOException {

        try {
            // first check whether the file already exists
            if (!storageFile.exists()) {
                try {
                    // only move the file if it does not yet exist
                    Files.move(incomingFile.toPath(), storageFile.toPath());

                    incomingFile = null;

                } catch (FileAlreadyExistsException ignored) {
                    log.warn(
                            "Detected concurrent file modifications. This should only happen if multiple"
                                    + "BlobServer use the same storage directory.");
                    // we cannot be sure at this point whether the file has already been uploaded to
                    // the blob
                    // store or not. Even if the blobStore might shortly be in an inconsistent
                    // state, we have
                    // to persist the blob. Otherwise we might not be able to recover the job.
                }

                if (blobStore != null) {
                    // only the one moving the incoming file to its final destination is allowed to
                    // upload the
                    // file to the blob store
                    blobStore.put(storageFile, jobId, blobKey);
                }
            } else {
                log.warn(
                        "File upload for an existing file with key {} for job {}. This may indicate a duplicate upload or a hash collision. Ignoring newest upload.",
                        blobKey,
                        jobId);
            }
            storageFile = null;
        } finally {
            // we failed to either create the local storage file or to upload it --> try to delete
            // the local file
            // while still having the write lock
            if (storageFile != null && !storageFile.delete() && storageFile.exists()) {
                log.warn("Could not delete the storage file {}.", storageFile);
            }
            if (incomingFile != null && !incomingFile.delete() && incomingFile.exists()) {
                log.warn(
                        "Could not delete the staging file {} for blob key {} and job {}.",
                        incomingFile,
                        blobKey,
                        jobId);
            }
        }
    }
}
