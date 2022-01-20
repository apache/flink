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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Reference;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

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
     * Creates the {@link BlobServer} from the given configuration, fallback storage directory and
     * blob store.
     *
     * @param configuration for the BlobServer
     * @param fallbackStorageDirectory fallback storage directory that is used if no other directory
     *     has been explicitly configured
     * @param blobStore blob store to use for this blob server
     * @return new blob server instance
     * @throws IOException if we could not create the blob storage directory
     */
    public static BlobServer createBlobServer(
            Configuration configuration,
            Reference<File> fallbackStorageDirectory,
            BlobStore blobStore)
            throws IOException {
        final Reference<File> storageDirectory =
                createBlobStorageDirectory(configuration, fallbackStorageDirectory);
        return new BlobServer(configuration, storageDirectory, blobStore);
    }

    /**
     * Creates the {@link BlobCacheService} from the given configuration, fallback storage
     * directory, blob view and blob server address.
     *
     * @param configuration for the BlobCacheService
     * @param fallbackStorageDirectory fallback storage directory
     * @param blobView blob view
     * @param serverAddress blob server address
     * @return new blob cache service instance
     * @throws IOException if we could not create the blob storage directory
     */
    public static BlobCacheService createBlobCacheService(
            Configuration configuration,
            Reference<File> fallbackStorageDirectory,
            BlobView blobView,
            @Nullable InetSocketAddress serverAddress)
            throws IOException {
        final Reference<File> storageDirectory =
                createBlobStorageDirectory(configuration, fallbackStorageDirectory);
        return new BlobCacheService(configuration, storageDirectory, blobView, serverAddress);
    }

    static Reference<File> createBlobStorageDirectory(
            Configuration configuration, @Nullable Reference<File> fallbackStorageDirectory)
            throws IOException {
        final String basePath = configuration.getString(BlobServerOptions.STORAGE_DIRECTORY);

        File baseDir = null;
        if (StringUtils.isNullOrWhitespaceOnly(basePath)) {
            if (fallbackStorageDirectory != null) {
                baseDir = fallbackStorageDirectory.deref();

                if (baseDir.mkdirs() || baseDir.exists()) {
                    return fallbackStorageDirectory;
                }
            }
        } else {
            baseDir = new File(basePath);

            File storageDir;

            // NOTE: although we will be using UUIDs, there may be collisions
            int maxAttempts = 10;
            for (int attempt = 0; attempt < maxAttempts; attempt++) {
                storageDir = new File(baseDir, String.format("blobStore-%s", UUID.randomUUID()));

                // Create the storage dir if it doesn't exist. Only return it when the operation was
                // successful.
                if (storageDir.mkdirs()) {
                    return Reference.owned(storageDir);
                }
            }
        }

        if (baseDir != null) {
            throw new IOException(
                    "Could not create storage directory for BLOB store in '" + baseDir + "'.");
        } else {
            throw new IOException(
                    String.format(
                            "Could not create storage directory for BLOB store because no storage directory has "
                                    + "been specified under %s and no fallback storage directory provided.",
                            BlobServerOptions.STORAGE_DIRECTORY.key()));
        }
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
        internalMoveTempFileToStore(
                incomingFile,
                jobId,
                blobKey,
                storageFile,
                log,
                blobStore,
                (source, target) -> Files.move(source.toPath(), target.toPath()));
    }

    @VisibleForTesting
    static void internalMoveTempFileToStore(
            File incomingFile,
            @Nullable JobID jobId,
            BlobKey blobKey,
            File storageFile,
            Logger log,
            @Nullable BlobStore blobStore,
            MoveFileOperation moveFileOperation)
            throws IOException {

        boolean success = false;
        try {
            // first check whether the file already exists
            if (!storageFile.exists()) {
                // persist the blob via the blob store
                if (blobStore != null) {
                    blobStore.put(incomingFile, jobId, blobKey);
                }

                try {
                    moveFileOperation.moveFile(incomingFile, storageFile);
                    incomingFile = null;

                } catch (FileAlreadyExistsException ignored) {
                    log.warn(
                            "Detected concurrent file modifications. This should only happen if multiple"
                                    + "BlobServer use the same storage directory.");
                }
            } else {
                log.warn(
                        "File upload for an existing file with key {} for job {}. This may indicate a duplicate upload or a hash collision. Ignoring newest upload.",
                        blobKey,
                        jobId);
            }
            success = true;
        } finally {
            if (!success) {
                if (blobStore != null) {
                    blobStore.delete(jobId, blobKey);
                }

                if (!storageFile.delete() && storageFile.exists()) {
                    log.warn("Could not delete the storage file {}.", storageFile);
                }
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

    interface MoveFileOperation {
        void moveFile(File source, File target) throws IOException;
    }

    public static byte[] calculateMessageDigest(File file) throws IOException {
        final MessageDigest messageDigest = createMessageDigest();

        final byte[] buffer = new byte[4096];

        try (final FileInputStream fis = new FileInputStream(file)) {
            while (true) {
                final int bytesRead = fis.read(buffer, 0, buffer.length);

                if (bytesRead == -1) {
                    break;
                }

                messageDigest.update(buffer, 0, bytesRead);
            }
        }

        return messageDigest.digest();
    }

    static void checkAndDeleteCorruptedBlobs(java.nio.file.Path storageDir, Logger log)
            throws IOException {
        for (Blob blob : listBlobsInDirectory(storageDir)) {
            final BlobKey blobKey = blob.getBlobKey();
            final java.nio.file.Path blobPath = blob.getPath();

            final byte[] messageDigest = calculateMessageDigest(blobPath.toFile());

            if (!Arrays.equals(blobKey.getHash(), messageDigest)) {
                log.info(
                        "Found corrupted blob {} under {}. Deleting this blob.", blobKey, blobPath);

                try {
                    FileUtils.deleteFileOrDirectory(blobPath.toFile());
                } catch (IOException ioe) {
                    log.debug("Could not delete the blob {}.", blobPath, ioe);
                }
            }
        }
    }

    @Nonnull
    static Collection<java.nio.file.Path> listBlobFilesInDirectory(java.nio.file.Path directory)
            throws IOException {
        return FileUtils.listFilesInDirectory(
                directory, file -> file.getFileName().toString().startsWith(BLOB_FILE_PREFIX));
    }

    @Nonnull
    static Collection<Blob<?>> listBlobsInDirectory(java.nio.file.Path directory)
            throws IOException {
        return listBlobFilesInDirectory(directory).stream()
                .map(
                        blobPath -> {
                            final BlobKey blobKey =
                                    BlobKey.fromString(
                                            blobPath.getFileName()
                                                    .toString()
                                                    .substring(BLOB_FILE_PREFIX.length()));
                            final String jobDirectory =
                                    blobPath.getParent().getFileName().toString();

                            @Nullable final JobID jobId;

                            if (jobDirectory.equals(NO_JOB_DIR_PREFIX)) {
                                jobId = null;
                            } else if (jobDirectory.startsWith(JOB_DIR_PREFIX)) {
                                jobId =
                                        new JobID(
                                                StringUtils.hexStringToByte(
                                                        jobDirectory.substring(
                                                                JOB_DIR_PREFIX.length())));
                            } else {
                                throw new IllegalStateException(
                                        String.format("Unknown job path %s.", jobDirectory));
                            }

                            if (blobKey instanceof TransientBlobKey) {
                                return new TransientBlob(
                                        (TransientBlobKey) blobKey, blobPath, jobId);
                            } else if (blobKey instanceof PermanentBlobKey) {
                                return new PermanentBlob(
                                        (PermanentBlobKey) blobKey, blobPath, jobId);
                            } else {
                                throw new IllegalStateException(
                                        String.format(
                                                "Unknown blob key format %s.", blobKey.getClass()));
                            }
                        })
                .collect(Collectors.toList());
    }

    @Nonnull
    static Collection<TransientBlob> listTransientBlobsInDirectory(java.nio.file.Path directory)
            throws IOException {
        return listBlobsInDirectory(directory).stream()
                .filter(blob -> blob.getBlobKey() instanceof TransientBlobKey)
                .map(blob -> (TransientBlob) blob)
                .collect(Collectors.toList());
    }

    @Nonnull
    static Collection<PermanentBlob> listPermanentBlobsInDirectory(java.nio.file.Path directory)
            throws IOException {
        return listBlobsInDirectory(directory).stream()
                .filter(blob -> blob.getBlobKey() instanceof PermanentBlobKey)
                .map(blob -> (PermanentBlob) blob)
                .collect(Collectors.toList());
    }

    static Set<JobID> listExistingJobs(java.nio.file.Path directory) throws IOException {
        return listBlobsInDirectory(directory).stream()
                .map(Blob::getJobId)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }

    abstract static class Blob<T extends BlobKey> {
        private final T blobKey;
        private final java.nio.file.Path path;
        @Nullable private final JobID jobId;

        Blob(T blobKey, java.nio.file.Path path, @Nullable JobID jobId) {
            this.blobKey = blobKey;
            this.path = path;
            this.jobId = jobId;
        }

        public T getBlobKey() {
            return blobKey;
        }

        public java.nio.file.Path getPath() {
            return path;
        }

        @Nullable
        public JobID getJobId() {
            return jobId;
        }
    }

    static final class TransientBlob extends Blob<TransientBlobKey> {
        TransientBlob(TransientBlobKey blobKey, java.nio.file.Path path, @Nullable JobID jobId) {
            super(blobKey, path, jobId);
        }
    }

    static final class PermanentBlob extends Blob<PermanentBlobKey> {
        PermanentBlob(PermanentBlobKey blobKey, java.nio.file.Path path, @Nullable JobID jobId) {
            super(blobKey, path, jobId);
        }
    }
}
