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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.net.SSLUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.ShutdownHookUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.net.ServerSocketFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.flink.runtime.blob.BlobKey.BlobType.PERMANENT_BLOB;
import static org.apache.flink.runtime.blob.BlobKey.BlobType.TRANSIENT_BLOB;
import static org.apache.flink.runtime.blob.BlobServerProtocol.BUFFER_SIZE;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class implements the BLOB server. The BLOB server is responsible for listening for incoming
 * requests and spawning threads to handle these requests. Furthermore, it takes care of creating
 * the directory structure to store the BLOBs or temporarily cache them.
 */
public class BlobServer extends Thread
        implements BlobService, BlobWriter, PermanentBlobService, TransientBlobService {

    /** The log object used for debugging. */
    private static final Logger LOG = LoggerFactory.getLogger(BlobServer.class);

    /** Counter to generate unique names for temporary files. */
    private final AtomicLong tempFileCounter = new AtomicLong(0);

    /** The server socket listening for incoming connections. */
    private final ServerSocket serverSocket;

    /** Blob Server configuration. */
    private final Configuration blobServiceConfiguration;

    /** Indicates whether a shutdown of server component has been requested. */
    private final AtomicBoolean shutdownRequested = new AtomicBoolean();

    /** Root directory for local file storage. */
    private final File storageDir;

    /** Blob store for distributed file storage, e.g. in HA. */
    private final BlobStore blobStore;

    /** Set of currently running threads. */
    private final Set<BlobServerConnection> activeConnections = new HashSet<>();

    /** The maximum number of concurrent connections. */
    private final int maxConnections;

    /** Lock guarding concurrent file accesses. */
    private final ReadWriteLock readWriteLock;

    /** Shutdown hook thread to ensure deletion of the local storage directory. */
    private final Thread shutdownHook;

    // --------------------------------------------------------------------------------------------

    /**
     * Map to store the TTL of each element stored in the local storage, i.e. via one of the {@link
     * #getFile} methods.
     */
    private final ConcurrentHashMap<Tuple2<JobID, TransientBlobKey>, Long> blobExpiryTimes =
            new ConcurrentHashMap<>();

    /** Time interval (ms) to run the cleanup task; also used as the default TTL. */
    private final long cleanupInterval;

    /** Timer task to execute the cleanup at regular intervals. */
    private final Timer cleanupTimer;

    /**
     * Instantiates a new BLOB server and binds it to a free network port.
     *
     * @param config Configuration to be used to instantiate the BlobServer
     * @param blobStore BlobStore to store blobs persistently
     * @throws IOException thrown if the BLOB server cannot bind to a free network port or if the
     *     (local or distributed) file storage cannot be created or is not usable
     */
    public BlobServer(Configuration config, BlobStore blobStore) throws IOException {
        this.blobServiceConfiguration = checkNotNull(config);
        this.blobStore = checkNotNull(blobStore);
        this.readWriteLock = new ReentrantReadWriteLock();

        // configure and create the storage directory
        this.storageDir = BlobUtils.initLocalStorageDirectory(config);
        LOG.info("Created BLOB server storage directory {}", storageDir);

        // configure the maximum number of concurrent connections
        final int maxConnections = config.getInteger(BlobServerOptions.FETCH_CONCURRENT);
        if (maxConnections >= 1) {
            this.maxConnections = maxConnections;
        } else {
            LOG.warn(
                    "Invalid value for maximum connections in BLOB server: {}. Using default value of {}",
                    maxConnections,
                    BlobServerOptions.FETCH_CONCURRENT.defaultValue());
            this.maxConnections = BlobServerOptions.FETCH_CONCURRENT.defaultValue();
        }

        // configure the backlog of connections
        int backlog = config.getInteger(BlobServerOptions.FETCH_BACKLOG);
        if (backlog < 1) {
            LOG.warn(
                    "Invalid value for BLOB connection backlog: {}. Using default value of {}",
                    backlog,
                    BlobServerOptions.FETCH_BACKLOG.defaultValue());
            backlog = BlobServerOptions.FETCH_BACKLOG.defaultValue();
        }

        // Initializing the clean up task
        this.cleanupTimer = new Timer(true);

        this.cleanupInterval = config.getLong(BlobServerOptions.CLEANUP_INTERVAL) * 1000;
        this.cleanupTimer.schedule(
                new TransientBlobCleanupTask(
                        blobExpiryTimes, readWriteLock.writeLock(), storageDir, LOG),
                cleanupInterval,
                cleanupInterval);

        this.shutdownHook = ShutdownHookUtil.addShutdownHook(this, getClass().getSimpleName(), LOG);

        //  ----------------------- start the server -------------------

        final String serverPortRange = config.getString(BlobServerOptions.PORT);
        final Iterator<Integer> ports = NetUtils.getPortRangeFromString(serverPortRange);

        final ServerSocketFactory socketFactory;
        if (SecurityOptions.isInternalSSLEnabled(config)
                && config.getBoolean(BlobServerOptions.SSL_ENABLED)) {
            try {
                socketFactory = SSLUtils.createSSLServerSocketFactory(config);
            } catch (Exception e) {
                throw new IOException("Failed to initialize SSL for the blob server", e);
            }
        } else {
            socketFactory = ServerSocketFactory.getDefault();
        }

        final int finalBacklog = backlog;
        final String bindHost =
                config.getOptional(JobManagerOptions.BIND_HOST)
                        .orElseGet(NetUtils::getWildcardIPAddress);

        this.serverSocket =
                NetUtils.createSocketFromPorts(
                        ports,
                        (port) ->
                                socketFactory.createServerSocket(
                                        port, finalBacklog, InetAddress.getByName(bindHost)));

        if (serverSocket == null) {
            throw new IOException(
                    "Unable to open BLOB Server in specified port range: " + serverPortRange);
        }

        // start the server thread
        setName("BLOB Server listener at " + getPort());
        setDaemon(true);

        if (LOG.isInfoEnabled()) {
            LOG.info(
                    "Started BLOB server at {}:{} - max concurrent requests: {} - max backlog: {}",
                    serverSocket.getInetAddress().getHostAddress(),
                    getPort(),
                    maxConnections,
                    backlog);
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Path Accessors
    // --------------------------------------------------------------------------------------------

    public File getStorageDir() {
        return storageDir;
    }

    /**
     * Returns a file handle to the file associated with the given blob key on the blob server.
     *
     * <p><strong>This is only called from {@link BlobServerConnection} or unit tests.</strong>
     *
     * @param jobId ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
     * @param key identifying the file
     * @return file handle to the file
     * @throws IOException if creating the directory fails
     */
    @VisibleForTesting
    public File getStorageLocation(@Nullable JobID jobId, BlobKey key) throws IOException {
        return BlobUtils.getStorageLocation(storageDir, jobId, key);
    }

    /**
     * Returns a temporary file inside the BLOB server's incoming directory.
     *
     * @return a temporary file inside the BLOB server's incoming directory
     * @throws IOException if creating the directory fails
     */
    File createTemporaryFilename() throws IOException {
        return new File(
                BlobUtils.getIncomingDirectory(storageDir),
                String.format("temp-%08d", tempFileCounter.getAndIncrement()));
    }

    /** Returns the lock used to guard file accesses. */
    ReadWriteLock getReadWriteLock() {
        return readWriteLock;
    }

    @Override
    public void run() {
        try {
            while (!this.shutdownRequested.get()) {
                BlobServerConnection conn =
                        new BlobServerConnection(NetUtils.acceptWithoutTimeout(serverSocket), this);
                try {
                    synchronized (activeConnections) {
                        while (activeConnections.size() >= maxConnections) {
                            activeConnections.wait(2000);
                        }
                        activeConnections.add(conn);
                    }

                    conn.start();
                    conn = null;
                } finally {
                    if (conn != null) {
                        conn.close();
                        synchronized (activeConnections) {
                            activeConnections.remove(conn);
                        }
                    }
                }
            }
        } catch (Throwable t) {
            if (!this.shutdownRequested.get()) {
                LOG.error("BLOB server stopped working. Shutting down", t);

                try {
                    close();
                } catch (Throwable closeThrowable) {
                    LOG.error("Could not properly close the BlobServer.", closeThrowable);
                }
            }
        }
    }

    /** Shuts down the BLOB server. */
    @Override
    public void close() throws IOException {
        cleanupTimer.cancel();

        if (shutdownRequested.compareAndSet(false, true)) {
            Exception exception = null;

            try {
                this.serverSocket.close();
            } catch (IOException ioe) {
                exception = ioe;
            }

            // wake the thread up, in case it is waiting on some operation
            interrupt();

            try {
                join();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();

                LOG.debug("Error while waiting for this thread to die.", ie);
            }

            synchronized (activeConnections) {
                if (!activeConnections.isEmpty()) {
                    for (BlobServerConnection conn : activeConnections) {
                        LOG.debug("Shutting down connection {}.", conn.getName());
                        conn.close();
                    }
                    activeConnections.clear();
                }
            }

            // Clean up the storage directory
            try {
                FileUtils.deleteDirectory(storageDir);
            } catch (IOException e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }

            // Remove shutdown hook to prevent resource leaks
            ShutdownHookUtil.removeShutdownHook(shutdownHook, getClass().getSimpleName(), LOG);

            if (LOG.isInfoEnabled()) {
                LOG.info(
                        "Stopped BLOB server at {}:{}",
                        serverSocket.getInetAddress().getHostAddress(),
                        getPort());
            }

            ExceptionUtils.tryRethrowIOException(exception);
        }
    }

    protected BlobClient createClient() throws IOException {
        return new BlobClient(
                new InetSocketAddress(serverSocket.getInetAddress(), getPort()),
                blobServiceConfiguration);
    }

    /**
     * Retrieves the local path of a (job-unrelated) file associated with a job and a blob key.
     *
     * <p>The blob server looks the blob key up in its local storage. If the file exists, it is
     * returned. If the file does not exist, it is retrieved from the HA blob store (if available)
     * or a {@link FileNotFoundException} is thrown.
     *
     * @param key blob key associated with the requested file
     * @return file referring to the local storage location of the BLOB
     * @throws IOException Thrown if the file retrieval failed.
     */
    @Override
    public File getFile(TransientBlobKey key) throws IOException {
        return getFileInternal(null, key);
    }

    /**
     * Retrieves the local path of a file associated with a job and a blob key.
     *
     * <p>The blob server looks the blob key up in its local storage. If the file exists, it is
     * returned. If the file does not exist, it is retrieved from the HA blob store (if available)
     * or a {@link FileNotFoundException} is thrown.
     *
     * @param jobId ID of the job this blob belongs to
     * @param key blob key associated with the requested file
     * @return file referring to the local storage location of the BLOB
     * @throws IOException Thrown if the file retrieval failed.
     */
    @Override
    public File getFile(JobID jobId, TransientBlobKey key) throws IOException {
        checkNotNull(jobId);
        return getFileInternal(jobId, key);
    }

    /**
     * Returns the path to a local copy of the file associated with the provided job ID and blob
     * key.
     *
     * <p>We will first attempt to serve the BLOB from the local storage. If the BLOB is not in
     * there, we will try to download it from the HA store.
     *
     * @param jobId ID of the job this blob belongs to
     * @param key blob key associated with the requested file
     * @return The path to the file.
     * @throws java.io.FileNotFoundException if the BLOB does not exist;
     * @throws IOException if any other error occurs when retrieving the file
     */
    @Override
    public File getFile(JobID jobId, PermanentBlobKey key) throws IOException {
        checkNotNull(jobId);
        return getFileInternal(jobId, key);
    }

    /**
     * Retrieves the local path of a file associated with a job and a blob key.
     *
     * <p>The blob server looks the blob key up in its local storage. If the file exists, it is
     * returned. If the file does not exist, it is retrieved from the HA blob store (if available)
     * or a {@link FileNotFoundException} is thrown.
     *
     * @param jobId ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
     * @param blobKey blob key associated with the requested file
     * @return file referring to the local storage location of the BLOB
     * @throws IOException Thrown if the file retrieval failed.
     */
    private File getFileInternal(@Nullable JobID jobId, BlobKey blobKey) throws IOException {
        checkArgument(blobKey != null, "BLOB key cannot be null.");

        final File localFile = BlobUtils.getStorageLocation(storageDir, jobId, blobKey);
        readWriteLock.readLock().lock();

        try {
            getFileInternal(jobId, blobKey, localFile);
            return localFile;
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    /**
     * Helper to retrieve the local path of a file associated with a job and a blob key.
     *
     * <p>The blob server looks the blob key up in its local storage. If the file exists, it is
     * returned. If the file does not exist, it is retrieved from the HA blob store (if available)
     * or a {@link FileNotFoundException} is thrown.
     *
     * <p><strong>Assumes the read lock has already been acquired.</strong>
     *
     * @param jobId ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
     * @param blobKey blob key associated with the requested file
     * @param localFile (local) file where the blob is/should be stored
     * @throws IOException Thrown if the file retrieval failed.
     */
    void getFileInternal(@Nullable JobID jobId, BlobKey blobKey, File localFile)
            throws IOException {
        // assume readWriteLock.readLock() was already locked (cannot really check that)

        if (localFile.exists()) {
            // update TTL for transient BLOBs:
            if (blobKey instanceof TransientBlobKey) {
                // regarding concurrent operations, it is not really important which timestamp makes
                // it into the map as they are close to each other anyway, also we can simply
                // overwrite old values as long as we are in the read (or write) lock
                blobExpiryTimes.put(
                        Tuple2.of(jobId, (TransientBlobKey) blobKey),
                        System.currentTimeMillis() + cleanupInterval);
            }
            return;
        } else if (blobKey instanceof PermanentBlobKey) {
            // Try the HA blob store
            // first we have to release the read lock in order to acquire the write lock
            readWriteLock.readLock().unlock();

            // use a temporary file (thread-safe without locking)
            File incomingFile = null;
            try {
                incomingFile = createTemporaryFilename();
                blobStore.get(jobId, blobKey, incomingFile);

                readWriteLock.writeLock().lock();
                try {
                    BlobUtils.moveTempFileToStore(
                            incomingFile, jobId, blobKey, localFile, LOG, null);
                } finally {
                    readWriteLock.writeLock().unlock();
                }

                return;
            } finally {
                // delete incomingFile from a failed download
                if (incomingFile != null && !incomingFile.delete() && incomingFile.exists()) {
                    LOG.warn(
                            "Could not delete the staging file {} for blob key {} and job {}.",
                            incomingFile,
                            blobKey,
                            jobId);
                }

                // re-acquire lock so that it can be unlocked again outside
                readWriteLock.readLock().lock();
            }
        }

        throw new FileNotFoundException(
                "Local file "
                        + localFile
                        + " does not exist "
                        + "and failed to copy from blob store.");
    }

    @Override
    public TransientBlobKey putTransient(byte[] value) throws IOException {
        return (TransientBlobKey) putBuffer(null, value, TRANSIENT_BLOB);
    }

    @Override
    public TransientBlobKey putTransient(JobID jobId, byte[] value) throws IOException {
        checkNotNull(jobId);
        return (TransientBlobKey) putBuffer(jobId, value, TRANSIENT_BLOB);
    }

    @Override
    public TransientBlobKey putTransient(InputStream inputStream) throws IOException {
        return (TransientBlobKey) putInputStream(null, inputStream, TRANSIENT_BLOB);
    }

    @Override
    public TransientBlobKey putTransient(JobID jobId, InputStream inputStream) throws IOException {
        checkNotNull(jobId);
        return (TransientBlobKey) putInputStream(jobId, inputStream, TRANSIENT_BLOB);
    }

    @Override
    public PermanentBlobKey putPermanent(JobID jobId, byte[] value) throws IOException {
        checkNotNull(jobId);
        return (PermanentBlobKey) putBuffer(jobId, value, PERMANENT_BLOB);
    }

    @Override
    public PermanentBlobKey putPermanent(JobID jobId, InputStream inputStream) throws IOException {
        checkNotNull(jobId);
        return (PermanentBlobKey) putInputStream(jobId, inputStream, PERMANENT_BLOB);
    }

    /**
     * Uploads the data of the given byte array for the given job to the BLOB server.
     *
     * @param jobId the ID of the job the BLOB belongs to
     * @param value the buffer to upload
     * @param blobType whether to make the data permanent or transient
     * @return the computed BLOB key identifying the BLOB on the server
     * @throws IOException thrown if an I/O error occurs while writing it to a local file, or
     *     uploading it to the HA store
     */
    private BlobKey putBuffer(@Nullable JobID jobId, byte[] value, BlobKey.BlobType blobType)
            throws IOException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Received PUT call for BLOB of job {}.", jobId);
        }

        File incomingFile = createTemporaryFilename();
        MessageDigest md = BlobUtils.createMessageDigest();
        BlobKey blobKey = null;
        try (FileOutputStream fos = new FileOutputStream(incomingFile)) {
            md.update(value);
            fos.write(value);
        } catch (IOException ioe) {
            // delete incomingFile from a failed download
            if (!incomingFile.delete() && incomingFile.exists()) {
                LOG.warn("Could not delete the staging file {} for job {}.", incomingFile, jobId);
            }
            throw ioe;
        }

        try {
            // persist file
            blobKey = moveTempFileToStore(incomingFile, jobId, md.digest(), blobType);

            return blobKey;
        } finally {
            // delete incomingFile from a failed download
            if (!incomingFile.delete() && incomingFile.exists()) {
                LOG.warn(
                        "Could not delete the staging file {} for blob key {} and job {}.",
                        incomingFile,
                        blobKey,
                        jobId);
            }
        }
    }

    /**
     * Uploads the data from the given input stream for the given job to the BLOB server.
     *
     * @param jobId the ID of the job the BLOB belongs to
     * @param inputStream the input stream to read the data from
     * @param blobType whether to make the data permanent or transient
     * @return the computed BLOB key identifying the BLOB on the server
     * @throws IOException thrown if an I/O error occurs while reading the data from the input
     *     stream, writing it to a local file, or uploading it to the HA store
     */
    private BlobKey putInputStream(
            @Nullable JobID jobId, InputStream inputStream, BlobKey.BlobType blobType)
            throws IOException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Received PUT call for BLOB of job {}.", jobId);
        }

        File incomingFile = createTemporaryFilename();
        BlobKey blobKey = null;
        try {
            MessageDigest md = writeStreamToFileAndCreateDigest(inputStream, incomingFile);

            // persist file
            blobKey = moveTempFileToStore(incomingFile, jobId, md.digest(), blobType);

            return blobKey;
        } finally {
            // delete incomingFile from a failed download
            if (!incomingFile.delete() && incomingFile.exists()) {
                LOG.warn(
                        "Could not delete the staging file {} for blob key {} and job {}.",
                        incomingFile,
                        blobKey,
                        jobId);
            }
        }
    }

    private static MessageDigest writeStreamToFileAndCreateDigest(
            InputStream inputStream, File file) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(file)) {
            MessageDigest md = BlobUtils.createMessageDigest();
            // read stream
            byte[] buf = new byte[BUFFER_SIZE];
            while (true) {
                final int bytesRead = inputStream.read(buf);
                if (bytesRead == -1) {
                    // done
                    break;
                }
                fos.write(buf, 0, bytesRead);
                md.update(buf, 0, bytesRead);
            }
            return md;
        }
    }

    /**
     * Moves the temporary <tt>incomingFile</tt> to its permanent location where it is available for
     * use.
     *
     * @param incomingFile temporary file created during transfer
     * @param jobId ID of the job this blob belongs to or <tt>null</tt> if job-unrelated
     * @param digest BLOB content digest, i.e. hash
     * @param blobType whether this file is a permanent or transient BLOB
     * @return unique BLOB key that identifies the BLOB on the server
     * @throws IOException thrown if an I/O error occurs while moving the file or uploading it to
     *     the HA store
     */
    BlobKey moveTempFileToStore(
            File incomingFile, @Nullable JobID jobId, byte[] digest, BlobKey.BlobType blobType)
            throws IOException {

        int retries = 10;

        int attempt = 0;
        while (true) {
            // add unique component independent of the BLOB content
            BlobKey blobKey = BlobKey.createKey(blobType, digest);
            File storageFile = BlobUtils.getStorageLocation(storageDir, jobId, blobKey);

            // try again until the key is unique (put the existence check into the lock!)
            readWriteLock.writeLock().lock();
            try {
                if (!storageFile.exists()) {
                    BlobUtils.moveTempFileToStore(
                            incomingFile,
                            jobId,
                            blobKey,
                            storageFile,
                            LOG,
                            blobKey instanceof PermanentBlobKey ? blobStore : null);
                    // add TTL for transient BLOBs:
                    if (blobKey instanceof TransientBlobKey) {
                        // must be inside read or write lock to add a TTL
                        blobExpiryTimes.put(
                                Tuple2.of(jobId, (TransientBlobKey) blobKey),
                                System.currentTimeMillis() + cleanupInterval);
                    }
                    return blobKey;
                }
            } finally {
                readWriteLock.writeLock().unlock();
            }

            ++attempt;
            if (attempt >= retries) {
                String message =
                        "Failed to find a unique key for BLOB of job "
                                + jobId
                                + " (last tried "
                                + storageFile.getAbsolutePath()
                                + ".";
                LOG.error(message + " No retries left.");
                throw new IOException(message);
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Trying to find a unique key for BLOB of job {} (retry {}, last tried {})",
                            jobId,
                            attempt,
                            storageFile.getAbsolutePath());
                }
            }
        }
    }

    /**
     * Deletes the (job-unrelated) file associated with the blob key in the local storage of the
     * blob server.
     *
     * @param key blob key associated with the file to be deleted
     * @return <tt>true</tt> if the given blob is successfully deleted or non-existing;
     *     <tt>false</tt> otherwise
     */
    @Override
    public boolean deleteFromCache(TransientBlobKey key) {
        return deleteInternal(null, key);
    }

    /**
     * Deletes the file associated with the blob key in the local storage of the blob server.
     *
     * @param jobId ID of the job this blob belongs to
     * @param key blob key associated with the file to be deleted
     * @return <tt>true</tt> if the given blob is successfully deleted or non-existing;
     *     <tt>false</tt> otherwise
     */
    @Override
    public boolean deleteFromCache(JobID jobId, TransientBlobKey key) {
        checkNotNull(jobId);
        return deleteInternal(jobId, key);
    }

    /**
     * Deletes the file associated with the blob key in the local storage of the blob server.
     *
     * @param jobId ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
     * @param key blob key associated with the file to be deleted
     * @return <tt>true</tt> if the given blob is successfully deleted or non-existing;
     *     <tt>false</tt> otherwise
     */
    boolean deleteInternal(@Nullable JobID jobId, TransientBlobKey key) {
        final File localFile =
                new File(
                        BlobUtils.getStorageLocationPath(storageDir.getAbsolutePath(), jobId, key));

        readWriteLock.writeLock().lock();

        try {
            if (!localFile.delete() && localFile.exists()) {
                LOG.warn(
                        "Failed to locally delete BLOB "
                                + key
                                + " at "
                                + localFile.getAbsolutePath());
                return false;
            }
            // this needs to happen inside the write lock in case of concurrent getFile() calls
            blobExpiryTimes.remove(Tuple2.of(jobId, key));
            return true;
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    /**
     * Deletes the file associated with the blob key in the local storage of the blob server.
     *
     * @param jobId ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
     * @param key blob key associated with the file to be deleted
     * @return <tt>true</tt> if the given blob is successfully deleted or non-existing;
     *     <tt>false</tt> otherwise
     */
    private boolean deleteInternal(JobID jobId, PermanentBlobKey key) {
        final File localFile =
                new File(
                        BlobUtils.getStorageLocationPath(storageDir.getAbsolutePath(), jobId, key));

        readWriteLock.writeLock().lock();

        try {
            boolean deleteLocally = true;
            if (!localFile.delete() && localFile.exists()) {
                LOG.warn(
                        "Failed to locally delete BLOB "
                                + key
                                + " at "
                                + localFile.getAbsolutePath());
                deleteLocally = false;
            }
            // this needs to happen inside the write lock in case of concurrent getFile() calls
            boolean deleteHA = blobStore.delete(jobId, key);
            return deleteLocally && deleteHA;
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    /**
     * Delete the uploaded data with the given {@link JobID} and {@link PermanentBlobKey}.
     *
     * @param jobId ID of the job this blob belongs to
     * @param key the key of this blob
     */
    @Override
    public boolean deletePermanent(JobID jobId, PermanentBlobKey key) {
        return deleteInternal(jobId, key);
    }

    /**
     * Removes all BLOBs from local and HA store belonging to the given job ID.
     *
     * @param jobId ID of the job this blob belongs to
     * @param cleanupBlobStoreFiles True if the corresponding blob store files shall be cleaned up
     *     as well. Otherwise false.
     * @return <tt>true</tt> if the job directory is successfully deleted or non-existing;
     *     <tt>false</tt> otherwise
     */
    public boolean cleanupJob(JobID jobId, boolean cleanupBlobStoreFiles) {
        checkNotNull(jobId);

        final File jobDir =
                new File(BlobUtils.getStorageLocationPath(storageDir.getAbsolutePath(), jobId));

        readWriteLock.writeLock().lock();

        try {
            // delete locally
            boolean deletedLocally = false;
            try {
                FileUtils.deleteDirectory(jobDir);

                // NOTE: Instead of going through blobExpiryTimes, keep lingering entries - they
                //       will be cleaned up by the timer task which tolerates non-existing files
                //       If inserted again with the same IDs (via put()), the TTL will be updated
                //       again.

                deletedLocally = true;
            } catch (IOException e) {
                LOG.warn(
                        "Failed to locally delete BLOB storage directory at "
                                + jobDir.getAbsolutePath(),
                        e);
            }

            // delete in HA blob store files
            final boolean deletedHA = !cleanupBlobStoreFiles || blobStore.deleteAll(jobId);

            return deletedLocally && deletedHA;
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public PermanentBlobService getPermanentBlobService() {
        return this;
    }

    @Override
    public TransientBlobService getTransientBlobService() {
        return this;
    }

    /**
     * Returns the configuration used by the BLOB server.
     *
     * @return configuration
     */
    @Override
    public final int getMinOffloadingSize() {
        return blobServiceConfiguration.getInteger(BlobServerOptions.OFFLOAD_MINSIZE);
    }

    /**
     * Returns the port on which the server is listening.
     *
     * @return port on which the server is listening
     */
    @Override
    public int getPort() {
        return this.serverSocket.getLocalPort();
    }

    /**
     * Returns the blob expiry times - for testing purposes only!
     *
     * @return blob expiry times (internal state!)
     */
    @VisibleForTesting
    ConcurrentMap<Tuple2<JobID, TransientBlobKey>, Long> getBlobExpiryTimes() {
        return blobExpiryTimes;
    }

    /**
     * Tests whether the BLOB server has been requested to shut down.
     *
     * @return True, if the server has been requested to shut down, false otherwise.
     */
    public boolean isShutdown() {
        return this.shutdownRequested.get();
    }

    /** Access to the server socket, for testing. */
    ServerSocket getServerSocket() {
        return this.serverSocket;
    }

    void unregisterConnection(BlobServerConnection conn) {
        synchronized (activeConnections) {
            activeConnections.remove(conn);
            activeConnections.notifyAll();
        }
    }

    /**
     * Returns all the current active connections in the BlobServer.
     *
     * @return the list of all the active in current BlobServer
     */
    List<BlobServerConnection> getCurrentActiveConnections() {
        synchronized (activeConnections) {
            return new ArrayList<>(activeConnections);
        }
    }
}
