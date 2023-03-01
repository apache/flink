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
import org.apache.flink.util.InstantiationUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.security.MessageDigest;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import static org.apache.flink.runtime.blob.BlobKey.BlobType.PERMANENT_BLOB;
import static org.apache.flink.runtime.blob.BlobKey.BlobType.TRANSIENT_BLOB;
import static org.apache.flink.runtime.blob.BlobServerProtocol.BUFFER_SIZE;
import static org.apache.flink.runtime.blob.BlobServerProtocol.GET_OPERATION;
import static org.apache.flink.runtime.blob.BlobServerProtocol.JOB_RELATED_CONTENT;
import static org.apache.flink.runtime.blob.BlobServerProtocol.JOB_UNRELATED_CONTENT;
import static org.apache.flink.runtime.blob.BlobServerProtocol.PUT_OPERATION;
import static org.apache.flink.runtime.blob.BlobServerProtocol.RETURN_ERROR;
import static org.apache.flink.runtime.blob.BlobServerProtocol.RETURN_OKAY;
import static org.apache.flink.runtime.blob.BlobUtils.closeSilently;
import static org.apache.flink.runtime.blob.BlobUtils.readFully;
import static org.apache.flink.runtime.blob.BlobUtils.readLength;
import static org.apache.flink.runtime.blob.BlobUtils.writeLength;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** A BLOB connection handles a series of requests from a particular BLOB client. */
class BlobServerConnection extends Thread {

    /** The log object used for debugging. */
    private static final Logger LOG = LoggerFactory.getLogger(BlobServerConnection.class);

    /** The socket to communicate with the client. */
    private final Socket clientSocket;

    /** The BLOB server. */
    private final BlobServer blobServer;

    /** Read lock to synchronize file accesses. */
    private final Lock readLock;

    /**
     * Creates a new BLOB connection for a client request.
     *
     * @param clientSocket The socket to read/write data.
     * @param blobServer The BLOB server.
     */
    BlobServerConnection(Socket clientSocket, BlobServer blobServer) {
        super("BLOB connection for " + clientSocket.getRemoteSocketAddress());
        setDaemon(true);

        this.clientSocket = clientSocket;
        this.blobServer = checkNotNull(blobServer);

        ReadWriteLock readWriteLock = blobServer.getReadWriteLock();

        this.readLock = readWriteLock.readLock();
    }

    // --------------------------------------------------------------------------------------------
    //  Connection / Thread methods
    // --------------------------------------------------------------------------------------------

    /** Main connection work method. Accepts requests until the other side closes the connection. */
    @Override
    public void run() {
        try {
            final InputStream inputStream = this.clientSocket.getInputStream();
            final OutputStream outputStream = this.clientSocket.getOutputStream();

            while (true) {
                // Read the requested operation
                final int operation = inputStream.read();
                if (operation < 0) {
                    // done, no one is asking anything from us
                    return;
                }

                switch (operation) {
                    case PUT_OPERATION:
                        put(inputStream, outputStream, new byte[BUFFER_SIZE]);
                        break;
                    case GET_OPERATION:
                        get(inputStream, outputStream, new byte[BUFFER_SIZE]);
                        break;
                    default:
                        throw new IOException("Unknown operation " + operation);
                }
            }
        } catch (SocketException e) {
            // this happens when the remote site closes the connection
            LOG.debug("Socket connection closed", e);
        } catch (Throwable t) {
            LOG.error(
                    "Error while executing BLOB connection from {}.",
                    clientSocket.getRemoteSocketAddress(),
                    t);
        } finally {
            closeSilently(clientSocket, LOG);
            blobServer.unregisterConnection(this);
        }
    }

    /** Closes the connection socket and lets the thread exit. */
    public void close() {
        closeSilently(clientSocket, LOG);
        interrupt();
    }

    // --------------------------------------------------------------------------------------------
    //  Actions
    // --------------------------------------------------------------------------------------------

    /**
     * Handles an incoming GET request from a BLOB client.
     *
     * <p>Transient BLOB files are deleted after a successful read operation by the client. Note
     * that we do not enforce atomicity here, i.e. multiple clients reading from the same BLOB may
     * still succeed.
     *
     * @param inputStream the input stream to read incoming data from
     * @param outputStream the output stream to send data back to the client
     * @param buf an auxiliary buffer for data serialization/deserialization
     * @throws IOException thrown if an I/O error occurs while reading/writing data from/to the
     *     respective streams
     */
    private void get(InputStream inputStream, OutputStream outputStream, byte[] buf)
            throws IOException {
        /*
         * Retrieve the file from the (distributed?) BLOB store and store it
         * locally, then send it to the service which requested it.
         *
         * Instead, we could send it from the distributed store directly but
         * chances are high that if there is one request, there will be more
         * so a local cache makes more sense.
         */

        final File blobFile;
        final JobID jobId;
        final BlobKey blobKey;

        try {
            // read HEADER contents: job ID, key, HA mode/permanent or transient BLOB
            final int mode = inputStream.read();
            if (mode < 0) {
                throw new EOFException("Premature end of GET request");
            }

            // Receive the jobId and key
            if (mode == JOB_UNRELATED_CONTENT) {
                jobId = null;
            } else if (mode == JOB_RELATED_CONTENT) {
                byte[] jidBytes = new byte[JobID.SIZE];
                readFully(inputStream, jidBytes, 0, JobID.SIZE, "JobID");
                jobId = JobID.fromByteArray(jidBytes);
            } else {
                throw new IOException("Unknown type of BLOB addressing: " + mode + '.');
            }
            blobKey = BlobKey.readFromInputStream(inputStream);

            checkArgument(
                    blobKey instanceof TransientBlobKey || jobId != null,
                    "Invalid BLOB addressing for permanent BLOBs");

            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Received GET request for BLOB {}/{} from {}.",
                        jobId,
                        blobKey,
                        clientSocket.getInetAddress());
            }

            // up to here, an error can give a good message
        } catch (Throwable t) {
            LOG.error("GET operation from {} failed.", clientSocket.getInetAddress(), t);
            try {
                writeErrorToStream(outputStream, t);
            } catch (IOException e) {
                // since we are in an exception case, it means that we could not send the error
                // ignore this
            }
            clientSocket.close();
            return;
        }

        try {

            readLock.lock();
            try {
                // copy the file to local store if it does not exist yet
                try {
                    blobFile = blobServer.getFileInternal(jobId, blobKey);

                    // enforce a 2GB max for now (otherwise the protocol's length field needs to be
                    // increased)
                    if (blobFile.length() > Integer.MAX_VALUE) {
                        throw new IOException("BLOB size exceeds the maximum size (2 GB).");
                    }

                    outputStream.write(RETURN_OKAY);
                } catch (Throwable t) {
                    LOG.error(
                            "GET operation failed for BLOB {}/{} from {}.",
                            jobId,
                            blobKey,
                            clientSocket.getInetAddress(),
                            t);
                    try {
                        writeErrorToStream(outputStream, t);
                    } catch (IOException e) {
                        // since we are in an exception case, it means that we could not send the
                        // error
                        // ignore this
                    }
                    clientSocket.close();
                    return;
                }

                // from here on, we started sending data, so all we can do is close the connection
                // when something happens
                int blobLen = (int) blobFile.length();
                writeLength(blobLen, outputStream);

                try (FileInputStream fis = new FileInputStream(blobFile)) {
                    int bytesRemaining = blobLen;
                    while (bytesRemaining > 0) {
                        int read = fis.read(buf);
                        if (read < 0) {
                            throw new IOException(
                                    "Premature end of BLOB file stream for "
                                            + blobFile.getAbsolutePath());
                        }
                        outputStream.write(buf, 0, read);
                        bytesRemaining -= read;
                    }
                }
            } finally {
                readLock.unlock();
            }

            // on successful transfer, delete transient files
            int result = inputStream.read();
            if (result < 0) {
                throw new EOFException("Premature end of GET request");
            } else if (blobKey instanceof TransientBlobKey && result == RETURN_OKAY) {
                // ignore the result from the operation
                if (!blobServer.deleteInternal(jobId, (TransientBlobKey) blobKey)) {
                    LOG.warn(
                            "DELETE operation failed for BLOB {}/{} from {}.",
                            jobId,
                            blobKey,
                            clientSocket.getInetAddress());
                }
            }

        } catch (SocketException e) {
            // happens when the other side disconnects
            LOG.debug("Socket connection closed", e);
        } catch (Throwable t) {
            LOG.error("GET operation failed", t);
            clientSocket.close();
        }
    }

    /**
     * Handles an incoming PUT request from a BLOB client.
     *
     * @param inputStream The input stream to read incoming data from
     * @param outputStream The output stream to send data back to the client
     * @param buf An auxiliary buffer for data serialization/deserialization
     * @throws IOException thrown if an I/O error occurs while reading/writing data from/to the
     *     respective streams
     */
    private void put(InputStream inputStream, OutputStream outputStream, byte[] buf)
            throws IOException {
        File incomingFile = null;

        try {
            // read HEADER contents: job ID, HA mode/permanent or transient BLOB
            final int mode = inputStream.read();
            if (mode < 0) {
                throw new EOFException("Premature end of PUT request");
            }

            final JobID jobId;
            if (mode == JOB_UNRELATED_CONTENT) {
                jobId = null;
            } else if (mode == JOB_RELATED_CONTENT) {
                byte[] jidBytes = new byte[JobID.SIZE];
                readFully(inputStream, jidBytes, 0, JobID.SIZE, "JobID");
                jobId = JobID.fromByteArray(jidBytes);
            } else {
                throw new IOException("Unknown type of BLOB addressing.");
            }

            final BlobKey.BlobType blobType;
            {
                final int read = inputStream.read();
                if (read < 0) {
                    throw new EOFException("Read an incomplete BLOB type");
                } else if (read == TRANSIENT_BLOB.ordinal()) {
                    blobType = TRANSIENT_BLOB;
                } else if (read == PERMANENT_BLOB.ordinal()) {
                    blobType = PERMANENT_BLOB;
                    checkArgument(jobId != null, "Invalid BLOB addressing for permanent BLOBs");
                } else {
                    throw new IOException("Invalid data received for the BLOB type: " + read);
                }
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Received PUT request for BLOB of job {} with from {}.",
                        jobId,
                        clientSocket.getInetAddress());
            }

            incomingFile = blobServer.createTemporaryFilename();
            byte[] digest = readFileFully(inputStream, incomingFile, buf);

            BlobKey blobKey = blobServer.moveTempFileToStore(incomingFile, jobId, digest, blobType);

            // Return computed key to client for validation
            outputStream.write(RETURN_OKAY);
            blobKey.writeToOutputStream(outputStream);
        } catch (SocketException e) {
            // happens when the other side disconnects
            LOG.debug("Socket connection closed", e);
        } catch (Throwable t) {
            LOG.error("PUT operation failed", t);
            try {
                writeErrorToStream(outputStream, t);
            } catch (IOException e) {
                // since we are in an exception case, it means not much that we could not send the
                // error
                // ignore this
            }
            clientSocket.close();
        } finally {
            if (incomingFile != null) {
                if (!incomingFile.delete() && incomingFile.exists()) {
                    LOG.warn(
                            "Cannot delete BLOB server staging file "
                                    + incomingFile.getAbsolutePath());
                }
            }
        }
    }

    /**
     * Reads a full file from <tt>inputStream</tt> into <tt>incomingFile</tt> returning its
     * checksum.
     *
     * @param inputStream stream to read from
     * @param incomingFile file to write to
     * @param buf An auxiliary buffer for data serialization/deserialization
     * @return the received file's content hash
     * @throws IOException thrown if an I/O error occurs while reading/writing data from/to the
     *     respective streams
     */
    private static byte[] readFileFully(
            final InputStream inputStream, final File incomingFile, final byte[] buf)
            throws IOException {
        MessageDigest md = BlobUtils.createMessageDigest();

        try (FileOutputStream fos = new FileOutputStream(incomingFile)) {
            while (true) {
                final int bytesExpected = readLength(inputStream);
                if (bytesExpected == -1) {
                    // done
                    break;
                }
                if (bytesExpected > BUFFER_SIZE) {
                    throw new IOException("Unexpected number of incoming bytes: " + bytesExpected);
                }

                readFully(inputStream, buf, 0, bytesExpected, "buffer");
                fos.write(buf, 0, bytesExpected);

                md.update(buf, 0, bytesExpected);
            }
            return md.digest();
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Utilities
    // --------------------------------------------------------------------------------------------

    /**
     * Writes to the output stream the error return code, and the given exception in serialized
     * form.
     *
     * @param out Thr output stream to write to.
     * @param t The exception to send.
     * @throws IOException Thrown, if the output stream could not be written to.
     */
    private static void writeErrorToStream(OutputStream out, Throwable t) throws IOException {
        byte[] bytes = InstantiationUtil.serializeObject(t);
        out.write(RETURN_ERROR);
        writeLength(bytes.length, out);
        out.write(bytes);
    }
}
