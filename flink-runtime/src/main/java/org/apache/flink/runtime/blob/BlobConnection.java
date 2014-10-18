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

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.security.MessageDigest;

import org.apache.flink.runtime.jobgraph.JobID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A BLOB connection handles a series of requests from a particular BLOB client.
 * <p>
 * This class it thread-safe.
 */
class BlobConnection extends Thread {

	/**
	 * The log object used for debugging.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(BlobConnection.class);

	/**
	 * The socket to communicate with the client.
	 */
	private final Socket clientSocket;

	/**
	 * The BLOB server.
	 */
	private final BlobServer blobServer;

	/**
	 * Creates a new BLOB connection for a client request
	 * 
	 * @param clientSocket
	 *        the socket to read/write data
	 * @param blobServer
	 *        the BLOB server
	 */
	BlobConnection(final Socket clientSocket, final BlobServer blobServer) {
		super("BLOB connection for " + clientSocket.getRemoteSocketAddress().toString());

		this.clientSocket = clientSocket;
		this.blobServer = blobServer;
	}

	@Override
	public void run() {

		try {

			final InputStream inputStream = this.clientSocket.getInputStream();
			final OutputStream outputStream = this.clientSocket.getOutputStream();
			final byte[] buffer = new byte[BlobServer.BUFFER_SIZE];

			while (true) {

				// Read the requested operation
				final int operation = inputStream.read();
				if (operation < 0) {
					return;
				}

				switch (operation) {
				case BlobServer.PUT_OPERATION:
					put(inputStream, outputStream, buffer);
					break;
				case BlobServer.GET_OPERATION:
					get(inputStream, outputStream, buffer);
					break;
				case BlobServer.DELETE_OPERATION:
					delete(inputStream, buffer);
					break;
				default:
					throw new IOException("Unknown operation " + operation);
				}
			}

		} catch (IOException ioe) {
			if (LOG.isErrorEnabled()) {
				LOG.error("Error while executing BLOB connection.", ioe);
			}
		} finally {
			closeSilently(this.clientSocket);
		}
	}

	/**
	 * Handles an incoming GET request from a BLOB client.
	 * 
	 * @param inputStream
	 *        the input stream to read incoming data from
	 * @param outputStream
	 *        the output stream to send data back to the client
	 * @param buf
	 *        an auxiliary buffer for data serialization/deserialization
	 * @throws IOException
	 *         thrown if an I/O error occurs while reading/writing data from/to the respective streams
	 */
	private void get(final InputStream inputStream, final OutputStream outputStream, final byte[] buf)
			throws IOException {

		File blob = null;

		final int contentAdressable = inputStream.read();
		if (contentAdressable < 0) {
			throw new EOFException("Expected GET header");
		}

		if (contentAdressable == 0) {
			// Receive the job ID
			BlobServer.readFully(inputStream, buf, 0, JobID.SIZE);
			final ByteBuffer bb = ByteBuffer.wrap(buf);
			final JobID jobID = JobID.fromByteBuffer(bb);
			// Receive the key
			final String key = readKey(buf, inputStream);
			blob = this.blobServer.getStorageLocation(jobID, key);
		} else {
			final BlobKey key = BlobKey.readFromInputStream(inputStream);
			blob = blobServer.getStorageLocation(key);
		}

		// Check if BLOB exists
		if (!blob.exists()) {
			BlobServer.writeLength(-1, buf, outputStream);
			return;
		}

		BlobServer.writeLength((int) blob.length(), buf, outputStream);
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(blob);

			while (true) {

				final int read = fis.read(buf);
				if (read < 0) {
					break;
				}
				outputStream.write(buf, 0, read);
			}

		} finally {
			if (fis != null) {
				fis.close();
			}
		}
	}

	/**
	 * Handles an incoming PUT request from a BLOB client.
	 * 
	 * @param inputStream
	 *        the input stream to read incoming data from
	 * @param outputStream
	 *        the output stream to send data back to the client
	 * @param buf
	 *        an auxiliary buffer for data serialization/deserialization
	 * @throws IOException
	 *         thrown if an I/O error occurs while reading/writing data from/to the respective streams
	 */
	private void put(final InputStream inputStream, final OutputStream outputStream, final byte[] buf)
			throws IOException {

		JobID jobID = null;
		String key = null;
		MessageDigest md = null;
		final int contentAdressable = inputStream.read();
		if (contentAdressable < 0) {
			throw new EOFException("Expected PUT header");
		}

		if (contentAdressable == 0) {
			// Receive the job ID
			BlobServer.readFully(inputStream, buf, 0, JobID.SIZE);
			final ByteBuffer bb = ByteBuffer.wrap(buf);
			jobID = JobID.fromByteBuffer(bb);
			// Receive the key
			key = readKey(buf, inputStream);
		} else {
			md = BlobUtils.createMessageDigest();
		}

		File incomingFile = null;
		FileOutputStream fos = null;

		try {
			incomingFile = blobServer.getTemporaryFilename();
			fos = new FileOutputStream(incomingFile);

			while (true) {

				final int bytesExpected = BlobServer.readLength(buf, inputStream);
				if (bytesExpected > BlobServer.BUFFER_SIZE) {
					throw new IOException("Unexpected number of incoming bytes: " + bytesExpected);
				}

				BlobServer.readFully(inputStream, buf, 0, bytesExpected);
				fos.write(buf, 0, bytesExpected);

				if (md != null) {
					md.update(buf, 0, bytesExpected);
				}

				if (bytesExpected < BlobServer.BUFFER_SIZE) {
					break;
				}
			}

			fos.close();
			fos = null;

			if (contentAdressable == 0) {
				final File storageFile = this.blobServer.getStorageLocation(jobID, key);
				incomingFile.renameTo(storageFile);
				incomingFile = null;
			} else {
				final BlobKey blobKey = new BlobKey(md.digest());
				final File storageFile = blobServer.getStorageLocation(blobKey);
				incomingFile.renameTo(storageFile);
				incomingFile = null;

				// Return computed key to client for validation
				blobKey.writeToOutputStream(outputStream);
			}
		} finally {
			if (fos != null) {
				fos.close();
			}
			if (incomingFile != null) {
				incomingFile.delete();
			}
		}
	}

	/**
	 * Handles an incoming DELETE request from a BLOB client.
	 * 
	 * @param inputStream
	 *        the input stream to read the request from.
	 * @param buf
	 *        an auxiliary buffer for data deserialization
	 * @throws IOException
	 *         thrown if an I/O error occurs while reading the request data from the input stream
	 */
	private void delete(final InputStream inputStream, final byte[] buf) throws IOException {

		// Receive the job ID
		BlobServer.readFully(inputStream, buf, 0, JobID.SIZE);
		final ByteBuffer bb = ByteBuffer.wrap(buf);
		final JobID jobID = JobID.fromByteBuffer(bb);
		String key = null;

		final int r = inputStream.read();
		if (r < 0) {
			throw new EOFException();
		}
		if (r > 0) {
			// Delete individual BLOB
			// Receive the key
			key = readKey(buf, inputStream);

			final File blob = this.blobServer.getStorageLocation(jobID, key);
			blob.delete();

		} else {
			// Delete all BLOBs for this job
			blobServer.deleteJobDirectory(jobID);
		}
	}

	/**
	 * Auxiliary method to silently close a {@link Socket}.
	 * 
	 * @param socket
	 *        the socket to close
	 */
	static void closeSilently(final Socket socket) {

		try {
			if (socket != null) {
				socket.close();
			}
		} catch (IOException ioe) {
		}
	}

	/**
	 * Reads the key of a BLOB from the given input stream.
	 * 
	 * @param buf
	 *        auxiliary buffer to data deserialization
	 * @param inputStream
	 *        the input stream to read the key from
	 * @return the key of a BLOB
	 * @throws IOException
	 *         thrown if an I/O error occurs while reading the key data from the input stream
	 */
	private static String readKey(final byte[] buf,
			final InputStream inputStream) throws IOException {

		final int keyLength = BlobServer.readLength(buf, inputStream);
		if (keyLength > BlobServer.MAX_KEY_LENGTH) {
			throw new IOException("Unexpected key length " + keyLength);
		}

		BlobServer.readFully(inputStream, buf, 0, keyLength);

		return new String(buf, 0, keyLength, BlobUtils.DEFAULT_CHARSET);
	}
}
