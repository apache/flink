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
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.util.InstantiationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.io.Closeable;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.runtime.blob.BlobServerProtocol.BUFFER_SIZE;
import static org.apache.flink.runtime.blob.BlobServerProtocol.CONTENT_ADDRESSABLE;
import static org.apache.flink.runtime.blob.BlobServerProtocol.DELETE_OPERATION;
import static org.apache.flink.runtime.blob.BlobServerProtocol.GET_OPERATION;
import static org.apache.flink.runtime.blob.BlobServerProtocol.JOB_ID_SCOPE;
import static org.apache.flink.runtime.blob.BlobServerProtocol.MAX_KEY_LENGTH;
import static org.apache.flink.runtime.blob.BlobServerProtocol.NAME_ADDRESSABLE;
import static org.apache.flink.runtime.blob.BlobServerProtocol.PUT_OPERATION;
import static org.apache.flink.runtime.blob.BlobServerProtocol.RETURN_ERROR;
import static org.apache.flink.runtime.blob.BlobServerProtocol.RETURN_OKAY;
import static org.apache.flink.runtime.blob.BlobUtils.readFully;
import static org.apache.flink.runtime.blob.BlobUtils.readLength;
import static org.apache.flink.runtime.blob.BlobUtils.writeLength;

/**
 * The BLOB client can communicate with the BLOB server and either upload (PUT), download (GET),
 * or delete (DELETE) BLOBs.
 */
public final class BlobClient implements Closeable {

	private static final Logger LOG = LoggerFactory.getLogger(BlobClient.class);

	/** The socket connection to the BLOB server. */
	private final Socket socket;

	/**
	 * Instantiates a new BLOB client.
	 * 
	 * @param serverAddress
	 *        the network address of the BLOB server
	 * @throws IOException
	 *         thrown if the connection to the BLOB server could not be established
	 */
	public BlobClient(InetSocketAddress serverAddress) throws IOException {
		this.socket = new Socket();
		try {
			this.socket.connect(serverAddress);
		}
		catch(IOException e) {
			BlobUtils.closeSilently(socket, LOG);
			throw new IOException("Could not connect to BlobServer at address " + serverAddress, e);
		}
	}

	@Override
	public void close() throws IOException {
		this.socket.close();
	}

	public boolean isClosed() {
		return this.socket.isClosed();
	}

	// --------------------------------------------------------------------------------------------
	//  GET
	// --------------------------------------------------------------------------------------------

	/**
	 * Downloads the BLOB identified by the given job ID and key from the BLOB server. If no such BLOB exists on the
	 * server, a {@link FileNotFoundException} is thrown.
	 * 
	 * @param jobID
	 *        the job ID identifying the BLOB to download
	 * @param key
	 *        the key identifying the BLOB to download
	 * @return an input stream to read the retrieved data from
	 * @throws IOException
	 *         thrown if an I/O error occurs during the download
	 */
	public InputStream get(JobID jobID, String key) throws IOException {
		if (key.length() > MAX_KEY_LENGTH) {
			throw new IllegalArgumentException("Keys must not be longer than " + MAX_KEY_LENGTH);
		}

		if (this.socket.isClosed()) {
			throw new IllegalStateException("BLOB Client is not connected. " +
					"Client has been shut down or encountered an error before.");
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("GET BLOB %s / \"%s\" from %s", jobID, key, socket.getLocalSocketAddress()));
		}

		try {
			OutputStream os = this.socket.getOutputStream();
			InputStream is = this.socket.getInputStream();

			sendGetHeader(os, jobID, key, null);
			receiveAndCheckResponse(is);

			return new BlobInputStream(is, null);
		}
		catch (Throwable t) {
			BlobUtils.closeSilently(socket, LOG);
			throw new IOException("GET operation failed: " + t.getMessage(), t);
		}
	}

	/**
	 * Downloads the BLOB identified by the given BLOB key from the BLOB server. If no such BLOB exists on the server, a
	 * {@link FileNotFoundException} is thrown.
	 * 
	 * @param blobKey
	 *        the BLOB key identifying the BLOB to download
	 * @return an input stream to read the retrieved data from
	 * @throws IOException
	 *         thrown if an I/O error occurs during the download
	 */
	public InputStream get(BlobKey blobKey) throws IOException {
		if (this.socket.isClosed()) {
			throw new IllegalStateException("BLOB Client is not connected. " +
					"Client has been shut down or encountered an error before.");
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("GET content addressable BLOB %s from %s", blobKey, socket.getLocalSocketAddress()));
		}

		try {
			OutputStream os = this.socket.getOutputStream();
			InputStream is = this.socket.getInputStream();

			// Send GET header
			sendGetHeader(os, null, null, blobKey);
			receiveAndCheckResponse(is);

			return new BlobInputStream(is, blobKey);
		}
		catch (Throwable t) {
			BlobUtils.closeSilently(socket, LOG);
			throw new IOException("GET operation failed: " + t.getMessage(), t);
		}
	}

	/**
	 * Constructs and writes the header data for a GET operation to the given output stream.
	 *
	 * @param outputStream
	 *        the output stream to write the header data to
	 * @param jobID
	 *        the job ID identifying the BLOB to download or <code>null</code> to indicate the BLOB key should be used
	 *        to identify the BLOB on the server instead
	 * @param key
	 *        the key identifying the BLOB to download or <code>null</code> to indicate the BLOB key should be used to
	 *        identify the BLOB on the server instead
	 * @param blobKey
	 *        the BLOB key to identify the BLOB to download if either the job ID or the regular key are
	 *        <code>null</code>
	 * @throws IOException
	 *         thrown if an I/O error occurs while writing the header data to the output stream
	 */
	private void sendGetHeader(OutputStream outputStream, JobID jobID, String key, BlobKey blobKey) throws IOException {

		// Signal type of operation
		outputStream.write(GET_OPERATION);

		// Check if GET should be done in content-addressable manner
		if (jobID == null || key == null) {
			outputStream.write(CONTENT_ADDRESSABLE);
			blobKey.writeToOutputStream(outputStream);
		}
		else {
			outputStream.write(NAME_ADDRESSABLE);
			// Send job ID and key
			outputStream.write(jobID.getBytes());
			byte[] keyBytes = key.getBytes(BlobUtils.DEFAULT_CHARSET);
			writeLength(keyBytes.length, outputStream);
			outputStream.write(keyBytes);
		}
	}

	private void receiveAndCheckResponse(InputStream is) throws IOException {
		int response = is.read();
		if (response < 0) {
			throw new EOFException("Premature end of response");
		}
		if (response == RETURN_ERROR) {
			Throwable cause = readExceptionFromStream(is);
			throw new IOException("Server side error: " + cause.getMessage(), cause);
		}
		else if (response != RETURN_OKAY) {
			throw new IOException("Unrecognized response");
		}
	}


	// --------------------------------------------------------------------------------------------
	//  PUT
	// --------------------------------------------------------------------------------------------

	/**
	 * Uploads the data of the given byte array to the BLOB server in a content-addressable manner.
	 *
	 * @param value
	 *        the buffer to upload
	 * @return the computed BLOB key identifying the BLOB on the server
	 * @throws IOException
	 *         thrown if an I/O error occurs while uploading the data to the BLOB server
	 */
	public BlobKey put(byte[] value) throws IOException {
		return put(value, 0, value.length);
	}

	/**
	 * Uploads data from the given byte array to the BLOB server in a content-addressable manner.
	 *
	 * @param value
	 *        the buffer to upload data from
	 * @param offset
	 *        the read offset within the buffer
	 * @param len
	 *        the number of bytes to upload from the buffer
	 * @return the computed BLOB key identifying the BLOB on the server
	 * @throws IOException
	 *         thrown if an I/O error occurs while uploading the data to the BLOB server
	 */
	public BlobKey put(byte[] value, int offset, int len) throws IOException {
		return putBuffer(null, null, value, offset, len);
	}

	/**
	 * Uploads the data of the given byte array to the BLOB server and stores it under the given job ID and key.
	 *
	 * @param jobId
	 *        the job ID to identify the uploaded data
	 * @param key
	 *        the key to identify the uploaded data
	 * @param value
	 *        the buffer to upload
	 * @throws IOException
	 *         thrown if an I/O error occurs while uploading the data to the BLOB server
	 */
	public void put(JobID jobId, String key, byte[] value) throws IOException {
		put(jobId, key, value, 0, value.length);
	}

	/**
	 * Uploads data from the given byte array to the BLOB server and stores it under the given job ID and key.
	 *
	 * @param jobId
	 *        the job ID to identify the uploaded data
	 * @param key
	 *        the key to identify the uploaded data
	 * @param value
	 *        the buffer to upload data from
	 * @param offset
	 *        the read offset within the buffer
	 * @param len
	 *        the number of bytes to upload from the buffer
	 * @throws IOException
	 *         thrown if an I/O error occurs while uploading the data to the BLOB server
	 */
	public void put(JobID jobId, String key, byte[] value, int offset, int len) throws IOException {
		if (key.length() > MAX_KEY_LENGTH) {
			throw new IllegalArgumentException("Keys must not be longer than " + MAX_KEY_LENGTH);
		}

		putBuffer(jobId, key, value, offset, len);
	}

	/**
	 * Uploads data from the given input stream to the BLOB server and stores it under the given job ID and key.
	 *
	 * @param jobId
	 *        the job ID to identify the uploaded data
	 * @param key
	 *        the key to identify the uploaded data
	 * @param inputStream
	 *        the input stream to read the data from
	 * @throws IOException
	 *         thrown if an I/O error occurs while reading the data from the input stream or uploading the data to the
	 *         BLOB server
	 */
	public void put(JobID jobId, String key, InputStream inputStream) throws IOException {
		if (key.length() > MAX_KEY_LENGTH) {
			throw new IllegalArgumentException("Keys must not be longer than " + MAX_KEY_LENGTH);
		}

		putInputStream(jobId, key, inputStream);
	}

	/**
	 * Uploads the data from the given input stream to the BLOB server in a content-addressable manner.
	 *
	 * @param inputStream
	 *        the input stream to read the data from
	 * @return the computed BLOB key identifying the BLOB on the server
	 * @throws IOException
	 *         thrown if an I/O error occurs while reading the data from the input stream or uploading the data to the
	 *         BLOB server
	 */
	public BlobKey put(InputStream inputStream) throws IOException {
		return putInputStream(null, null, inputStream);
	}

	/**
	 * Uploads data from the given byte buffer to the BLOB server.
	 *
	 * @param jobId
	 *        the ID of the job the BLOB belongs to or <code>null</code> to store the BLOB in a content-addressable
	 *        manner
	 * @param key
	 *        the key to identify the BLOB on the server or <code>null</code> to store the BLOB in a content-addressable
	 *        manner
	 * @param value
	 *        the buffer to read the data from
	 * @param offset
	 *        the read offset within the buffer
	 * @param len
	 *        the number of bytes to read from the buffer
	 * @return the computed BLOB key if the BLOB has been stored in a content-addressable manner, <code>null</code>
	 *         otherwise
	 * @throws IOException
	 *         thrown if an I/O error occurs while uploading the data to the BLOB server
	 */
	private BlobKey putBuffer(JobID jobId, String key, byte[] value, int offset, int len) throws IOException {
		if (this.socket.isClosed()) {
			throw new IllegalStateException("BLOB Client is not connected. " +
					"Client has been shut down or encountered an error before.");
		}

		if (LOG.isDebugEnabled()) {
			if (jobId == null) {
				LOG.debug(String.format("PUT content addressable BLOB buffer (%d bytes) to %s",
						len, socket.getLocalSocketAddress()));
			} else {
				LOG.debug(String.format("PUT BLOB buffer (%d bytes) under %s / \"%s\" to %s",
						len, jobId, key, socket.getLocalSocketAddress()));
			}
		}

		try {
			final OutputStream os = this.socket.getOutputStream();
			final MessageDigest md = jobId == null ? BlobUtils.createMessageDigest() : null;

			// Send the PUT header
			sendPutHeader(os, jobId, key);

			// Send the value in iterations of BUFFER_SIZE
			int remainingBytes = len;

			while (remainingBytes > 0) {
				final int bytesToSend = Math.min(BUFFER_SIZE, remainingBytes);
				writeLength(bytesToSend, os);

				os.write(value, offset, bytesToSend);

				// Update the message digest if necessary
				if (md != null) {
					md.update(value, offset, bytesToSend);
				}

				remainingBytes -= bytesToSend;
				offset += bytesToSend;
			}
			// send -1 as the stream end
			writeLength(-1, os);

			// Receive blob key and compare
			final InputStream is = this.socket.getInputStream();
			return receivePutResponseAndCompare(is, md);
		}
		catch (Throwable t) {
			BlobUtils.closeSilently(socket, LOG);
			throw new IOException("PUT operation failed: " + t.getMessage(), t);
		}
	}

	/**
	 * Uploads data from the given input stream to the BLOB server.
	 *
	 * @param jobId
	 *        the ID of the job the BLOB belongs to or <code>null</code> to store the BLOB in a content-addressable
	 *        manner
	 * @param key
	 *        the key to identify the BLOB on the server or <code>null</code> to store the BLOB in a content-addressable
	 *        manner
	 * @param inputStream
	 *        the input stream to read the data from
	 * @return he computed BLOB key if the BLOB has been stored in a content-addressable manner, <code>null</code>
	 *         otherwise
	 * @throws IOException
	 *         thrown if an I/O error occurs while uploading the data to the BLOB server
	 */
	private BlobKey putInputStream(JobID jobId, String key, InputStream inputStream) throws IOException {
		if (this.socket.isClosed()) {
			throw new IllegalStateException("BLOB Client is not connected. " +
					"Client has been shut down or encountered an error before.");
		}

		if (LOG.isDebugEnabled()) {
			if (jobId == null) {
				LOG.debug(String.format("PUT content addressable BLOB stream to %s",
						socket.getLocalSocketAddress()));
			} else {
				LOG.debug(String.format("PUT BLOB stream under %s / \"%s\" to %s",
						jobId, key, socket.getLocalSocketAddress()));
			}
		}

		try {
			final OutputStream os = this.socket.getOutputStream();
			final MessageDigest md = jobId == null ? BlobUtils.createMessageDigest() : null;
			final byte[] xferBuf = new byte[BUFFER_SIZE];

			// Send the PUT header
			sendPutHeader(os, jobId, key);

			while (true) {
				final int read = inputStream.read(xferBuf);
				if (read < 0) {
					// we are done. send a -1 and be done
					writeLength(-1, os);
					break;
				}
				if (read > 0) {
					writeLength(read, os);
					os.write(xferBuf, 0, read);
					if (md != null) {
						md.update(xferBuf, 0, read);
					}
				}
			}

			// Receive blob key and compare
			final InputStream is = this.socket.getInputStream();
			return receivePutResponseAndCompare(is, md);
		}
		catch (Throwable t) {
			BlobUtils.closeSilently(socket, LOG);
			throw new IOException("PUT operation failed: " + t.getMessage(), t);
		}
	}

	private BlobKey receivePutResponseAndCompare(InputStream is, MessageDigest md) throws IOException {
		int response = is.read();
		if (response < 0) {
			throw new EOFException("Premature end of response");
		}
		else if (response == RETURN_OKAY) {
			if (md == null) {
				// not content addressable
				return null;
			}

			BlobKey remoteKey = BlobKey.readFromInputStream(is);
			BlobKey localKey = new BlobKey(md.digest());

			if (!localKey.equals(remoteKey)) {
				throw new IOException("Detected data corruption during transfer");
			}

			return localKey;
		}
		else if (response == RETURN_ERROR) {
			Throwable cause = readExceptionFromStream(is);
			throw new IOException("Server side error: " + cause.getMessage(), cause);
		}
		else {
			throw new IOException("Unrecognized response");
		}
	}

	/**
	 * Constructs and writes the header data for a PUT request to the given output stream.
	 * NOTE: If the jobId and key are null, we send the data to the content addressable section.
	 *
	 * @param outputStream
	 *        the output stream to write the PUT header data to
	 * @param jobID
	 *        the ID of job the BLOB belongs to or <code>null</code> to indicate the upload of a
	 *        content-addressable BLOB
	 * @param key
	 *        the key of the BLOB to upload or <code>null</code> to indicate the upload of a content-addressable BLOB
	 * @throws IOException
	 *         thrown if an I/O error occurs while writing the header data to the output stream
	 */
	private void sendPutHeader(OutputStream outputStream, JobID jobID, String key) throws IOException {
		// sanity check that either both are null or both are not null
		if ((jobID != null || key != null) && !(jobID != null && key != null)) {
			throw new IllegalArgumentException();
		}

		// Signal type of operation
		outputStream.write(PUT_OPERATION);

		// Check if PUT should be done in content-addressable manner
		if (jobID == null) {
			outputStream.write(CONTENT_ADDRESSABLE);
		}
		else {
			outputStream.write(NAME_ADDRESSABLE);
			// Send job ID and the key
			byte[] idBytes = jobID.getBytes();
			byte[] keyBytes = key.getBytes(BlobUtils.DEFAULT_CHARSET);
			outputStream.write(idBytes);
			writeLength(keyBytes.length, outputStream);
			outputStream.write(keyBytes);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  DELETE
	// --------------------------------------------------------------------------------------------

	/**
	 * Deletes the BLOB identified by the given BLOB key from the BLOB server.
	 *
	 * @param key
	 *        the key to identify the BLOB
	 * @throws IOException
	 *         thrown if an I/O error occurs while transferring the request to the BLOB server
	 */
	public void delete(BlobKey key) throws IOException {
		if (key == null) {
			throw new IllegalArgumentException("BLOB key must not be null");
		}

		deleteInternal(null, null, key);
	}

	/**
	 * Deletes the BLOB identified by the given job ID and key from the BLOB server.
	 *
	 * @param jobId
	 *        the job ID to identify the BLOB
	 * @param key
	 *        the key to identify the BLOB
	 * @throws IOException
	 *         thrown if an I/O error occurs while transferring the request to the BLOB server
	 */
	public void delete(JobID jobId, String key) throws IOException {
		if (jobId == null) {
			throw new IllegalArgumentException("JobID must not be null");
		}
		if (key == null) {
			throw new IllegalArgumentException("Key must not be null");
		}
		if (key.length() > MAX_KEY_LENGTH) {
			throw new IllegalArgumentException("Keys must not be longer than " + MAX_KEY_LENGTH);
		}

		deleteInternal(jobId, key, null);
	}

	/**
	 * Deletes all BLOBs belonging to the job with the given ID from the BLOB server
	 *
	 * @param jobId
	 *        the job ID to identify the BLOBs to be deleted
	 * @throws IOException
	 *         thrown if an I/O error occurs while transferring the request to the BLOB server
	 */
	public void deleteAll(JobID jobId) throws IOException {
		if (jobId == null) {
			throw new IllegalArgumentException("Argument jobID must not be null");
		}

		deleteInternal(jobId, null, null);
	}

	/**
	 * Delete one or multiple BLOBs from the BLOB server.
	 *
	 * @param jobId The job ID to identify the BLOB(s) to be deleted.
	 * @param key The key to identify the specific BLOB to delete or <code>null</code> to delete
	 *            all BLOBs associated with the job id.
	 * @param bKey The blob key to identify a specific content addressable BLOB. This parameter
	 *             is exclusive with jobId and key.
	 * @throws IOException Thrown if an I/O error occurs while transferring the request to the BLOB server.
	 */
	private void deleteInternal(JobID jobId, String key, BlobKey bKey) throws IOException {
		if ((jobId != null && bKey != null) || (jobId == null && bKey == null)) {
			throw new IllegalArgumentException();
		}

		try {
			final OutputStream outputStream = this.socket.getOutputStream();
			final InputStream inputStream = this.socket.getInputStream();

			// Signal type of operation
			outputStream.write(DELETE_OPERATION);

			// Check if DELETE should be done in content-addressable manner
			if (jobId == null) {
				// delete blob key
				outputStream.write(CONTENT_ADDRESSABLE);
				bKey.writeToOutputStream(outputStream);
			}
			else if (key != null) {
				// delete BLOB for jobID and name key
				outputStream.write(NAME_ADDRESSABLE);
				// Send job ID and the key
				byte[] idBytes = jobId.getBytes();
				byte[] keyBytes = key.getBytes(BlobUtils.DEFAULT_CHARSET);
				outputStream.write(idBytes);
				writeLength(keyBytes.length, outputStream);
				outputStream.write(keyBytes);
			}
			else {
				// delete all blobs for JobID
				outputStream.write(JOB_ID_SCOPE);
				byte[] idBytes = jobId.getBytes();
				outputStream.write(idBytes);
			}

			int response = inputStream.read();
			if (response < 0) {
				throw new EOFException("Premature end of response");
			}
			if (response == RETURN_ERROR) {
				Throwable cause = readExceptionFromStream(inputStream);
				throw new IOException("Server side error: " + cause.getMessage(), cause);
			}
			else if (response != RETURN_OKAY) {
				throw new IOException("Unrecognized response");
			}
		}
		catch (Throwable t) {
			BlobUtils.closeSilently(socket, LOG);
			throw new IOException("DELETE operation failed: " + t.getMessage(), t);
		}
	}

	/**
	 * Retrieves the {@link BlobServer} address from the JobManager and uploads
	 * the JAR files to it.
	 *
	 * @param jobManager Server address of the {@link BlobServer}
	 * @param askTimeout Ask timeout for blob server address retrieval
	 * @param jars       List of JAR files to upload
	 * @throws IOException Thrown if the address retrieval or upload fails
	 */
	public static List<BlobKey> uploadJarFiles(
			ActorGateway jobManager,
			FiniteDuration askTimeout,
			List<Path> jars) throws IOException {

		if (jars.isEmpty()) {
			return Collections.emptyList();
		} else {
			Object msg = JobManagerMessages.getRequestBlobManagerPort();
			Future<Object> futureBlobPort = jobManager.ask(msg, askTimeout);

			try {
				// Retrieve address
				Object result = Await.result(futureBlobPort, askTimeout);
				if (result instanceof Integer) {
					int port = (Integer) result;

					Option<String> jmHost = jobManager.actor().path().address().host();
					String jmHostname = jmHost.isDefined() ? jmHost.get() : "localhost";
					InetSocketAddress serverAddress = new InetSocketAddress(jmHostname, port);

					// Now, upload
					return uploadJarFiles(serverAddress, jars);
				} else {
					throw new Exception("Expected port number (int) as answer, received " + result);
				}
			} catch (Exception e) {
				throw new IOException("Could not retrieve the JobManager's blob port.", e);
			}
		}
	}

	/**
	 * Uploads the JAR files to a {@link BlobServer} at the given address.
	 *
	 * @param serverAddress Server address of the {@link BlobServer}
	 * @param jars List of JAR files to upload
	 * @throws IOException Thrown if the upload fails
	 */
	public static List<BlobKey> uploadJarFiles(InetSocketAddress serverAddress, List<Path> jars) throws IOException {
		if (jars.isEmpty()) {
			return Collections.emptyList();
		} else {
			List<BlobKey> blobKeys = new ArrayList<>();

			try (BlobClient blobClient = new BlobClient(serverAddress)) {
				for (final Path jar : jars) {
					final FileSystem fs = jar.getFileSystem();
					FSDataInputStream is = null;
					try {
						is = fs.open(jar);
						final BlobKey key = blobClient.put(is);
						blobKeys.add(key);
					} finally {
						if (is != null) {
							is.close();
						}
					}
				}
			}

			return blobKeys;
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Miscellaneous
	// --------------------------------------------------------------------------------------------

	private static Throwable readExceptionFromStream(InputStream in) throws IOException {
		int len = readLength(in);
		byte[] bytes = new byte[len];
		readFully(in, bytes, 0, len, "Error message");

		try {
			return (Throwable) InstantiationUtil.deserializeObject(bytes, ClassLoader.getSystemClassLoader());
		}
		catch (ClassNotFoundException e) {
			// should never occur
			throw new IOException("Could not transfer error message", e);
		}
	}
}
