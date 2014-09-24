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

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.security.MessageDigest;

import org.apache.flink.runtime.AbstractID;
import org.apache.flink.runtime.jobgraph.JobID;

/**
 * The BLOB client can communicate with the BLOB server and either upload (PUT), download (GET), or delete (DELETE)
 * BLOBs.
 * <p>
 * This class is not thread-safe.
 */
public final class BlobClient implements Closeable {

	/**
	 * The socket connection to the BLOB server.
	 */
	private Socket socket;

	/**
	 * Instantiates a new BLOB client.
	 * 
	 * @param serverAddress
	 *        the network address of the BLOB server
	 * @throws IOException
	 *         thrown if the connection to the BLOB server could not be established
	 */
	public BlobClient(final InetSocketAddress serverAddress) throws IOException {

		this.socket = new Socket();
		this.socket.connect(serverAddress);
	}

	/**
	 * Constructs and writes the header data for a PUT request to the given output stream.
	 * 
	 * @param outputStream
	 *        the output stream to write the PUT header data to
	 * @param jobID
	 *        the ID of job the BLOB belongs to or <code>null</code> to indicate the upload of a
	 *        content-addressable BLOB
	 * @param key
	 *        the key of the BLOB to upload or <code>null</code> to indicate the upload of a content-addressable BLOB
	 * @param buf
	 *        an auxiliary buffer used for data serialization
	 * @throws IOException
	 *         thrown if an I/O error occurs while writing the header data to the output stream
	 */
	private void sendPutHeader(final OutputStream outputStream, final JobID jobID, final String key, final byte[] buf)
			throws IOException {

		// Signal type of operation
		outputStream.write(BlobServer.PUT_OPERATION);

		// Check if PUT should be done in content-addressable manner
		if (jobID == null || key == null) {
			outputStream.write(1);
		} else {
			outputStream.write(0);
			// Send job ID
			final ByteBuffer bb = ByteBuffer.wrap(buf);
			jobID.write(bb);
			outputStream.write(buf);

			// Send the key
			byte[] keyBytes = key.getBytes(BlobUtils.DEFAULT_CHARSET);
			BlobServer.writeLength(keyBytes.length, buf, outputStream);
			outputStream.write(keyBytes);
		}
	}

	/**
	 * Uploads the data of the given byte array to the BLOB server in a content-addressable manner.
	 * 
	 * @param value
	 *        the buffer to upload
	 * @return the computed BLOB key identifying the BLOB on the server
	 * @throws IOException
	 *         thrown if an I/O error occurs while uploading the data to the BLOB server
	 */
	public BlobKey put(final byte[] value) throws IOException {

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
	public BlobKey put(final byte[] value, final int offset, final int len) throws IOException {

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
	public void put(final JobID jobId, final String key, final byte[] value) throws IOException {

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
	public void put(final JobID jobId, final String key, final byte[] value, final int offset, final int len)
			throws IOException {

		if (key.length() > BlobServer.MAX_KEY_LENGTH) {
			throw new IllegalArgumentException("Keys must not be longer than " + BlobServer.MAX_KEY_LENGTH);
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
	public void put(final JobID jobId, final String key, final InputStream inputStream) throws IOException {

		if (key.length() > BlobServer.MAX_KEY_LENGTH) {
			throw new IllegalArgumentException("Keys must not be longer than " + BlobServer.MAX_KEY_LENGTH);
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
	public BlobKey put(final InputStream inputStream) throws IOException {

		return putInputStream(null, null, inputStream);
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
	public void delete(final JobID jobId, final String key) throws IOException {

		if (jobId == null) {
			throw new IllegalArgumentException("Argument jobID must not be null");
		}

		if (key == null) {
			throw new IllegalArgumentException("Argument key must not be null");
		}

		if (key.length() > BlobServer.MAX_KEY_LENGTH) {
			throw new IllegalArgumentException("Keys must not be longer than " + BlobServer.MAX_KEY_LENGTH);
		}

		deleteInternal(jobId, key);
	}

	/**
	 * Deletes all BLOBs belonging to the job with the given ID from the BLOB server
	 * 
	 * @param jobId
	 *        the job ID to identify the BLOBs to be deleted
	 * @throws IOException
	 *         thrown if an I/O error occurs while transferring the request to the BLOB server
	 */
	public void deleteAll(final JobID jobId) throws IOException {

		if (jobId == null) {
			throw new IllegalArgumentException("Argument jobID must not be null");
		}

		deleteInternal(jobId, null);
	}

	/**
	 * Delete one or multiple BLOBs from the BLOB server.
	 * 
	 * @param jobId
	 *        the job ID to identify the BLOB(s) to be deleted
	 * @param key
	 *        the key to identify the specific BLOB to delete or <code>null</code> to delete all BLOBs associated with
	 *        the job
	 * @throws IOException
	 *         thrown if an I/O error occurs while transferring the request to the BLOB server
	 */
	private void deleteInternal(final JobID jobId, final String key) throws IOException {

		final OutputStream os = this.socket.getOutputStream();
		final byte[] buf = new byte[AbstractID.SIZE];

		// Signal type of operation
		os.write(BlobServer.DELETE_OPERATION);

		// Send job ID
		final ByteBuffer bb = ByteBuffer.wrap(buf);
		jobId.write(bb);
		os.write(buf);

		if (key == null) {
			os.write(0);
		} else {
			os.write(1);
			// Send the key
			byte[] keyBytes = key.getBytes(BlobUtils.DEFAULT_CHARSET);
			BlobServer.writeLength(keyBytes.length, buf, os);
			os.write(keyBytes);
		}
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
	private BlobKey putBuffer(final JobID jobId, final String key, final byte[] value, final int offset, final int len)
			throws IOException {

		final OutputStream os = this.socket.getOutputStream();
		final MessageDigest md = (jobId == null || key == null) ? BlobUtils.createMessageDigest() :
				null;
		final byte[] buf = new byte[AbstractID.SIZE];

		// Send the PUT header
		sendPutHeader(os, jobId, key, buf);

		// Send the value in iterations of BUFFER_SIZE
		int remainingBytes = value.length;
		int bytesSent = 0;

		while (remainingBytes > 0) {

			final int bytesToSend = Math.min(BlobServer.BUFFER_SIZE, remainingBytes);
			BlobServer.writeLength(bytesToSend, buf, os);

			os.write(value, offset + bytesSent, bytesToSend);

			// Update the message digest if necessary
			if (md != null) {
				md.update(value, offset + bytesSent, bytesToSend);
			}

			remainingBytes -= bytesToSend;
			bytesSent += bytesToSend;
		}

		if (md == null) {
			return null;
		}

		// Receive blob key and compare
		final InputStream is = this.socket.getInputStream();
		final BlobKey localKey = new BlobKey(md.digest());
		final BlobKey remoteKey = BlobKey.readFromInputStream(is);

		if (!localKey.equals(remoteKey)) {
			throw new IOException("Detected data corruption during transfer");
		}

		return localKey;
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
	private BlobKey putInputStream(final JobID jobId, final String key, final InputStream inputStream)
			throws IOException {

		final OutputStream os = this.socket.getOutputStream();
		final MessageDigest md = (jobId == null || key == null) ? BlobUtils.createMessageDigest
				() : null;
		final byte[] buf = new byte[AbstractID.SIZE];
		final byte[] xferBuf = new byte[BlobServer.BUFFER_SIZE];

		// Send the PUT header
		sendPutHeader(os, jobId, key, buf);

		while (true) {

			final int read = inputStream.read(xferBuf);
			if (read < 0) {
				break;
			}
			if (read > 0) {
				BlobServer.writeLength(read, buf, os);
				os.write(xferBuf, 0, read);
				if (md != null) {
					md.update(xferBuf, 0, read);
				}
			}
		}

		if (md == null) {
			return null;
		}

		// Receive blob key and compare
		final InputStream is = this.socket.getInputStream();
		final BlobKey localKey = new BlobKey(md.digest());
		final BlobKey remoteKey = BlobKey.readFromInputStream(is);

		if (!localKey.equals(remoteKey)) {
			throw new IOException("Detected data corruption during transfer");
		}

		return localKey;
	}

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
	public InputStream get(final JobID jobID, final String key) throws IOException {

		if (key.length() > BlobServer.MAX_KEY_LENGTH) {
			throw new IllegalArgumentException("Keys must not be longer than " + BlobServer.MAX_KEY_LENGTH);
		}

		final OutputStream os = this.socket.getOutputStream();
		final byte[] buf = new byte[AbstractID.SIZE];

		// Send GET header
		sendGetHeader(os, jobID, key, null, buf);

		return new BlobInputStream(this.socket.getInputStream(), null, buf);
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
	public InputStream get(final BlobKey blobKey) throws IOException {

		final OutputStream os = this.socket.getOutputStream();
		final byte[] buf = new byte[AbstractID.SIZE];

		// Send GET header
		sendGetHeader(os, null, null, blobKey, buf);

		return new BlobInputStream(this.socket.getInputStream(), blobKey, buf);
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
	 * @param key2
	 *        the BLOB key to identify the BLOB to download if either the job ID or the regular key are
	 *        <code>null</code>
	 * @param buf
	 *        auxiliary buffer used for data serialization
	 * @throws IOException
	 *         thrown if an I/O error occurs while writing the header data to the output stream
	 */
	private void sendGetHeader(final OutputStream outputStream, final JobID jobID, final String key,
			final BlobKey key2, final byte[] buf) throws IOException {

		// Signal type of operation
		outputStream.write(BlobServer.GET_OPERATION);

		// Check if GET should be done in content-addressable manner
		if (jobID == null || key == null) {
			outputStream.write(1);
			key2.writeToOutputStream(outputStream);
		} else {
			outputStream.write(0);
			// Send job ID
			final ByteBuffer bb = ByteBuffer.wrap(buf);
			jobID.write(bb);
			outputStream.write(buf);

			// Send the key
			byte[] keyBytes = key.getBytes(BlobUtils.DEFAULT_CHARSET);
			BlobServer.writeLength(keyBytes.length, buf, outputStream);
			outputStream.write(keyBytes);
		}
	}

	@Override
	public void close() throws IOException {

		this.socket.close();
	}
}
