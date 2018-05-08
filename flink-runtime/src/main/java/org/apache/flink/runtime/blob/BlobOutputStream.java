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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.security.MessageDigest;
import java.util.Arrays;

import static org.apache.flink.runtime.blob.BlobServerProtocol.BUFFER_SIZE;
import static org.apache.flink.runtime.blob.BlobServerProtocol.JOB_RELATED_CONTENT;
import static org.apache.flink.runtime.blob.BlobServerProtocol.JOB_UNRELATED_CONTENT;
import static org.apache.flink.runtime.blob.BlobServerProtocol.PUT_OPERATION;
import static org.apache.flink.runtime.blob.BlobServerProtocol.RETURN_ERROR;
import static org.apache.flink.runtime.blob.BlobServerProtocol.RETURN_OKAY;
import static org.apache.flink.runtime.blob.BlobUtils.writeLength;

/**
 * The BLOB output stream is a special implementation of an {@link OutputStream} to send data vi PUT to the BLOB server.
 */
final class BlobOutputStream extends OutputStream {

	private static final Logger LOG = LoggerFactory.getLogger(BlobOutputStream.class);

	private final BlobKey.BlobType blobType;
	private final OutputStream socketStream;
	private final Socket socket;
	private final MessageDigest md;

	BlobOutputStream(JobID jobID, BlobKey.BlobType blobType, Socket socket) throws IOException {
		this.blobType = blobType;

		if (socket.isClosed()) {
			throw new IllegalStateException("BLOB Client is not connected. " +
				"Client has been shut down or encountered an error before.");
		}

		this.socket = socket;
		this.socketStream = socket.getOutputStream();
		this.md = BlobUtils.createMessageDigest();
		sendPutHeader(socketStream, jobID, blobType);
	}

	@Override
	public void write(int b) throws IOException {
		writeLength(1, socketStream);
		socketStream.write(b);
		md.update((byte) b);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		// Send the value in iterations of BUFFER_SIZE
		int remainingBytes = len;

		while (remainingBytes > 0) {
			// want a common code path for byte[] and InputStream at the BlobServer
			// -> since for InputStream we don't know a total size beforehand, send lengths iteratively
			final int bytesToSend = Math.min(BUFFER_SIZE, remainingBytes);
			writeLength(bytesToSend, socketStream);

			socketStream.write(b, off, bytesToSend);

			// Update the message digest
			md.update(b, off, bytesToSend);

			remainingBytes -= bytesToSend;
			off += bytesToSend;
		}
	}

	public BlobKey finish() throws IOException {
		// send -1 as the stream end
		writeLength(-1, socketStream);

		// Receive blob key and compare
		final InputStream is = this.socket.getInputStream();
		return receiveAndCheckPutResponse(is, md, blobType);
	}

	/**
	 * Constructs and writes the header data for a PUT request to the given output stream.
	 *
	 * @param outputStream
	 * 		the output stream to write the PUT header data to
	 * @param jobId
	 * 		the ID of job the BLOB belongs to (or <tt>null</tt> if job-unrelated)
	 * @param blobType
	 * 		whether the BLOB should become permanent or transient
	 *
	 * @throws IOException
	 * 		thrown if an I/O error occurs while writing the header data to the output stream
	 */
	private static void sendPutHeader(
		OutputStream outputStream, @Nullable JobID jobId, BlobKey.BlobType blobType)
		throws IOException {
		// Signal type of operation
		outputStream.write(PUT_OPERATION);
		if (jobId == null) {
			outputStream.write(JOB_UNRELATED_CONTENT);
		} else {
			outputStream.write(JOB_RELATED_CONTENT);
			outputStream.write(jobId.getBytes());
		}
		outputStream.write(blobType.ordinal());
	}

	/**
	 * Reads the response from the input stream and throws in case of errors.
	 *
	 * @param is
	 * 		stream to read from
	 * @param md
	 * 		message digest to check the response against
	 * @param blobType
	 * 		whether the BLOB should be permanent or transient
	 *
	 * @throws IOException
	 * 		if the response is an error, the message digest does not match or reading the response
	 * 		failed
	 */
	private static BlobKey receiveAndCheckPutResponse(
		InputStream is, MessageDigest md, BlobKey.BlobType blobType)
		throws IOException {
		int response = is.read();
		if (response < 0) {
			throw new EOFException("Premature end of response");
		}
		else if (response == RETURN_OKAY) {

			BlobKey remoteKey = BlobKey.readFromInputStream(is);
			byte[] localHash = md.digest();

			if (blobType != remoteKey.getType()) {
				throw new IOException("Detected data corruption during transfer");
			}
			if (!Arrays.equals(localHash, remoteKey.getHash())) {
				throw new IOException("Detected data corruption during transfer");
			}

			return remoteKey;
		}
		else if (response == RETURN_ERROR) {
			Throwable cause = BlobUtils.readExceptionFromStream(is);
			throw new IOException("Server side error: " + cause.getMessage(), cause);
		}
		else {
			throw new IOException("Unrecognized response: " + response + '.');
		}
	}
}
