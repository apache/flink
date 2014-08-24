/**
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.runtime.jobgraph.JobID;

class BlobConnection extends Thread {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(BlobConnection.class);

	private final Socket clientSocket;

	BlobConnection(final Socket clientSocket) {
		super("BLOB connection for " + clientSocket.getRemoteSocketAddress().toString());

		this.clientSocket = clientSocket;
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
				LOG.error(ioe);
			}
		} finally {
			closeSilently(this.clientSocket);
		}
	}

	private static void get(final InputStream inputStream, final OutputStream outputStream, final byte[] buf)
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
			blob = BlobServer.getStorageLocation(jobID, key);
		} else {
			final BlobKey key = BlobKey.readFromInputStream(inputStream);
			blob = BlobServer.getStorageLocation(key);
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

	private static void put(final InputStream inputStream, final OutputStream outputStream, final byte[] buf)
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
			md = BlobServer.createMessageDigest();
		}

		File incomingFile = null;
		FileOutputStream fos = null;

		try {
			incomingFile = BlobServer.getTemporaryFilename();
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
				final File storageFile = BlobServer.getStorageLocation(jobID, key);
				incomingFile.renameTo(storageFile);
				incomingFile = null;
			} else {
				final BlobKey blobKey = new BlobKey(md.digest());
				final File storageFile = BlobServer.getStorageLocation(blobKey);
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

	private static void delete(final InputStream inputStream, final byte[] buf) throws IOException {

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

			final File blob = BlobServer.getStorageLocation(jobID, key);
			blob.delete();

		} else {
			// Delete all BLOBs for this job
			BlobServer.deleteJobDirectory(jobID);
		}
	}

	static void closeSilently(final Closeable closeable) {

		try {
			if (closeable != null) {
				closeable.close();
			}
		} catch (IOException ioe) {
		}
	}

	private static String readKey(final byte[] buf,
			final InputStream inputStream) throws IOException {

		final int keyLength = BlobServer.readLength(buf, inputStream);
		if (keyLength > BlobServer.MAX_KEY_LENGTH) {
			throw new IOException("Unexpected key length " + keyLength);
		}

		BlobServer.readFully(inputStream, buf, 0, keyLength);

		return new String(buf, 0, keyLength, BlobServer.DEFAULT_CHARSET);
	}
}
