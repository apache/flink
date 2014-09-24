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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.URL;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.flink.runtime.jobgraph.JobID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the BLOB server. The BLOB server is responsible for listening for incoming requests and
 * spawning threads to handle these requests. Furthermore, it takes care of creating the directory structure to store
 * the BLOBs or temporarily cache them.
 * <p>
 * This class is thread-safe.
 */
public final class BlobServer extends Thread implements BlobService{

	/**
	 * The log object used for debugging.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(BlobServer.class);

	/**
	 * The buffer size in bytes for network transfers.
	 */
	static final int BUFFER_SIZE = 4096;

	/**
	 * The maximum key length allowed for storing BLOBs.
	 */
	static final int MAX_KEY_LENGTH = 64;

	/**
	 * Internal code to identify a PUT operation.
	 */
	static final byte PUT_OPERATION = 0;

	/**
	 * Internal code to identify a GET operation.
	 */
	static final byte GET_OPERATION = 1;

	/**
	 * Internal code to identify a DELETE operation.
	 */
	static final byte DELETE_OPERATION = 2;

	/**
	 * Counter to generate unique names for temporary files.
	 */
	private final AtomicInteger tempFileCounter = new AtomicInteger(0);

	/**
	 * The server socket listening for incoming connections.
	 */
	private final ServerSocket serverSocket;

	/**
	 * Indicates whether a shutdown of server component has been requested.
	 */
	private volatile boolean shutdownRequested = false;

	/**
	 * Is the root directory for file storage
	 */
	private final File storageDir;

	/**
	 * Instantiates a new BLOB server and binds it to a free network port.
	 * 
	 * @throws IOException
	 *         thrown if the BLOB server cannot bind to a free network port
	 */
	public BlobServer() throws IOException {

		this.serverSocket = new ServerSocket(0);
		start();

		if (LOG.isInfoEnabled()) {
			LOG.info(String.format("Started BLOB server on port %d",
				this.serverSocket.getLocalPort()));
		}

		this.storageDir = BlobUtils.initStorageDirectory();
	}

	/**
	 * Returns the network port the BLOB server is bound to. The return value of this method is undefined after the BLOB
	 * server has been shut down.
	 * 
	 * @return the network port the BLOB server is bound to
	 */
	public int getServerPort() {

		return this.serverSocket.getLocalPort();
	}

	/**
	 * Returns a file handle to the file associated with the given blob key on the blob
	 * server.
	 *
	 * @param key identifying the file
	 * @return file handle to the file
	 */
	public File getStorageLocation(BlobKey key){
		return BlobUtils.getStorageLocation(storageDir, key);
	}

	/**
	 * Returns a file handle to the file identified by the given jobID and key.
	 *
	 * @param jobID to which the file is associated
	 * @param key to identify the file within the job context
	 * @return file handle to the file
	 */
	public File getStorageLocation(JobID jobID, String key){
		return BlobUtils.getStorageLocation(storageDir, jobID, key);
	}

	/**
	 * Method which deletes all files associated with the given jobID.
	 *
	 * @param jobID all files associated to this jobID will be deleted
	 * @throws IOException
	 */
	public void deleteJobDirectory(JobID jobID) throws IOException{
		BlobUtils.deleteJobDirectory(storageDir, jobID);
	}

	/**
	 * Returns a temporary file inside the BLOB server's incoming directory.
	 * 
	 * @return a temporary file inside the BLOB server's incoming directory
	 */
	File getTemporaryFilename() {
		return new File(BlobUtils.getIncomingDirectory(storageDir), String.format("temp-%08d",
				tempFileCounter.getAndIncrement()));
	}

	@Override
	public void run() {

		try {

			while (!this.shutdownRequested) {
				new BlobConnection(this.serverSocket.accept(), this).start();
			}

		} catch (IOException ioe) {
			if (!this.shutdownRequested && LOG.isErrorEnabled()) {
				LOG.error("Blob server stopped working.", ioe);
			}
		}
	}

	/**
	 * Shuts down the BLOB server.
	 */
	@Override
	public void shutdown() throws IOException {

		this.shutdownRequested = true;
		try {
			this.serverSocket.close();
		} catch (IOException ioe) {
				LOG.debug("Error while closing the server socket.", ioe);
		}
		try {
			join();
		} catch (InterruptedException ie) {
			LOG.debug("Error while waiting for this thread to die.", ie);
		}

		// Clean up the storage directory
		FileUtils.deleteDirectory(storageDir);

		// TODO: Find/implement strategy to handle content-addressable BLOBs
	}

	/**
	 * Method which retrieves the URL of a file associated with a blob key. The blob server looks
	 * the blob key up in its local storage. If the file exists, then the URL is returned. If the
	 * file does not exist, then a FileNotFoundException is thrown.
	 *
	 * @param requiredBlob blob key associated with the requested file
	 * @return URL of the file
	 * @throws IOException
	 */
	@Override
	public URL getURL(BlobKey requiredBlob) throws IOException {
		if(requiredBlob == null){
			throw new IllegalArgumentException("Required BLOB cannot be null.");
		}

		final File localFile = BlobUtils.getStorageLocation(storageDir, requiredBlob);

		if(!localFile.exists()){
			throw new FileNotFoundException("File " + localFile.getCanonicalPath() + " does " +
					"not exist.");
		}else{
			return localFile.toURI().toURL();
		}
	}

	/**
	 * This method deletes the file associated to the blob key if it exists in the local storage
	 * of the blob server.
	 *
	 * @param blobKey associated with the file to be deleted
	 * @throws IOException
	 */
	@Override
	public void delete(BlobKey blobKey) throws IOException {
		final File localFile = BlobUtils.getStorageLocation(storageDir, blobKey);

		if(localFile.exists()){
			localFile.delete();
		}
	}

	/**
	 * Returns the port on which the server is listening.
	 *
	 * @return port on which the server is listening
	 */
	@Override
	public int getPort() {
		return getServerPort();
	}

	/**
	 * Auxiliary method to write the length of an upcoming data chunk to an
	 * output stream.
	 *
	 * @param length
	 *        the length of the upcoming data chunk in bytes
	 * @param buf
	 *        the byte buffer to use for the integer serialization
	 * @param outputStream
	 *        the output stream to write the length to
	 * @throws IOException
	 *         thrown if an I/O error occurs while writing to the output
	 *         stream
	 */
	static void writeLength(final int length, final byte[] buf,
							final OutputStream outputStream) throws IOException {

		buf[0] = (byte) (length & 0xff);
		buf[1] = (byte) ((length >> 8) & 0xff);
		buf[2] = (byte) ((length >> 16) & 0xff);
		buf[3] = (byte) ((length >> 24) & 0xff);

		outputStream.write(buf, 0, 4);
	}

	/**
	 * Auxiliary method to read the length of an upcoming data chunk from an
	 * input stream.
	 *
	 * @param buf
	 *        the byte buffer to use for the integer deserialization
	 * @param inputStream
	 *        the input stream to read the length from
	 * @return the length of the upcoming data chunk in bytes
	 * @throws IOException
	 *         thrown if an I/O error occurs while reading from the input
	 *         stream
	 */
	static int readLength(final byte[] buf, final InputStream inputStream)
			throws IOException {

		int bytesRead = 0;
		while (bytesRead < 4) {
			final int read = inputStream.read(buf, bytesRead, 4 - bytesRead);
			if (read < 0) {
				throw new EOFException();
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
	 * Auxiliary method to read a particular number of bytes from an input stream. This method blocks until the
	 * requested number of bytes have been read from the stream. If the stream cannot offer enough data, an
	 * {@link EOFException} is thrown.
	 *
	 * @param inputStream
	 *        the input stream to read the data from
	 * @param buf
	 *        the buffer to store the read data
	 * @param off
	 *        the offset inside the buffer
	 * @param len
	 *        the number of bytes to read from the stream
	 * @throws IOException
	 *         thrown if I/O error occurs while reading from the stream or the stream cannot offer enough data
	 */
	static void readFully(final InputStream inputStream,
						final byte[] buf, final int off, final int len) throws IOException {

		int bytesRead = 0;
		while (bytesRead < len) {

			final int read = inputStream.read(buf, off + bytesRead, len
					- bytesRead);
			if (read < 0) {
				throw new EOFException();
			}
			bytesRead += read;
		}
	}
}
