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
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.net.SSLUtils;
import org.apache.flink.util.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;

import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.apache.flink.runtime.blob.BlobKey.BlobType.PERMANENT_BLOB;
import static org.apache.flink.runtime.blob.BlobServerProtocol.BUFFER_SIZE;
import static org.apache.flink.runtime.blob.BlobServerProtocol.GET_OPERATION;
import static org.apache.flink.runtime.blob.BlobServerProtocol.JOB_RELATED_CONTENT;
import static org.apache.flink.runtime.blob.BlobServerProtocol.JOB_UNRELATED_CONTENT;
import static org.apache.flink.runtime.blob.BlobServerProtocol.RETURN_ERROR;
import static org.apache.flink.runtime.blob.BlobServerProtocol.RETURN_OKAY;
import static org.apache.flink.runtime.blob.BlobUtils.readExceptionFromStream;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The BLOB client can communicate with the BLOB server and either upload (PUT), download (GET),
 * or delete (DELETE) BLOBs.
 */
public final class BlobClient implements Closeable {

	private static final Logger LOG = LoggerFactory.getLogger(BlobClient.class);

	/** The socket connection to the BLOB server. */
	private Socket socket;

	/**
	 * Instantiates a new BLOB client.
	 *
	 * @param serverAddress
	 *        the network address of the BLOB server
	 * @param clientConfig
	 *        additional configuration like SSL parameters required to connect to the blob server
	 *
	 * @throws IOException
	 *         thrown if the connection to the BLOB server could not be established
	 */
	public BlobClient(InetSocketAddress serverAddress, Configuration clientConfig) throws IOException {

		try {
			// Check if ssl is enabled
			SSLContext clientSSLContext = null;
			if (clientConfig != null &&
				clientConfig.getBoolean(BlobServerOptions.SSL_ENABLED)) {

				clientSSLContext = SSLUtils.createSSLClientContext(clientConfig);
			}

			if (clientSSLContext != null) {

				LOG.info("Using ssl connection to the blob server");

				SSLSocket sslSocket = (SSLSocket) clientSSLContext.getSocketFactory().createSocket(
					serverAddress.getAddress(),
					serverAddress.getPort());

				// Enable hostname verification for remote SSL connections
				if (!serverAddress.getAddress().isLoopbackAddress()) {
					SSLParameters newSSLParameters = sslSocket.getSSLParameters();
					SSLUtils.setSSLVerifyHostname(clientConfig, newSSLParameters);
					sslSocket.setSSLParameters(newSSLParameters);
				}
				this.socket = sslSocket;
			} else {
				this.socket = new Socket();
				this.socket.connect(serverAddress);
			}

		}
		catch (Exception e) {
			BlobUtils.closeSilently(socket, LOG);
			throw new IOException("Could not connect to BlobServer at address " + serverAddress, e);
		}
	}

	/**
	 * Downloads the given BLOB from the given server and stores its contents to a (local) file.
	 *
	 * <p>Transient BLOB files are deleted after a successful copy of the server's data into the
	 * given <tt>localJarFile</tt>.
	 *
	 * @param jobId
	 * 		job ID the BLOB belongs to or <tt>null</tt> if job-unrelated
	 * @param blobKey
	 * 		BLOB key
	 * @param localJarFile
	 * 		the local file to write to
	 * @param serverAddress
	 * 		address of the server to download from
	 * @param blobClientConfig
	 * 		client configuration for the connection
	 * @param numFetchRetries
	 * 		number of retries before failing
	 *
	 * @throws IOException
	 * 		if an I/O error occurs during the download
	 */
	static void downloadFromBlobServer(
			@Nullable JobID jobId,
			BlobKey blobKey,
			File localJarFile,
			InetSocketAddress serverAddress,
			Configuration blobClientConfig,
			int numFetchRetries) throws IOException {

		final byte[] buf = new byte[BUFFER_SIZE];
		LOG.info("Downloading {}/{} from {}", jobId, blobKey, serverAddress);

		// loop over retries
		int attempt = 0;
		while (true) {
			try (
				final BlobClient bc = new BlobClient(serverAddress, blobClientConfig);
				final InputStream is = bc.getInternal(jobId, blobKey);
				final OutputStream os = new FileOutputStream(localJarFile)
			) {
				while (true) {
					final int read = is.read(buf);
					if (read < 0) {
						break;
					}
					os.write(buf, 0, read);
				}

				return;
			}
			catch (Throwable t) {
				String message = "Failed to fetch BLOB " + jobId + "/" + blobKey + " from " + serverAddress +
					" and store it under " + localJarFile.getAbsolutePath();
				if (attempt < numFetchRetries) {
					if (LOG.isDebugEnabled()) {
						LOG.error(message + " Retrying...", t);
					} else {
						LOG.error(message + " Retrying...");
					}
				}
				else {
					LOG.error(message + " No retries left.", t);
					throw new IOException(message, t);
				}

				// retry
				++attempt;
				LOG.info("Downloading {}/{} from {} (retry {})", jobId, blobKey, serverAddress, attempt);
			}
		} // end loop over retries
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
	 * Downloads the BLOB identified by the given BLOB key from the BLOB server.
	 *
	 * @param jobId
	 * 		ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
	 * @param blobKey
	 * 		blob key associated with the requested file
	 *
	 * @return an input stream to read the retrieved data from
	 *
	 * @throws FileNotFoundException
	 * 		if there is no such file;
	 * @throws IOException
	 * 		if an I/O error occurs during the download
	 */
	InputStream getInternal(@Nullable JobID jobId, BlobKey blobKey)
			throws IOException {

		if (this.socket.isClosed()) {
			throw new IllegalStateException("BLOB Client is not connected. " +
					"Client has been shut down or encountered an error before.");
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("GET BLOB {}/{} from {}.", jobId, blobKey,
				socket.getLocalSocketAddress());
		}

		try {
			OutputStream os = this.socket.getOutputStream();
			InputStream is = this.socket.getInputStream();

			// Send GET header
			sendGetHeader(os, jobId, blobKey);
			receiveAndCheckGetResponse(is);

			return new BlobInputStream(is, blobKey, os);
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
	 * @param jobId
	 * 		ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
	 * @param blobKey
	 * 		blob key associated with the requested file
	 *
	 * @throws IOException
	 *         thrown if an I/O error occurs while writing the header data to the output stream
	 */
	private static void sendGetHeader(
			OutputStream outputStream, @Nullable JobID jobId, BlobKey blobKey)
			throws IOException {
		checkNotNull(blobKey);
		checkArgument(jobId != null || blobKey instanceof TransientBlobKey,
			"permanent BLOBs must be job-related");

		// Signal type of operation
		outputStream.write(GET_OPERATION);

		// Send job ID and key
		if (jobId == null) {
			outputStream.write(JOB_UNRELATED_CONTENT);
		} else {
			outputStream.write(JOB_RELATED_CONTENT);
			outputStream.write(jobId.getBytes());
		}
		blobKey.writeToOutputStream(outputStream);
	}

	/**
	 * Reads the response from the input stream and throws in case of errors.
	 *
	 * @param is
	 * 		stream to read from
	 *
	 * @throws IOException
	 * 		if the response is an error or reading the response failed
	 */
	private static void receiveAndCheckGetResponse(InputStream is) throws IOException {
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
	 * Uploads data from the given byte buffer to the BLOB server.
	 *
	 * @param jobId
	 * 		the ID of the job the BLOB belongs to (or <tt>null</tt> if job-unrelated)
	 * @param value
	 * 		the buffer to read the data from
	 * @param offset
	 * 		the read offset within the buffer
	 * @param len
	 * 		the number of bytes to read from the buffer
	 * @param blobType
	 * 		whether the BLOB should become permanent or transient
	 *
	 * @return the computed BLOB key of the uploaded BLOB
	 *
	 * @throws IOException
	 * 		thrown if an I/O error occurs while uploading the data to the BLOB server
	 */
	BlobKey putBuffer(
			@Nullable JobID jobId, byte[] value, int offset, int len, BlobKey.BlobType blobType)
			throws IOException {

		if (this.socket.isClosed()) {
			throw new IllegalStateException("BLOB Client is not connected. " +
					"Client has been shut down or encountered an error before.");
		}
		checkNotNull(value);

		if (LOG.isDebugEnabled()) {
			LOG.debug("PUT BLOB buffer (" + len + " bytes) to " + socket.getLocalSocketAddress() + ".");
		}

		try (BlobOutputStream os = new BlobOutputStream(jobId, blobType, socket)) {
			os.write(value, offset, len);
			// Receive blob key and compare
			return os.finish();
		} catch (Throwable t) {
			BlobUtils.closeSilently(socket, LOG);
			throw new IOException("PUT operation failed: " + t.getMessage(), t);
		}
	}

	/**
	 * Uploads data from the given input stream to the BLOB server.
	 *
	 * @param jobId
	 * 		the ID of the job the BLOB belongs to (or <tt>null</tt> if job-unrelated)
	 * @param inputStream
	 * 		the input stream to read the data from
	 * @param blobType
	 * 		whether the BLOB should become permanent or transient
	 *
	 * @return the computed BLOB key of the uploaded BLOB
	 *
	 * @throws IOException
	 * 		thrown if an I/O error occurs while uploading the data to the BLOB server
	 */
	BlobKey putInputStream(@Nullable JobID jobId, InputStream inputStream, BlobKey.BlobType blobType)
			throws IOException {

		if (this.socket.isClosed()) {
			throw new IllegalStateException("BLOB Client is not connected. " +
					"Client has been shut down or encountered an error before.");
		}
		checkNotNull(inputStream);

		if (LOG.isDebugEnabled()) {
			LOG.debug("PUT BLOB stream to {}.", socket.getLocalSocketAddress());
		}

		try (BlobOutputStream os = new BlobOutputStream(jobId, blobType, socket)) {
			IOUtils.copyBytes(inputStream, os, BUFFER_SIZE, false);
			return os.finish();
		} catch (Throwable t) {
			BlobUtils.closeSilently(socket, LOG);
			throw new IOException("PUT operation failed: " + t.getMessage(), t);
		}
	}

	/**
	 * Uploads the JAR files to the {@link PermanentBlobService} of the {@link BlobServer} at the
	 * given address with HA as configured.
	 *
	 * @param serverAddress
	 * 		Server address of the {@link BlobServer}
	 * @param clientConfig
	 * 		Any additional configuration for the blob client
	 * @param jobId
	 * 		ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
	 * @param files
	 * 		List of files to upload
	 *
	 * @throws IOException
	 * 		if the upload fails
	 */
	public static List<PermanentBlobKey> uploadFiles(
			InetSocketAddress serverAddress, Configuration clientConfig, JobID jobId, List<Path> files)
			throws IOException {

		checkNotNull(jobId);

		if (files.isEmpty()) {
			return Collections.emptyList();
		} else {
			List<PermanentBlobKey> blobKeys = new ArrayList<>();

			try (BlobClient blobClient = new BlobClient(serverAddress, clientConfig)) {
				for (final Path file : files) {
					final PermanentBlobKey key = blobClient.uploadFile(jobId, file);
					blobKeys.add(key);
				}
			}

			return blobKeys;
		}
	}

	/**
	 * Uploads a single file to the {@link PermanentBlobService} of the given {@link BlobServer}.
	 *
	 * @param jobId
	 * 		ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
	 * @param file
	 * 		file to upload
	 *
	 * @throws IOException
	 * 		if the upload fails
	 */
	public PermanentBlobKey uploadFile(JobID jobId, Path file) throws IOException {
		final FileSystem fs = file.getFileSystem();
		if (fs.getFileStatus(file).isDir()) {
			return uploadDirectory(jobId, file, fs);
		} else {
			try (InputStream is = fs.open(file)) {
				return (PermanentBlobKey) putInputStream(jobId, is, PERMANENT_BLOB);
			}
		}
	}

	private PermanentBlobKey uploadDirectory(JobID jobId, Path file, FileSystem fs) throws IOException {
		try (BlobOutputStream blobOutputStream = new BlobOutputStream(jobId, PERMANENT_BLOB, socket)) {
			try (ZipOutputStream zipStream = new ZipOutputStream(blobOutputStream)) {
				compressDirectoryToZipfile(fs, fs.getFileStatus(file), fs.getFileStatus(file), zipStream);
				zipStream.finish();
				return (PermanentBlobKey) blobOutputStream.finish();
			}
		}
	}

	private static void compressDirectoryToZipfile(FileSystem fs, FileStatus rootDir, FileStatus sourceDir, ZipOutputStream out) throws IOException {
		for (FileStatus file : fs.listStatus(sourceDir.getPath())) {
			LOG.info("Zipping file: {}", file);
			if (file.isDir()) {
				compressDirectoryToZipfile(fs, rootDir, file, out);
			} else {
				String entryName = file.getPath().getPath().replace(rootDir.getPath().getPath(), "");
				LOG.info("Zipping entry: {}, file: {}, rootDir: {}", entryName, file, rootDir);
				ZipEntry entry = new ZipEntry(entryName);
				out.putNextEntry(entry);

				try (FSDataInputStream in = fs.open(file.getPath())) {
					IOUtils.copyBytes(in, out, false);
				}
				out.closeEntry();
			}
		}
	}

}
