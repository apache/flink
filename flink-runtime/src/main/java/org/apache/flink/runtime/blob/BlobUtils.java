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

import com.google.common.io.BaseEncoding;
import org.slf4j.Logger;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility class to work with blob data.
 */
public class BlobUtils {

	/**
	 * Algorithm to be used for calculating the BLOB keys.
	 */
	private static final String HASHING_ALGORITHM = "SHA-1";

	/**
	 * The default character set to translate between characters and bytes.
	 */
	static final Charset DEFAULT_CHARSET = Charset.forName("utf-8");

	/**
	 * Translates the user's key for a BLOB into the internal name used by the BLOB server
	 *
	 * @param key
	 *        the user's key for a BLOB
	 * @return the internal name for the BLOB as used by the BLOB server
	 */
	static String encodeKey(String key) {
		return BaseEncoding.base64().encode(key.getBytes(DEFAULT_CHARSET));
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
			throw new RuntimeException("Cannot instantiate the message digest algorithm " + HASHING_ALGORITHM, e);
		}
	}

	/**
	 * Adds a shutdown hook to the JVM and returns the Thread, which has been registered.
	 */
	static Thread addShutdownHook(final BlobService service, final Logger logger) {
		checkNotNull(service);
		checkNotNull(logger);

		final Thread shutdownHook = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					service.shutdown();
				}
				catch (Throwable t) {
					logger.error("Error during shutdown of blob service via JVM shutdown hook: " + t.getMessage(), t);
				}
			}
		});

		try {
			// Add JVM shutdown hook to call shutdown of service
			Runtime.getRuntime().addShutdownHook(shutdownHook);
			return shutdownHook;
		}
		catch (IllegalStateException e) {
			// JVM is already shutting down. no need to do our work
			return null;
		}
		catch (Throwable t) {
			logger.error("Cannot register shutdown hook that cleanly terminates the BLOB service.");
			return null;
		}
	}

	/**
	 * Auxiliary method to write the length of an upcoming data chunk to an
	 * output stream.
	 *
	 * @param length
	 *        the length of the upcoming data chunk in bytes
	 * @param outputStream
	 *        the output stream to write the length to
	 * @throws IOException
	 *         thrown if an I/O error occurs while writing to the output
	 *         stream
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
	 * Auxiliary method to read the length of an upcoming data chunk from an
	 * input stream.
	 *
	 * @param inputStream
	 *        the input stream to read the length from
	 * @return the length of the upcoming data chunk in bytes
	 * @throws IOException
	 *         thrown if an I/O error occurs while reading from the input
	 *         stream
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
	 * Auxiliary method to read a particular number of bytes from an input stream. This method blocks until the
	 * requested number of bytes have been read from the stream. If the stream cannot offer enough data, an
	 * {@link EOFException} is thrown.
	 *
	 * @param inputStream The input stream to read the data from.
	 * @param buf The buffer to store the read data.
	 * @param off The offset inside the buffer.
	 * @param len The number of bytes to read from the stream.
	 * @param type The name of the type, to throw a good error message in case of not enough data.
	 * @throws IOException
	 *         Thrown if I/O error occurs while reading from the stream or the stream cannot offer enough data.
	 */
	static void readFully(InputStream inputStream, byte[] buf, int off, int len, String type) throws IOException {

		int bytesRead = 0;
		while (bytesRead < len) {

			final int read = inputStream.read(buf, off + bytesRead, len
					- bytesRead);
			if (read < 0) {
				throw new EOFException("Received an incomplete " + type);
			}
			bytesRead += read;
		}
	}

	static void closeSilently(Socket socket, Logger LOG) {
		if (socket != null) {
			try {
				socket.close();
			} catch (Throwable t) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Error while closing resource after BLOB transfer.", t);
				}
			}
		}
	}

	/**
	 * Private constructor to prevent instantiation.
	 */
	private BlobUtils() {
		throw new RuntimeException();
	}
}
