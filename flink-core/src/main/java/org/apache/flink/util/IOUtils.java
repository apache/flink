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

package org.apache.flink.util;

import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.Socket;

import static java.util.Arrays.asList;

/**
 * An utility class for I/O related functionality.
 */
public final class IOUtils {

	/** The block size for byte operations in byte. */
	private static final int BLOCKSIZE = 4096;

	// ------------------------------------------------------------------------
	//  Byte copy operations
	// ------------------------------------------------------------------------

	/**
	 * Copies from one stream to another.
	 *
	 * @param in
	 *        InputStream to read from
	 * @param out
	 *        OutputStream to write to
	 * @param buffSize
	 *        the size of the buffer
	 * @param close
	 *        whether or not close the InputStream and OutputStream at the end. The streams are closed in the finally
	 *        clause.
	 * @throws IOException
	 *         thrown if an error occurred while writing to the output stream
	 */
	public static void copyBytes(final InputStream in, final OutputStream out, final int buffSize, final boolean close)
			throws IOException {

		@SuppressWarnings("resource")
		final PrintStream ps = out instanceof PrintStream ? (PrintStream) out : null;
		final byte[] buf = new byte[buffSize];
		try {
			int bytesRead = in.read(buf);
			while (bytesRead >= 0) {
				out.write(buf, 0, bytesRead);
				if ((ps != null) && ps.checkError()) {
					throw new IOException("Unable to write to output stream.");
				}
				bytesRead = in.read(buf);
			}
		} finally {
			if (close) {
				out.close();
				in.close();
			}
		}
	}

	/**
	 * Copies from one stream to another. <strong>closes the input and output
	 * streams at the end</strong>.
	 *
	 * @param in
	 *        InputStream to read from
	 * @param out
	 *        OutputStream to write to
	 * @throws IOException
	 *         thrown if an I/O error occurs while copying
	 */
	public static void copyBytes(final InputStream in, final OutputStream out) throws IOException {
		copyBytes(in, out, BLOCKSIZE, true);
	}

	/**
	 * Copies from one stream to another.
	 *
	 * @param in
	 *        InputStream to read from
	 * @param out
	 *        OutputStream to write to
	 * @param close
	 *        whether or not close the InputStream and OutputStream at the
	 *        end. The streams are closed in the finally clause.
	 * @throws IOException
	 *         thrown if an I/O error occurs while copying
	 */
	public static void copyBytes(final InputStream in, final OutputStream out, final boolean close) throws IOException {
		copyBytes(in, out, BLOCKSIZE, close);
	}

	// ------------------------------------------------------------------------
	//  Stream input skipping
	// ------------------------------------------------------------------------

	/**
	 * Reads len bytes in a loop.
	 *
	 * @param in
	 *        The InputStream to read from
	 * @param buf
	 *        The buffer to fill
	 * @param off
	 *        offset from the buffer
	 * @param len
	 *        the length of bytes to read
	 * @throws IOException
	 *         if it could not read requested number of bytes for any reason (including EOF)
	 */
	public static void readFully(final InputStream in, final byte[] buf, int off, final int len)
			throws IOException {
		int toRead = len;
		while (toRead > 0) {
			final int ret = in.read(buf, off, toRead);
			if (ret < 0) {
				throw new IOException("Premeture EOF from inputStream");
			}
			toRead -= ret;
			off += ret;
		}
	}

	/**
	 * Similar to readFully(). Skips bytes in a loop.
	 *
	 * @param in
	 *        The InputStream to skip bytes from
	 * @param len
	 *        number of bytes to skip
	 * @throws IOException
	 *         if it could not skip requested number of bytes for any reason (including EOF)
	 */
	public static void skipFully(final InputStream in, long len) throws IOException {
		while (len > 0) {
			final long ret = in.skip(len);
			if (ret < 0) {
				throw new IOException("Premeture EOF from inputStream");
			}
			len -= ret;
		}
	}

	// ------------------------------------------------------------------------
	//  Silent I/O cleanup / closing
	// ------------------------------------------------------------------------

	/**
	 * Close the AutoCloseable objects and <b>ignore</b> any {@link Exception} or
	 * null pointers. Must only be used for cleanup in exception handlers.
	 *
	 * @param log
	 *        the log to record problems to at debug level. Can be <code>null</code>.
	 * @param closeables
	 *        the objects to close
	 */
	public static void cleanup(final Logger log, final AutoCloseable... closeables) {
		for (AutoCloseable c : closeables) {
			if (c != null) {
				try {
					c.close();
				} catch (Exception e) {
					if (log != null && log.isDebugEnabled()) {
						log.debug("Exception in closing " + c, e);
					}
				}
			}
		}
	}

	/**
	 * Closes the stream ignoring {@link IOException}. Must only be called in
	 * cleaning up from exception handlers.
	 *
	 * @param stream
	 *        the stream to close
	 */
	public static void closeStream(final java.io.Closeable stream) {
		cleanup(null, stream);
	}

	/**
	 * Closes the socket ignoring {@link IOException}.
	 *
	 * @param sock
	 *        the socket to close
	 */
	public static void closeSocket(final Socket sock) {
		// avoids try { close() } dance
		if (sock != null) {
			try {
				sock.close();
			} catch (IOException ignored) {
			}
		}
	}

	/**
	 * @see #closeAll(Iterable)
	 */
	public static void closeAll(AutoCloseable... closeables) throws Exception {
		closeAll(asList(closeables));
	}

	/**
	 * Closes all {@link AutoCloseable} objects in the parameter, suppressing exceptions. Exception will be emitted
	 * after calling close() on every object.
	 *
	 * @param closeables iterable with closeables to close.
	 * @throws Exception collected exceptions that occurred during closing
	 */
	public static void closeAll(Iterable<? extends AutoCloseable> closeables) throws Exception {
		if (null != closeables) {

			Exception collectedExceptions = null;

			for (AutoCloseable closeable : closeables) {
				try {
					if (null != closeable) {
						closeable.close();
					}
				} catch (Exception e) {
					collectedExceptions = ExceptionUtils.firstOrSuppressed(e, collectedExceptions);
				}
			}

			if (null != collectedExceptions) {
				throw collectedExceptions;
			}
		}
	}

	/**
	 * Closes all elements in the iterable with closeQuietly().
	 */
	public static void closeAllQuietly(AutoCloseable... closeables) {
		closeAllQuietly(asList(closeables));
	}

	/**
	 * Closes all elements in the iterable with closeQuietly().
	 */
	public static void closeAllQuietly(Iterable<? extends AutoCloseable> closeables) {
		if (null != closeables) {
			for (AutoCloseable closeable : closeables) {
				closeQuietly(closeable);
			}
		}
	}

	/**
	 * Closes the given AutoCloseable.
	 *
	 * <p><b>Important:</b> This method is expected to never throw an exception.
	 */
	public static void closeQuietly(AutoCloseable closeable) {
		try {
			if (closeable != null) {
				closeable.close();
			}
		} catch (Throwable ignored) {}
	}

	// ------------------------------------------------------------------------

	/**
	 * Private constructor to prevent instantiation.
	 */
	private IOUtils() {}
}
