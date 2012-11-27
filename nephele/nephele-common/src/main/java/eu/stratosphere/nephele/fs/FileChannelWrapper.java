/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.fs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

public final class FileChannelWrapper extends FileChannel {

	private final FileSystem fs;

	private final Path checkpointFile;

	private final byte[] buf;

	private final short replication;

	private FSDataOutputStream outputStream = null;

	private FSDataInputStream inputStream = null;

	private long nextExpectedWritePosition = 0L;

	private long nextExpectedReadPosition = 0L;

	public FileChannelWrapper(final FileSystem fs, final Path checkpointFile, final int bufferSize,
			final short replication) {

		this.fs = fs;
		this.checkpointFile = checkpointFile;
		this.buf = new byte[bufferSize];
		this.replication = replication;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void force(final boolean metaData) throws IOException {

		throw new UnsupportedOperationException("Method force is not implemented");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FileLock lock(final long position, final long size, final boolean shared) throws IOException {

		throw new UnsupportedOperationException("Method lock is not implemented");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public MappedByteBuffer map(final MapMode mode, final long position, final long size) throws IOException {

		throw new UnsupportedOperationException("Method map is not implemented");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long position() throws IOException {

		throw new UnsupportedOperationException("Method position is not implemented");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FileChannel position(final long newPosition) throws IOException {

		throw new UnsupportedOperationException("Method position is not implemented");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int read(final ByteBuffer dst) throws IOException {

		throw new UnsupportedOperationException("Method read is not implemented");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int read(final ByteBuffer dst, final long position) throws IOException {

		final int length = Math.min(this.buf.length, dst.remaining());

		final FSDataInputStream inputStream = getInputStream();
		if (position != this.nextExpectedReadPosition) {
			System.out.println("Next expected position is " + this.nextExpectedReadPosition + ", seeking to "
				+ position);
			inputStream.seek(position);
			this.nextExpectedReadPosition = position;
		}

		final int bytesRead = inputStream.read(this.buf, 0, length);
		if (bytesRead == -1) {
			return -1;
		}

		dst.put(this.buf, 0, length);

		this.nextExpectedReadPosition += bytesRead;

		return bytesRead;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long read(final ByteBuffer[] dsts, final int offset, final int length) throws IOException {

		throw new UnsupportedOperationException("Method read is not implemented");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long size() throws IOException {

		throw new UnsupportedOperationException("Method size is not implemented");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long transferFrom(final ReadableByteChannel src, final long position, final long count) throws IOException {

		throw new UnsupportedOperationException("Method transferFrom is not implemented");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long transferTo(final long position, final long count, final WritableByteChannel target) throws IOException {

		throw new UnsupportedOperationException("Method transferTo is not implemented");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FileChannel truncate(final long size) throws IOException {

		throw new UnsupportedOperationException("Method truncate is not implemented");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FileLock tryLock(final long position, final long size, final boolean shared) throws IOException {

		throw new UnsupportedOperationException("Method tryLock is not implemented");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int write(final ByteBuffer src) throws IOException {

		return write(src, this.nextExpectedWritePosition);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int write(final ByteBuffer src, final long position) throws IOException {

		if (position != this.nextExpectedWritePosition) {
			throw new IOException("Next expected write position is " + this.nextExpectedWritePosition);
		}

		final FSDataOutputStream outputStream = getOutputStream();

		int totalBytesWritten = 0;

		while (src.hasRemaining()) {

			final int length = Math.min(this.buf.length, src.remaining());
			src.get(this.buf, 0, length);
			outputStream.write(this.buf, 0, length);
			totalBytesWritten += length;
		}

		this.nextExpectedWritePosition += totalBytesWritten;

		return totalBytesWritten;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long write(final ByteBuffer[] srcs, final int offset, final int length) throws IOException {

		throw new UnsupportedOperationException("Method write is not implemented");
	}

	private FSDataOutputStream getOutputStream() throws IOException {

		if (this.outputStream == null) {
			this.outputStream = this.fs.create(this.checkpointFile, false, this.buf.length, this.replication,
				this.fs.getDefaultBlockSize());
		}

		return this.outputStream;
	}

	private FSDataInputStream getInputStream() throws IOException {

		if (this.inputStream == null) {
			this.inputStream = this.fs.open(this.checkpointFile, this.buf.length);
		}

		return this.inputStream;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void implCloseChannel() throws IOException {

		if (this.outputStream != null) {
			this.outputStream.close();
			this.outputStream = null;
		}

		if (this.inputStream != null) {
			this.inputStream.close();
			this.inputStream = null;
		}
	}
}
