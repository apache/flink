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

	private long nextExpectedWritePosition = 0L;

	public FileChannelWrapper(final FileSystem fs, final Path checkpointFile, final int bufferSize,
			final short replication) {

		this.fs = fs;
		this.checkpointFile = checkpointFile;
		this.buf = new byte[bufferSize];
		this.replication = replication;
	}

	@Override
	public void force(boolean metaData) throws IOException {
		// TODO Auto-generated method stub
		System.out.println("force called");
	}

	@Override
	public FileLock lock(long position, long size, boolean shared) throws IOException {
		// TODO Auto-generated method stub

		System.out.println("lock called");

		return null;
	}

	@Override
	public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
		// TODO Auto-generated method stub

		System.out.println("map called");

		return null;
	}

	@Override
	public long position() throws IOException {
		// TODO Auto-generated method stub

		System.out.println("position called");

		return 0;
	}

	@Override
	public FileChannel position(long newPosition) throws IOException {
		// TODO Auto-generated method stub

		System.out.println("position2 called");

		return null;
	}

	@Override
	public int read(ByteBuffer dst) throws IOException {
		// TODO Auto-generated method stub

		System.out.println("read called");

		return 0;
	}

	@Override
	public int read(ByteBuffer dst, long position) throws IOException {
		// TODO Auto-generated method stub

		System.out.println("read2 called");

		return 0;
	}

	@Override
	public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
		// TODO Auto-generated method stub

		System.out.println("read3 called");

		return 0;
	}

	@Override
	public long size() throws IOException {
		// TODO Auto-generated method stub

		System.out.println("size called");

		return 0;
	}

	@Override
	public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {

		System.out.println("transferFrom called");

		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long transferTo(long position, long count, WritableByteChannel target) throws IOException {

		System.out.println("transferTo called");

		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public FileChannel truncate(long size) throws IOException {

		System.out.println("truncate called");

		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FileLock tryLock(long position, long size, boolean shared) throws IOException {

		System.out.println("tryLock called");

		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int write(ByteBuffer src) throws IOException {

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

	@Override
	public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {

		System.out.println("write3 called");

		// TODO Auto-generated method stub
		return 0;
	}

	private FSDataOutputStream getOutputStream() throws IOException {

		if (this.outputStream == null) {
			this.outputStream = this.fs.create(this.checkpointFile, false, this.buf.length, this.replication,
				this.fs.getDefaultBlockSize());
		}

		return this.outputStream;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void implCloseChannel() throws IOException {

		getOutputStream().close();
	}
}
