package eu.stratosphere.nephele.rpc;

import java.io.IOException;
import java.io.OutputStream;

final class MemoryBackedOutputStream extends OutputStream {

	private final byte[] buf;

	private int len = 0;

	MemoryBackedOutputStream(final byte[] buf) {
		this.buf = buf;
	}

	@Override
	public void write(final int b) throws IOException {

		if (this.len == this.buf.length) {
			throw new IOException("Insufficient buffer space");
		}

		this.buf[this.len++] = (byte) b;
	}

	@Override
	public void write(final byte[] b) throws IOException {

		write(b, 0, b.length);
	}

	@Override
	public void write(final byte[] b, final int off, final int len) throws IOException {

		if (this.len + len > this.buf.length) {
			throw new IOException("Insufficient buffer space");
		}

		System.arraycopy(b, off, this.buf, this.len, len);
		this.len += len;
	}

	@Override
	public void close() {
		// Nothing to do here
	}

	@Override
	public void flush() {
		// Nothing to do here
	}

	byte[] getBuf() {
		return this.buf;
	}

	int getLen() {
		return this.len;
	}

	void reset() {
		this.len = 0;
	}
}
