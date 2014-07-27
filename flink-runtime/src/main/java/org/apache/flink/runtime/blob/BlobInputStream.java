package org.apache.flink.runtime.blob;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;

final class BlobInputStream extends InputStream {

	private final InputStream wrappedInputStream;

	private final BlobKey blobKey;

	private final int bytesToReceive;

	private final MessageDigest md;

	private int bytesReceived;

	BlobInputStream(final InputStream wrappedInputStream, final BlobKey blobKey, final byte[] buf) throws IOException {

		this.wrappedInputStream = wrappedInputStream;
		this.blobKey = blobKey;
		this.bytesToReceive = BlobServer.readLength(buf, wrappedInputStream);
		if (this.bytesToReceive < 1) {
			// TODO: Improve this
			throw new FileNotFoundException();
		}

		this.md = (blobKey != null) ? BlobServer.createMessageDigest() : null;
	}

	private void throwEOFException() throws EOFException {
		throw new EOFException(String.format("Expected to read %d more bytes from stream", this.bytesToReceive
			- this.bytesReceived));
	}

	@Override
	public int read() throws IOException {

		if (this.bytesReceived == this.bytesToReceive) {
			return -1;
		}

		final int read = this.wrappedInputStream.read();
		if (read < 0) {
			throwEOFException();
		}

		++this.bytesReceived;

		if (this.md != null) {
			this.md.update((byte) read);
			if (this.bytesReceived == this.bytesToReceive) {
				final BlobKey computedKey = new BlobKey(this.md.digest());
				if (!computedKey.equals(this.blobKey)) {
					throw new IOException("Detected data corruption during transfer");
				}
			}
		}

		return read;
	}

	@Override
	public int read(byte b[]) throws IOException {
		return read(b, 0, b.length);
	}

	@Override
	public int read(byte b[], int off, int len) throws IOException {

		final int bytesMissing = this.bytesToReceive - this.bytesReceived;

		if (bytesMissing == 0) {
			return -1;
		}

		final int maxRecv = Math.min(len, bytesMissing);
		final int read = this.wrappedInputStream.read(b, off, maxRecv);
		if (read < 0) {
			throwEOFException();
		}

		this.bytesReceived += read;

		if (this.md != null) {
			this.md.update(b, off, read);
			if (this.bytesReceived == this.bytesToReceive) {
				final BlobKey computedKey = new BlobKey(this.md.digest());
				if (!computedKey.equals(this.blobKey)) {
					throw new IOException("Detected data corruption during transfer");
				}
			}
		}

		return read;
	}

	@Override
	public long skip(long n) throws IOException {

		return 0L;
	}

	@Override
	public int available() throws IOException {

		return 0;
	}

	@Override
	public void close() throws IOException {
		// This method does not do anything as the wrapped input stream may be used for multiple get operations.
	}

	public void mark(final int readlimit) {

		// Do not do anything here
	}

	@Override
	public void reset() throws IOException {

		throw new IOException("mark/reset not supported");
	}

	@Override
	public boolean markSupported() {

		return false;
	}
}
