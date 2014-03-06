package eu.stratosphere.api.avro;

import java.io.Closeable;
import java.io.IOException;

import org.apache.avro.file.SeekableInput;

import eu.stratosphere.core.fs.FSDataInputStream;


/**
 * Code copy pasted from org.apache.avro.mapred.FSInput (which is Apache licensed as well)
 * 
 * The wrapper keeps track of the position in the data stream.
 */
public class FSDataInputStreamWrapper implements Closeable, SeekableInput {
	private final FSDataInputStream stream;
	private final long len;
	private long pos;

	public FSDataInputStreamWrapper(final FSDataInputStream stream, final int len) {
		this.stream = stream;
		this.len = len;
		this.pos = 0;
	}

	public long length() {
		return len;
	}

	public int read(byte[] b, int off, int len) throws IOException {
		int read;
		read = stream.read(b, off, len);
		pos += read;
		return read;
	}

	public void seek(long p) throws IOException {
		stream.seek(p);
		pos = p;
	}

	public long tell() throws IOException {
		return pos;
	}

	public void close() throws IOException {
		stream.close();
	}
}
