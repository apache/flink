package org.apache.flink.api.common.io;

import org.apache.flink.core.fs.FSDataInputStream;

import java.io.IOException;
import java.io.InputStream;

/**
 * This class wraps an {@link java.io.InputStream} and exposes it as {@link org.apache.flink.core.fs.FSDataInputStream}.
 * <br>
 * <i>NB: {@link #seek(long)} and {@link #getPos()} are currently not supported.</i>
 */
public class InputStreamFSInputWrapper extends FSDataInputStream {

    private final InputStream inStream;

    private long pos = 0;

    public InputStreamFSInputWrapper(InputStream inStream) {
        this.inStream = inStream;
    }

    @Override
    public void seek(long desired) throws IOException {
        if (desired < this.pos) {
            throw new IllegalArgumentException("Wrapped InputStream: cannot search backwards.");
        } else if (desired == pos) {
            return;
        }

        this.inStream.skip(desired - pos);
        this.pos = desired;
    }

    @Override
    public long getPos() throws IOException {
        return this.pos;
    }

    @Override
    public int read() throws IOException {
        int read = inStream.read();
        if (read != -1) {
            this.pos++;
        }
        return read;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int numReadBytes = inStream.read(b, off, len);
        this.pos += numReadBytes;
        return numReadBytes;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }
}
