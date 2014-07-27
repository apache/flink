package org.apache.flink.runtime.blob;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;

import org.apache.flink.runtime.AbstractID;
import org.apache.flink.runtime.jobgraph.JobID;

public final class BlobClient implements Closeable {

	private Socket socket;

	public BlobClient(final int port) throws IOException {

		this.socket = new Socket();
		this.socket.connect(new InetSocketAddress("localhost", port));
	}

	public void put(final JobID jobId, final String key, final byte[] value)
			throws IOException {

		put(jobId, key, value, 0, value.length);
	}

	public void put(final JobID jobId, final String key, final byte[] value,
			final int offset, final int len) throws IOException {

		final OutputStream os = this.socket.getOutputStream();

		// Signal type of operation
		os.write(BlobServer.PUT_OPERATION);

		// Send job ID
		final byte[] buf = new byte[AbstractID.SIZE];
		final ByteBuffer bb = ByteBuffer.wrap(buf);
		jobId.write(bb);
		os.write(buf);

		// Send the key
		byte[] keyBytes = key.getBytes(BlobServer.DEFAULT_CHARSET);
		BlobServer.writeLength(keyBytes.length, buf, os);
		os.write(keyBytes);

		// Send the length of the value
		BlobServer.writeLength(len, buf, os);

		// Send the value
		os.write(value, offset, len);
	}

	@Override
	public void close() throws IOException {

		this.socket.close();
	}
}
