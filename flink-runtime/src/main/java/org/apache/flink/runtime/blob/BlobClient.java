package org.apache.flink.runtime.blob;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.security.MessageDigest;

import org.apache.flink.runtime.AbstractID;
import org.apache.flink.runtime.jobgraph.JobID;

public final class BlobClient implements Closeable {

	private Socket socket;

	public BlobClient(final int port) throws IOException {

		this.socket = new Socket();
		this.socket.connect(new InetSocketAddress("localhost", port));
	}

	private void sendPutHeader(final OutputStream outputStream, final JobID jobID, final String key, final byte[] buf)
			throws IOException {

		// Signal type of operation
		outputStream.write(BlobServer.PUT_OPERATION);

		// Check if PUT should be done in content-addressable manner
		if (jobID == null || key == null) {
			outputStream.write(1);
		} else {
			outputStream.write(0);
			// Send job ID
			final ByteBuffer bb = ByteBuffer.wrap(buf);
			jobID.write(bb);
			outputStream.write(buf);

			// Send the key
			byte[] keyBytes = key.getBytes(BlobServer.DEFAULT_CHARSET);
			BlobServer.writeLength(keyBytes.length, buf, outputStream);
			outputStream.write(keyBytes);
		}
	}

	private void sendPutFooter(final OutputStream outputStream) throws IOException {

		// Indicate there in no more data following
		outputStream.write(-1);
	}

	public BlobKey put(final byte[] value) throws IOException {

		return put(value, 0, value.length);
	}

	public BlobKey put(final byte[] value, final int offset, final int len) throws IOException {

		return putBuffer(null, null, value, offset, len);
	}

	public void put(final JobID jobId, final String key, final byte[] value) throws IOException {

		put(jobId, key, value, 0, value.length);
	}

	public void put(final JobID jobId, final String key, final byte[] value, final int offset, final int len)
			throws IOException {

		putBuffer(jobId, key, value, offset, len);
	}

	public void put(final JobID jobId, final String key, final InputStream inputStream) throws IOException {

		putInputStream(jobId, key, inputStream);
	}

	public BlobKey put(final InputStream inputStream) throws IOException {

		return putInputStream(null, null, inputStream);
	}

	private BlobKey putBuffer(final JobID jobId, final String key, final byte[] value, final int offset, final int len)
			throws IOException {

		final OutputStream os = this.socket.getOutputStream();
		final MessageDigest md = (jobId == null || key == null) ? BlobServer.createMessageDigest() : null;
		final byte[] buf = new byte[AbstractID.SIZE];

		// Send the PUT header
		sendPutHeader(os, jobId, key, buf);

		// Send the value in iterations of BUFFER_SIZE
		int remainingBytes = value.length;
		int bytesSent = 0;

		while (remainingBytes > 0) {

			final int bytesToSend = Math.min(BlobServer.BUFFER_SIZE, remainingBytes);
			BlobServer.writeLength(bytesToSend, buf, os);

			os.write(value, offset + bytesSent, bytesToSend);

			// Update the message digest if necessary
			if (md != null) {
				md.update(value, offset + bytesSent, bytesToSend);
			}

			remainingBytes -= bytesToSend;
			bytesSent += bytesToSend;
		}

		if (md == null) {
			return null;
		}

		// Receive blob key and compare
		final InputStream is = this.socket.getInputStream();
		final BlobKey localKey = new BlobKey(md.digest());
		final BlobKey remoteKey = BlobKey.readFromInputStream(is);

		if (!localKey.equals(remoteKey)) {
			throw new IOException("Detected data corruption during transfer");
		}

		return localKey;
	}

	private BlobKey putInputStream(final JobID jobId, final String key, final InputStream inputStream)
			throws IOException {

		final OutputStream os = this.socket.getOutputStream();
		final MessageDigest md = (jobId == null || key == null) ? BlobServer.createMessageDigest() : null;
		final byte[] buf = new byte[AbstractID.SIZE];
		final byte[] xferBuf = new byte[BlobServer.BUFFER_SIZE];

		// Send the PUT header
		sendPutHeader(os, jobId, key, buf);

		while (true) {

			final int read = inputStream.read(xferBuf);
			if (read < 0) {
				break;
			}
			if (read > 0) {
				BlobServer.writeLength(read, buf, os);
				os.write(xferBuf, 0, read);
				if (md != null) {
					md.update(xferBuf, 0, read);
				}
			}
		}

		// Send the PUT footer
		sendPutFooter(os);

		if (md == null) {
			return null;
		}

		// Receive blob key and compare
		final InputStream is = this.socket.getInputStream();
		final BlobKey localKey = new BlobKey(md.digest());
		final BlobKey remoteKey = BlobKey.readFromInputStream(is);

		if (!localKey.equals(remoteKey)) {
			throw new IOException("Detected data corruption during transfer");
		}

		return localKey;
	}

	public InputStream get(final JobID jobID, final String key) throws IOException {

		final OutputStream os = this.socket.getOutputStream();
		final byte[] buf = new byte[AbstractID.SIZE];

		// Send GET header
		sendGetHeader(os, jobID, key, null, buf);

		return new BlobInputStream(this.socket.getInputStream(), null, buf);
	}

	public InputStream get(final BlobKey blobKey) throws IOException {

		final OutputStream os = this.socket.getOutputStream();
		final byte[] buf = new byte[AbstractID.SIZE];

		// Send GET header
		sendGetHeader(os, null, null, blobKey, buf);

		return new BlobInputStream(this.socket.getInputStream(), blobKey, buf);
	}

	private void sendGetHeader(final OutputStream outputStream, final JobID jobID, final String key,
			final BlobKey key2, final byte[] buf) throws IOException {

		// Signal type of operation
		outputStream.write(BlobServer.GET_OPERATION);

		// Check if GET should be done in content-addressable manner
		if (jobID == null || key == null) {
			outputStream.write(1);
			key2.writeToOutputStream(outputStream);
		} else {
			outputStream.write(0);
			// Send job ID
			final ByteBuffer bb = ByteBuffer.wrap(buf);
			jobID.write(bb);
			outputStream.write(buf);

			// Send the key
			byte[] keyBytes = key.getBytes(BlobServer.DEFAULT_CHARSET);
			BlobServer.writeLength(keyBytes.length, buf, outputStream);
			outputStream.write(keyBytes);
		}
	}

	@Override
	public void close() throws IOException {

		this.socket.close();
	}
}
