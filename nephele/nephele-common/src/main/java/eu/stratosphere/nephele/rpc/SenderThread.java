package eu.stratosphere.nephele.rpc;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.util.concurrent.ArrayBlockingQueue;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.minlog.Log;

final class SenderThread extends Thread {

	private final DatagramSocket socket;

	private volatile boolean shutdownRequested = false;

	private static final class SendingRequest {

		private final InetSocketAddress remoteSocketAddress;

		private final RPCMessage rpcMessage;

		private SendingRequest(final InetSocketAddress remoteSocketAddress, final RPCMessage rpcMessage) {
			this.remoteSocketAddress = remoteSocketAddress;
			this.rpcMessage = rpcMessage;
		}
	}

	private final ArrayBlockingQueue<SendingRequest> msgQueue = new ArrayBlockingQueue<SendingRequest>(128);

	SenderThread(final DatagramSocket socket) {
		super("RPC Sender Thread");

		this.socket = socket;
	}

	@Override
	public void run() {

		final Kryo kryo = RPCService.createKryoObject();
		final byte[] buf = new byte[RPCMessage.MAXIMUM_MSG_SIZE];
		final MemoryBackedOutputStream mbos = new MemoryBackedOutputStream(buf);

		while (!this.shutdownRequested) {

			SendingRequest sendingRequest = null;
			try {
				sendingRequest = this.msgQueue.take();
			} catch (InterruptedException ie) {
				if (this.shutdownRequested) {
					return;
				} else {
					continue;
				}
			}

			mbos.reset();
			final Output output = new Output(mbos);
			kryo.writeObject(output, new RPCEnvelope(sendingRequest.rpcMessage));
			output.close();
			mbos.close();

			final DatagramPacket datagramPacket = new DatagramPacket(mbos.getBuf(), mbos.getLen());
			datagramPacket.setSocketAddress(sendingRequest.remoteSocketAddress);

			try {
				this.socket.send(datagramPacket);
			} catch (IOException ioe) {
				Log.error("Shutting down sender thread due to error: ", ioe);
				return;
			}
		}

	}

	void sendMessage(final InetSocketAddress remoteSocketAddress, final RPCMessage rpcMessage)
			throws InterruptedException {

		this.msgQueue.put(new SendingRequest(remoteSocketAddress, rpcMessage));
	}

	void requestShutdown() {

		this.shutdownRequested = true;
		interrupt();
	}
}
