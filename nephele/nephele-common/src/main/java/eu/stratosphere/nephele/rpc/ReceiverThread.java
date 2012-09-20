package eu.stratosphere.nephele.rpc;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.minlog.Log;

final class ReceiverThread extends Thread {

	private final RPCService rpcService;

	private final DatagramSocket socket;

	private volatile boolean shutdownRequested = false;

	ReceiverThread(final RPCService rpcService, final DatagramSocket socket) {
		super("RPC Receiver Thread");

		this.rpcService = rpcService;
		this.socket = socket;
	}

	@Override
	public void run() {

		final Kryo kryo = RPCService.createKryoObject();
		final byte[] buf = new byte[RPCMessage.MAXIMUM_MSG_SIZE];
		final DatagramPacket dp = new DatagramPacket(buf, buf.length);

		while (!this.shutdownRequested) {

			try {
				this.socket.receive(dp);
			} catch (SocketException se) {
				if (this.shutdownRequested) {
					return;
				}
				Log.error("Shutting down receiver thread due to error: ", se);
				return;
			} catch (IOException ioe) {
				Log.error("Shutting down receiver thread due to error: ", ioe);
				return;
			}

			final InetSocketAddress remoteSocketAddress = (InetSocketAddress) dp.getSocketAddress();
			final MemoryBackedInputStream mbis = new MemoryBackedInputStream(dp.getData(), dp.getOffset(),
				dp.getLength());
			final Input input = new Input(mbis);
			final RPCEnvelope envelope = kryo.readObject(input, RPCEnvelope.class);
			final RPCMessage msg = envelope.getRPCMessage();

			if (msg instanceof RPCRequest) {

				while (true) {

					try {
						this.rpcService.processIncomingRPCRequest(remoteSocketAddress, (RPCRequest) msg);
						break;
					} catch (InterruptedException e) {
						if (this.shutdownRequested) {
							return;
						} else {
							continue;
						}
					}
				}
			} else if (msg instanceof RPCResponse) {
				this.rpcService.processIncomingRPCResponse(remoteSocketAddress, (RPCResponse) msg);
			} else {
				this.rpcService.processIncomingRPCCleanup(remoteSocketAddress, (RPCCleanup) msg);
			}
		}

	}

	void requestShutdown() {

		this.shutdownRequested = true;
		interrupt();
	}
}
