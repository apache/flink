package eu.stratosphere.nephele.rpc;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.minlog.Log;

public final class RPCService {

	private static final int DEFAULT_NUM_RPC_HANDLERS = 1;

	private static final int TIMEOUT = 1000;

	private static final int RETRY_LIMIT = 10;

	private static final int CLEANUP_INTERVAL = 20000;

	private final ExecutorService rpcHandlers;

	private final int rpcPort;

	private final DatagramSocket socket;

	private final SenderThread senderThread;

	private final ReceiverThread receiverThread;

	private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);

	private final Timer cleanupTimer = new Timer();

	private final ConcurrentHashMap<String, RPCProtocol> callbackHandlers = new ConcurrentHashMap<String, RPCProtocol>();

	private final ConcurrentHashMap<Integer, RPCRequest> pendingRequests = new ConcurrentHashMap<Integer, RPCRequest>();

	private final ConcurrentHashMap<Integer, RPCResponse> pendingResponses = new ConcurrentHashMap<Integer, RPCResponse>();

	private final ConcurrentHashMap<Integer, CachedResponse> cachedResponses = new ConcurrentHashMap<Integer, CachedResponse>();

	private final ConcurrentHashMap<MultiPacketInputStreamKey, MultiPacketInputStream> incompleteInputStreams = new ConcurrentHashMap<MultiPacketInputStreamKey, MultiPacketInputStream>();

	private static final class CachedResponse {

		private final long creationTime;

		private final RPCResponse rpcResponse;

		private CachedResponse(final long creationTime, final RPCResponse rpcResponse) {
			this.creationTime = creationTime;
			this.rpcResponse = rpcResponse;
		}
	}

	private final class RPCInvocationHandler implements InvocationHandler {

		private final InetSocketAddress remoteSocketAddress;

		private final String interfaceName;

		private RPCInvocationHandler(final InetSocketAddress remoteSocketAddress, final String interfaceName) {
			this.remoteSocketAddress = remoteSocketAddress;
			this.interfaceName = interfaceName;
		}

		@Override
		public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {

			final int requestID = (int) ((double) Integer.MIN_VALUE + (Math.random() * (double) Integer.MAX_VALUE * 2.0));
			final RPCRequest rpcRequest = new RPCRequest(requestID, this.interfaceName, method, args);

			return sendRPCRequest(this.remoteSocketAddress, rpcRequest);
		}

	}

	private final class CleanupTask extends TimerTask {

		@Override
		public void run() {

			final long now = System.currentTimeMillis();
			final Iterator<Map.Entry<Integer, CachedResponse>> it = cachedResponses.entrySet().iterator();
			while (it.hasNext()) {

				final Map.Entry<Integer, CachedResponse> entry = it.next();
				final CachedResponse cachedResponse = entry.getValue();
				if (cachedResponse.creationTime + CLEANUP_INTERVAL < now) {
					it.remove();
				}
			}
		}
	}

	private static final class MultiPacketInputStreamKey {

		private final SocketAddress socketAddress;

		private final short fragmentationID;

		private MultiPacketInputStreamKey(final SocketAddress socketAddress, final short fragmentationID) {
			this.socketAddress = socketAddress;
			this.fragmentationID = fragmentationID;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public boolean equals(final Object obj) {

			if (!(obj instanceof MultiPacketInputStreamKey)) {
				return false;
			}

			final MultiPacketInputStreamKey mpisk = (MultiPacketInputStreamKey) obj;

			if (this.fragmentationID != mpisk.fragmentationID) {
				return false;
			}

			if (!this.socketAddress.equals(mpisk.socketAddress)) {
				return false;
			}

			return true;
		}

		@Override
		public int hashCode() {

			return this.socketAddress.hashCode() + this.fragmentationID;
		}
	}

	static Kryo createKryoObject() {

		final Kryo kryo = new Kryo();
		kryo.register(RPCMessage.class);
		kryo.register(RPCRequest.class);

		return kryo;
	}

	public RPCService(final int rpcPort, final int numRPCHandlers) throws IOException {

		this.rpcHandlers = Executors.newFixedThreadPool(numRPCHandlers);

		this.rpcPort = rpcPort;

		this.socket = new DatagramSocket(rpcPort);

		this.senderThread = new SenderThread(this.socket);
		this.senderThread.start();

		this.receiverThread = new ReceiverThread(this, this.socket);
		this.receiverThread.start();

		this.cleanupTimer.schedule(new CleanupTask(), CLEANUP_INTERVAL, CLEANUP_INTERVAL);
	}

	public RPCService() throws IOException {
		this(DEFAULT_NUM_RPC_HANDLERS);
	}

	public RPCService(final int numRPCHandlers) throws IOException {

		this.rpcHandlers = Executors.newFixedThreadPool(numRPCHandlers);

		this.rpcPort = -1;

		this.socket = new DatagramSocket();

		this.senderThread = new SenderThread(this.socket);
		this.senderThread.start();

		this.receiverThread = new ReceiverThread(this, this.socket);
		this.receiverThread.start();

		this.cleanupTimer.schedule(new CleanupTask(), CLEANUP_INTERVAL, CLEANUP_INTERVAL);
	}

	public void setProtocolCallbackHandler(final Class<? extends RPCProtocol> protocol,
			final RPCProtocol callbackHandler) {

		if (this.callbackHandlers.putIfAbsent(protocol.getName(), callbackHandler) != null) {
			Log.error("There is already a protocol call back handler set for protocol " + protocol.getName());
		}

	}

	@SuppressWarnings("unchecked")
	public <T extends RPCProtocol> T getProxy(final InetSocketAddress remoteAddress, Class<T> protocol)
			throws IOException {

		final Class<?>[] interfaces = new Class<?>[1];
		interfaces[0] = protocol;
		return (T) java.lang.reflect.Proxy.newProxyInstance(RPCService.class.getClassLoader(), interfaces,
			new RPCInvocationHandler(remoteAddress, protocol.getName()));
	}

	Object sendRPCRequest(final InetSocketAddress remoteSocketAddress, final RPCRequest request) throws Throwable {

		if (this.shutdownRequested.get()) {
			throw new IOException("Shutdown of RPC service has already been requested");
		}

		final Integer requestID = Integer.valueOf(request.getRequestID());

		this.pendingRequests.put(requestID, request);

		for (int i = 0; i < RETRY_LIMIT; ++i) {

			this.senderThread.sendMessage(remoteSocketAddress, request);

			synchronized (request) {
				try {
					request.wait(TIMEOUT);
				} catch (InterruptedException ie) {
					// TODO: This is a bit of a hack, maybe we can find a more elegant solution later on
					throw new IOException(ie);
				}
			}

			// Check if response has arrived
			final RPCResponse rpcResponse = this.pendingResponses.remove(requestID);
			if (rpcResponse == null) {
				// Resend message
				continue;
			}

			// Request is no longer pending
			this.pendingRequests.remove(requestID);

			// Send clean up message
			this.senderThread.sendMessage(remoteSocketAddress, new RPCCleanup(request.getRequestID()));

			if (rpcResponse instanceof RPCReturnValue) {
				return ((RPCReturnValue) rpcResponse).getRetVal();
			}

			throw ((RPCThrowable) rpcResponse).getThrowable();
		}

		this.pendingRequests.remove(requestID);

		throw new IOException("Unable to complete RPC of method " + request.getMethodName() + " on "
			+ remoteSocketAddress);
	}

	public void shutDown() {

		if (!this.shutdownRequested.compareAndSet(false, true)) {
			return;
		}

		// Request shutdown of sender and receiver thread
		this.senderThread.requestShutdown();
		this.receiverThread.requestShutdown();

		this.socket.close();

		try {
			this.senderThread.join();
		} catch (InterruptedException ie) {
			Log.debug("Caught exception while waiting for sender thread to shut down: ", ie);
		}

		try {
			this.receiverThread.join();
		} catch (InterruptedException ie) {
			Log.debug("Caught exception while waiting for receiver thread to shut down: ", ie);
		}

		this.rpcHandlers.shutdown();

		try {
			this.rpcHandlers.awaitTermination(5000L, TimeUnit.MILLISECONDS);
		} catch (InterruptedException ie) {
			Log.debug("Caught exception while waiting for RPC handlers to finish: ", ie);
		}

		this.cleanupTimer.cancel();
	}

	void processIncomingRPCRequest(final InetSocketAddress remoteSocketAddress, final RPCRequest rpcRequest)
			throws InterruptedException {

		final Integer requestID = Integer.valueOf(rpcRequest.getRequestID());
		final CachedResponse cachedResponse = this.cachedResponses.get(requestID);
		if (cachedResponse != null) {
			this.senderThread.sendMessage(remoteSocketAddress, cachedResponse.rpcResponse);
			return;
		}

		final Runnable runnable = new Runnable() {

			@Override
			public void run() {

				final RPCProtocol callbackHandler = callbackHandlers.get(rpcRequest.getInterfaceName());
				if (callbackHandler == null) {
					Log.error("Cannot find callback handler for protocol " + rpcRequest.getInterfaceName());
					return;
				}

				Method method = null;
				try {
					method = callbackHandler.getClass().getMethod(rpcRequest.getMethodName(),
						rpcRequest.getParameterTypes());
				} catch (Exception e) {
					e.printStackTrace();
					Log.error("Error while processing incoming RPC request: ", e);
					return;
				}

				RPCResponse rpcResponse = null;
				try {
					final Object retVal = method.invoke(callbackHandler, rpcRequest.getArgs());
					rpcResponse = new RPCReturnValue(rpcRequest.getRequestID(), retVal);
				} catch (InvocationTargetException ite) {
					rpcResponse = new RPCThrowable(rpcRequest.getRequestID(), ite.getTargetException());
				} catch (Exception e) {
					e.printStackTrace();
					Log.error("Error while processing incoming RPC request: ", e);
					return;
				}

				cachedResponses.put(requestID, new CachedResponse(System.currentTimeMillis(), rpcResponse));
				try {
					senderThread.sendMessage(remoteSocketAddress, rpcResponse);
				} catch (InterruptedException e) {
					Log.error("Caught interrupted excetion while trying to send RPC response: ", e);
				}
			}
		};

		this.rpcHandlers.execute(runnable);
	}

	void processIncomingRPCResponse(final InetSocketAddress remoteSocketAddress, final RPCResponse rpcResponse) {

		final Integer requestID = Integer.valueOf(rpcResponse.getRequestID());

		final RPCRequest request = this.pendingRequests.get(requestID);
		if (request == null) {
			return;
		}

		this.pendingResponses.put(requestID, rpcResponse);

		synchronized (request) {
			request.notify();
		}
	}

	void processIncomingRPCCleanup(final InetSocketAddress remoteSocketAddress, final RPCCleanup rpcCleanup) {

		this.cachedResponses.remove(Integer.valueOf(rpcCleanup.getRequestID()));
	}

	public int getRPCPort() {

		return this.rpcPort;
	}

	MultiPacketInputStream getIncompleteInputStream(final SocketAddress socketAddress, final short fragmentationID,
			final short numberOfPackets) {

		final MultiPacketInputStreamKey key = new MultiPacketInputStreamKey(socketAddress, fragmentationID);
		MultiPacketInputStream mpis = this.incompleteInputStreams.get(key);
		if (mpis == null) {
			mpis = new MultiPacketInputStream(numberOfPackets);
			MultiPacketInputStream oldVal = this.incompleteInputStreams.putIfAbsent(key, mpis);
			if (oldVal != null) {
				mpis = oldVal;
			}
		}

		return mpis;
	}

	void removeIncompleteInputStream(final SocketAddress socketAddress, final short fragmentationID) {

		this.incompleteInputStreams.remove(new MultiPacketInputStreamKey(socketAddress, fragmentationID));
	}
}
