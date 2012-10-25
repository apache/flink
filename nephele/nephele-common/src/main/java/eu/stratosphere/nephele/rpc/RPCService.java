/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.rpc;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.minlog.Log;

/**
 * This class implements a lightweight, UDP-based RPC service.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class RPCService {

	/**
	 * The default number of threads handling RPC requests.
	 */
	private static final int DEFAULT_NUM_RPC_HANDLERS = 1;

	/**
	 * The maximum number of retries in case the response does not arrive on time.
	 */
	private static final int RETRY_LIMIT = 300;

	/**
	 * Interval in which the background clean-up routine runs in milliseconds.
	 */
	private static final int CLEANUP_INTERVAL = 180000;

	/**
	 * The executor service managing the RPC handler threads.
	 */
	private final ExecutorService rpcHandlers;

	/**
	 * The UDP port this service is bound to.
	 */
	private final int rpcPort;

	/**
	 * The datagram socket used for the actual network communication.
	 */
	private final DatagramSocket socket;

	/**
	 * Background thread to wait for incoming data and dispatch it among the available RPC handler threads.
	 */
	private final ReceiverThread receiverThread;

	/**
	 * Stores whether the RPC service was requested to shut down.
	 */
	private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);

	/**
	 * The statistics module collects statistics on the operation of the RPC service.
	 */
	private final RPCStatistics statistics = new RPCStatistics();

	/**
	 * Periodic timer to handle clean-up tasks in the background.
	 */
	private final Timer cleanupTimer = new Timer();

	private final ConcurrentHashMap<String, RPCProtocol> callbackHandlers = new ConcurrentHashMap<String, RPCProtocol>();

	private final ConcurrentHashMap<Integer, RPCRequestMonitor> pendingRequests = new ConcurrentHashMap<Integer, RPCRequestMonitor>();

	private final ConcurrentHashMap<Integer, RPCRequest> requestsBeingProcessed = new ConcurrentHashMap<Integer, RPCRequest>();

	private final ConcurrentHashMap<Integer, CachedResponse> cachedResponses = new ConcurrentHashMap<Integer, CachedResponse>();

	private final ConcurrentHashMap<MultiPacketInputStreamKey, MultiPacketInputStream> incompleteInputStreams = new ConcurrentHashMap<MultiPacketInputStreamKey, MultiPacketInputStream>();

	private final List<Class<?>> kryoTypesToRegister;

	final ThreadLocal<Kryo> kryo = new ThreadLocal<Kryo>() {

		/**
		 * {@inheritDoc}
		 */
		@Override
		protected Kryo initialValue() {

			final Kryo kryo = new Kryo();
			if (kryoTypesToRegister != null) {
				kryo.setAutoReset(false);
				kryo.setRegistrationRequired(true);

				for (final Class<?> kryoType : kryoTypesToRegister) {
					kryo.register(kryoType);
				}
			}

			return kryo;
		}
	};

	/**
	 * This class implements the receiver thread which waits for incoming data and dispatch it among the available RPC
	 * handler threads.
	 * <p>
	 * This class is thread-safe.
	 * 
	 * @author warneke
	 */
	private static final class ReceiverThread extends Thread {

		/**
		 * Reference to the RPC service object.
		 */
		private final RPCService rpcService;

		/**
		 * The datagram socket to receive data from.
		 */
		private final DatagramSocket socket;

		/**
		 * Stores whether the thread has been requested to stop and shut down.
		 */
		private volatile boolean shutdownRequested = false;

		/**
		 * Constructs a new receiver thread.
		 * 
		 * @param rpcService
		 *        reference to the RPC service object
		 * @param socket
		 *        the datagram socket to receive data from
		 */
		private ReceiverThread(final RPCService rpcService, final DatagramSocket socket) {
			super("RPC Receiver Thread");

			this.rpcService = rpcService;
			this.socket = socket;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void run() {

			while (!this.shutdownRequested) {

				final byte[] buf = new byte[RPCMessage.MAXIMUM_MSG_SIZE + RPCMessage.METADATA_SIZE];
				final DatagramPacket dp = new DatagramPacket(buf, buf.length);

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
				final int length = dp.getLength() - RPCMessage.METADATA_SIZE;
				final byte[] dbbuf = dp.getData();
				final short numberOfPackets = byteArrayToShort(dbbuf, length + 2);

				if (numberOfPackets == 1) {
					this.rpcService.processIncomingRPCMessage(remoteSocketAddress, new Input(
						new SinglePacketInputStream(dbbuf, length)));
				} else {
					final short packetIndex = byteArrayToShort(dbbuf, length);
					final short fragmentationID = byteArrayToShort(dbbuf, length + 4);
					final MultiPacketInputStream mpis = this.rpcService.getIncompleteInputStream(remoteSocketAddress,
						fragmentationID, numberOfPackets);

					mpis.addPacket(packetIndex, dp);
					if (!mpis.isComplete()) {
						continue;
					}

					this.rpcService.removeIncompleteInputStream(remoteSocketAddress, fragmentationID);
					this.rpcService.processIncomingRPCMessage(remoteSocketAddress, new Input(mpis));
				}
			}
		}

		private void requestShutdown() {
			this.shutdownRequested = true;
			interrupted();
		}
	}

	private static final class CachedResponse {

		private final long creationTime;

		private final DatagramPacket[] packets;

		private CachedResponse(final long creationTime, final DatagramPacket[] packets) {
			this.creationTime = creationTime;
			this.packets = packets;
		}
	}

	private static final class RPCRequestMonitor {

		private RPCResponse rpcResponse = null;
	}

	private final class RPCInvocationHandler implements InvocationHandler {

		private final InetSocketAddress remoteSocketAddress;

		private final String interfaceName;

		private RPCInvocationHandler(final InetSocketAddress remoteSocketAddress, final String interfaceName) {
			this.remoteSocketAddress = remoteSocketAddress;
			this.interfaceName = interfaceName;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {

			final int messageID = (int) ((double) Integer.MIN_VALUE + (Math.random() * (double) Integer.MAX_VALUE * 2.0));
			final RPCRequest rpcRequest = new RPCRequest(messageID, this.interfaceName, method, args);

			return sendRPCRequest(this.remoteSocketAddress, rpcRequest);
		}

	}

	private final class CleanupTask extends TimerTask {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void run() {

			// Process the collected data
			statistics.processCollectedData();

			final long now = System.currentTimeMillis();
			final Iterator<Map.Entry<Integer, CachedResponse>> it = cachedResponses.entrySet().iterator();
			while (it.hasNext()) {

				final Map.Entry<Integer, CachedResponse> entry = it.next();
				final CachedResponse cachedResponse = entry.getValue();
				if (cachedResponse.creationTime + CLEANUP_INTERVAL < now) {
					it.remove();
				}
			}

			final Iterator<Map.Entry<MultiPacketInputStreamKey, MultiPacketInputStream>> it2 = incompleteInputStreams
				.entrySet().iterator();
			while (it2.hasNext()) {
				final Map.Entry<MultiPacketInputStreamKey, MultiPacketInputStream> entry = it2.next();
				final MultiPacketInputStream mpis = entry.getValue();
				if (mpis.getCreationTime() + CLEANUP_INTERVAL < now) {
					it2.remove();
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

		/**
		 * {@inheritDoc}
		 */
		@Override
		public int hashCode() {

			return this.socketAddress.hashCode() + this.fragmentationID;
		}
	}

	public RPCService(final int rpcPort, final int numRPCHandlers, final List<Class<?>> typesToRegister)
			throws IOException {

		this.rpcHandlers = Executors.newFixedThreadPool(numRPCHandlers);

		if (typesToRegister == null) {
			this.kryoTypesToRegister = null;
		} else {
			ArrayList<Class<?>> kryoTypesToRegister = new ArrayList<Class<?>>();
			addBasicRPCTypes(kryoTypesToRegister);
			kryoTypesToRegister.addAll(typesToRegister);
			this.kryoTypesToRegister = Collections.unmodifiableList(kryoTypesToRegister);
		}

		this.rpcPort = rpcPort;

		this.socket = new DatagramSocket(rpcPort);

		this.receiverThread = new ReceiverThread(this, this.socket);
		this.receiverThread.start();

		this.cleanupTimer.schedule(new CleanupTask(), CLEANUP_INTERVAL, CLEANUP_INTERVAL);
	}

	private static void addBasicRPCTypes(final List<Class<?>> typesToRegister) {

		typesToRegister.add(ArrayList.class);
		typesToRegister.add(AssertionError.class);
		typesToRegister.add(boolean[].class);
		typesToRegister.add(Class.class);
		typesToRegister.add(Class[].class);
		typesToRegister.add(IllegalArgumentException.class);
		typesToRegister.add(KryoException.class);
		typesToRegister.add(List.class);
		typesToRegister.add(Object[].class);
		typesToRegister.add(RPCEnvelope.class);
		typesToRegister.add(RPCRequest.class);
		typesToRegister.add(RPCReturnValue.class);
		typesToRegister.add(RPCCleanup.class);
		typesToRegister.add(RPCThrowable.class);
		typesToRegister.add(StackTraceElement[].class);
		typesToRegister.add(String[].class);
		typesToRegister.add(StringBuffer.class);
	}

	public RPCService() throws IOException {
		this(DEFAULT_NUM_RPC_HANDLERS, null);
	}

	public RPCService(final List<Class<?>> typesToRegister) throws IOException {
		this(DEFAULT_NUM_RPC_HANDLERS, typesToRegister);
	}

	public RPCService(final int numRPCHandlers, final List<Class<?>> typesToRegister) throws IOException {

		this.rpcHandlers = Executors.newFixedThreadPool(numRPCHandlers);

		if (typesToRegister == null) {
			this.kryoTypesToRegister = null;
		} else {
			ArrayList<Class<?>> kryoTypesToRegister = new ArrayList<Class<?>>();
			addBasicRPCTypes(kryoTypesToRegister);
			kryoTypesToRegister.addAll(typesToRegister);
			this.kryoTypesToRegister = Collections.unmodifiableList(kryoTypesToRegister);
		}

		this.rpcPort = -1;

		this.socket = new DatagramSocket();

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

	/**
	 * Sends an RPC request to the given {@link InetSocketAddress}.
	 * 
	 * @param remoteSocketAddress
	 *        the remote address to send the request to
	 * @param request
	 *        the RPC request to send
	 * @return the return value of the RPC call, possibly <code>null</code>
	 * @throws Throwable
	 *         any exception that is thrown by the remote receiver of the RPC call
	 */
	Object sendRPCRequest(final InetSocketAddress remoteSocketAddress, final RPCRequest request) throws Throwable {

		if (this.shutdownRequested.get()) {
			throw new IOException("Shutdown of RPC service has already been requested");
		}

		DatagramPacket[] packets = messageToPackets(remoteSocketAddress, request);
		final Integer messageID = Integer.valueOf(request.getMessageID());

		final RPCRequestMonitor requestMonitor = new RPCRequestMonitor();

		this.pendingRequests.put(messageID, requestMonitor);

		for (int i = 0; i < RETRY_LIMIT; ++i) {

			sendPackets(packets);

			RPCResponse rpcResponse;
			try {
				synchronized (requestMonitor) {

					if (requestMonitor.rpcResponse == null) {
						requestMonitor.wait(this.statistics.calculateTimeout(packets.length, i));
					}

					rpcResponse = requestMonitor.rpcResponse;
				}
			} catch (InterruptedException ie) {
				Log.debug("Caught interrupted exception while waiting for RPC request to complete: ", ie);
				return null;
			}

			// Check if response has arrived
			if (rpcResponse == null) {
				// Report timeout and resend message
				Log.debug("Timeout, retransmitting request " + request.getMessageID());
				continue;
			}

			// Request is no longer pending
			this.pendingRequests.remove(messageID);

			// Report the successful call to the statistics module
			this.statistics.reportSuccessfulCall(request.getMethodName(), packets.length, i);

			packets = messageToPackets(remoteSocketAddress, new RPCCleanup(request.getMessageID()));
			sendPackets(packets);

			if (rpcResponse instanceof RPCReturnValue) {
				return ((RPCReturnValue) rpcResponse).getRetVal();
			}
			throw ((RPCThrowable) rpcResponse).getThrowable();
		}

		this.pendingRequests.remove(messageID);

		throw new IOException("Unable to complete RPC of method " + request.getMethodName() + " on "
			+ remoteSocketAddress);
	}

	public void shutDown() {

		if (!this.shutdownRequested.compareAndSet(false, true)) {
			return;
		}

		// Request shutdown of receiver thread
		this.receiverThread.requestShutdown();

		this.socket.close();

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

		// Finally, process the last collected data
		this.statistics.processCollectedData();
	}

	void processIncomingRPCMessage(final InetSocketAddress remoteSocketAddress, final Input input) {

		final ThreadLocal<Kryo> threadLocalKryo = this.kryo;

		final Runnable runnable = new Runnable() {

			/**
			 * {@inheritDoc}
			 */
			@Override
			public void run() {

				final Kryo kryo = threadLocalKryo.get();
				kryo.reset();
				final RPCEnvelope envelope = kryo.readObject(input, RPCEnvelope.class);
				final RPCMessage msg = envelope.getRPCMessage();

				if (msg instanceof RPCRequest) {
					processIncomingRPCRequest(remoteSocketAddress, (RPCRequest) msg);
				} else if (msg instanceof RPCResponse) {
					processIncomingRPCResponse((RPCResponse) msg);
				} else {
					processIncomingRPCCleanup(remoteSocketAddress, (RPCCleanup) msg);
				}
			}
		};

		this.rpcHandlers.execute(runnable);
	}

	private void sendPackets(final DatagramPacket[] packets) throws IOException {

		try {
			for (int i = 0; i < packets.length; ++i) {
				this.socket.send(packets[i]);
			}
		} catch (SocketException se) {
			// Drop exception when the service has already been shut down
			if (!this.shutdownRequested.get()) {
				throw se;
			}
		}
	}

	private void processIncomingRPCRequest(final InetSocketAddress remoteSocketAddress, final RPCRequest rpcRequest) {

		final Integer messageID = Integer.valueOf(rpcRequest.getMessageID());

		if (this.requestsBeingProcessed.putIfAbsent(messageID, rpcRequest) != null) {
			Log.debug("Request " + rpcRequest.getMessageID() + " is already being processed at the moment");
			return;
		}

		final CachedResponse cachedResponse = this.cachedResponses.get(messageID);
		if (cachedResponse != null) {
			try {
				sendPackets(cachedResponse.packets);
			} catch (IOException e) {
				Log.error("Caught exception while trying to send RPC response: ", e);
			} finally {
				this.requestsBeingProcessed.remove(messageID);
			}
			return;
		}

		final RPCProtocol callbackHandler = callbackHandlers.get(rpcRequest.getInterfaceName());
		if (callbackHandler == null) {
			Log.error("Cannot find callback handler for protocol " + rpcRequest.getInterfaceName());
			this.requestsBeingProcessed.remove(messageID);
			return;
		}

		try {
			final Method method = callbackHandler.getClass().getMethod(rpcRequest.getMethodName(),
				rpcRequest.getParameterTypes());

			RPCResponse rpcResponse = null;
			try {
				final Object retVal = method.invoke(callbackHandler, rpcRequest.getArgs());
				rpcResponse = new RPCReturnValue(rpcRequest.getMessageID(), retVal);
			} catch (InvocationTargetException ite) {
				rpcResponse = new RPCThrowable(rpcRequest.getMessageID(), ite.getTargetException());
			}
			final DatagramPacket[] packets = messageToPackets(remoteSocketAddress, rpcResponse);
			cachedResponses.put(messageID, new CachedResponse(System.currentTimeMillis(), packets));

			sendPackets(packets);

		} catch (Exception e) {
			Log.error("Caught processing RPC request: ", e);
		} finally {
			this.requestsBeingProcessed.remove(messageID);
		}

	}

	private DatagramPacket[] messageToPackets(final InetSocketAddress remoteSocketAddress, final RPCMessage rpcMessage) {

		final MultiPacketOutputStream mpos = new MultiPacketOutputStream(RPCMessage.MAXIMUM_MSG_SIZE
			+ RPCMessage.METADATA_SIZE);
		final Kryo kryo = this.kryo.get();
		kryo.reset();

		final Output output = new Output(mpos);

		kryo.writeObject(output, new RPCEnvelope(rpcMessage));
		output.close();
		mpos.close();

		return mpos.createPackets(remoteSocketAddress);
	}

	/**
	 * Processes an incoming RPC response.
	 * 
	 * @param rpcResponse
	 *        the RPC response to be processed
	 */
	void processIncomingRPCResponse(final RPCResponse rpcResponse) {

		final Integer messageID = Integer.valueOf(rpcResponse.getMessageID());

		final RPCRequestMonitor requestMonitor = this.pendingRequests.get(messageID);

		// The caller has already timed out or received an earlier response
		if (requestMonitor == null) {
			return;
		}

		synchronized (requestMonitor) {
			requestMonitor.rpcResponse = rpcResponse;
			requestMonitor.notify();
		}
	}

	void processIncomingRPCCleanup(final InetSocketAddress remoteSocketAddress, final RPCCleanup rpcCleanup) {

		this.cachedResponses.remove(Integer.valueOf(rpcCleanup.getMessageID()));
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

	static short byteArrayToShort(final byte[] arr, final int offset) {

		return (short) (((arr[offset] << 8)) | ((arr[offset + 1] & 0xFF)));
	}

	static void shortToByteArray(final short val, final byte[] arr, final int offset) {

		arr[offset] = (byte) ((val & 0xFF00) >> 8);
		arr[offset + 1] = (byte) (val & 0x00FF);
	}
}
