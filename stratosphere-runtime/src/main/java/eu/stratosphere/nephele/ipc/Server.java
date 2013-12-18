/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

/**
 * This file is based on source code from the Hadoop Project (http://hadoop.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. 
 */

package eu.stratosphere.nephele.ipc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.core.io.StringRecord;
import eu.stratosphere.core.protocols.VersionedProtocol;
import eu.stratosphere.util.ClassUtils;

/**
 * An abstract IPC service. IPC calls take a single {@link Writable} as a
 * parameter, and return a {@link Writable} as their value. A service runs on
 * a port and is defined by a parameter class and a value class.
 * 
 * @see Client
 */
public abstract class Server {

	public static final Log LOG = LogFactory.getLog(Server.class);

	private static final Class<?>[] EMPTY_ARRAY = new Class[] {};

	public static final ByteBuffer HEADER = ByteBuffer.wrap("crpc".getBytes());

	/**
	 * How many calls/handler are allowed in the queue.
	 */
	private static final int MAX_QUEUE_SIZE_PER_HANDLER = 100;

	private static final ThreadLocal<Server> SERVER = new ThreadLocal<Server>();

	private static final Map<String, Class<? extends VersionedProtocol>> PROTOCOL_CACHE = new ConcurrentHashMap<String, Class<? extends VersionedProtocol>>();

	static Class<? extends VersionedProtocol> getProtocolClass(String protocolName) throws ClassNotFoundException {

		Class<? extends VersionedProtocol> protocol = PROTOCOL_CACHE.get(protocolName);
		if (protocol == null) {
			protocol = (Class<? extends VersionedProtocol>) ClassUtils.getProtocolByName(protocolName);
			PROTOCOL_CACHE.put(protocolName, protocol);
		}
		return protocol;
	}

	/**
	 * Returns the server instance called under or null. May be called under {@link #call(Writable, long)}
	 * implementations, and under {@link Writable} methods of paramters and return values. Permits applications to
	 * access
	 * the server context.
	 */
	public static Server get() {
		return SERVER.get();
	}

	/**
	 * This is set to Call object before Handler invokes an RPC and reset
	 * after the call returns.
	 */
	private static final ThreadLocal<Call> CurCall = new ThreadLocal<Call>();

	/**
	 * Returns the remote side ip address when invoked inside an RPC
	 * Returns null incase of an error.
	 */
	public static InetAddress getRemoteIp() {
		Call call = CurCall.get();
		if (call != null) {
			return call.connection.socket.getInetAddress();
		}
		return null;
	}

	/**
	 * Returns remote address as a string when invoked inside an RPC.
	 * Returns null in case of an error.
	 */
	public static String getRemoteAddress() {
		InetAddress addr = getRemoteIp();
		return (addr == null) ? null : addr.getHostAddress();
	}

	private String bindAddress;

	private int port; // port we listen on

	private int handlerCount; // number of handler threads

	private Class<? extends IOReadableWritable> invocationClass; // invocation class to call

	private int maxIdleTime; // the maximum idle time after

	// which a client may be disconnected
	private int thresholdIdleConnections; // the number of idle connections

	// after which we will start
	// cleaning up idle
	// connections
	int maxConnectionsToNuke; // the max number of

	// connections to nuke
	// during a cleanup

	private int maxQueueSize;

	private int socketSendBufferSize;

	private final boolean tcpNoDelay; // if T then disable Nagle's Algorithm

	volatile private boolean running = true; // true while server runs

	private BlockingQueue<Call> callQueue; // queued calls

	private List<Connection> connectionList = Collections.synchronizedList(new LinkedList<Connection>());

	// maintain a list
	// of client connections
	private Listener listener = null;

	private Responder responder = null;

	private int numConnections = 0;

	private Handler[] handlers = null;

	/**
	 * A convenience method to bind to a given address and report
	 * better exceptions if the address is not a valid host.
	 * 
	 * @param socket
	 *        the socket to bind
	 * @param address
	 *        the address to bind to
	 * @param backlog
	 *        the number of connections allowed in the queue
	 * @throws BindException
	 *         if the address can't be bound
	 * @throws UnknownHostException
	 *         if the address isn't a valid host name
	 * @throws IOException
	 *         other random errors from bind
	 */
	public static void bind(ServerSocket socket, InetSocketAddress address, int backlog) throws IOException {
		try {
			socket.bind(address, backlog);
		} catch (BindException e) {
			BindException bindException = new BindException("Problem binding to " + address + " : " + e.getMessage());
			bindException.initCause(e);
			throw bindException;
		} catch (SocketException e) {
			// If they try to bind to a different host's address, give a better
			// error message.
			if ("Unresolved address".equals(e.getMessage())) {
				throw new UnknownHostException("Invalid hostname for server: " + address.getHostName());
			} else {
				throw e;
			}
		}
	}

	/** A call queued for handling. */
	private static class Call {
		private int id; // the client's call id

		private IOReadableWritable param; // the parameter passed

		private Connection connection; // connection to client

		private long timestamp; // the time received when response is null

		// the time served when response is not null
		private ByteBuffer response; // the response for this call

		public Call(int id, IOReadableWritable param, Connection connection) {
			this.id = id;
			this.param = param;
			this.connection = connection;
			this.timestamp = System.currentTimeMillis();
			this.response = null;
		}

		@Override
		public String toString() {
			return param.toString() + " from " + connection.toString();
		}

		public void setResponse(ByteBuffer response) {
			this.response = response;
		}
	}

	/** Listens on the socket. Creates jobs for the handler threads */
	private class Listener extends Thread {

		private ServerSocketChannel acceptChannel = null; // the accept channel

		private Selector selector = null; // the selector that we use for the server

		private InetSocketAddress address; // the address we bind at

		private Random rand = new Random();

		private long lastCleanupRunTime = 0; // the last time when a cleanup connec-

		// -tion (for idle connections) ran
		private long cleanupInterval = 10000; // the minimum interval between

		// two cleanup runs
		private int backlogLength = 128;

		private volatile boolean shutDown = false;

		public Listener()
							throws IOException {
			address = new InetSocketAddress(bindAddress, port);
			// Create a new server socket and set to non blocking mode
			acceptChannel = ServerSocketChannel.open();
			acceptChannel.configureBlocking(false);

			// Bind the server socket to the local host and port
			bind(acceptChannel.socket(), address, backlogLength);
			port = acceptChannel.socket().getLocalPort(); // Could be an ephemeral port
			// create a selector;
			selector = Selector.open();

			// Register accepts on the server socket with the selector.
			acceptChannel.register(selector, SelectionKey.OP_ACCEPT);
			this.setName("IPC Server listener on " + port);
			this.setDaemon(true);
		}

		/**
		 * cleanup connections from connectionList. Choose a random range
		 * to scan and also have a limit on the number of the connections
		 * that will be cleanedup per run. The criteria for cleanup is the time
		 * for which the connection was idle. If 'force' is true then all
		 * connections will be looked at for the cleanup.
		 */
		private void cleanupConnections(boolean force) {
			if (force || numConnections > thresholdIdleConnections) {
				long currentTime = System.currentTimeMillis();
				if (!force && (currentTime - lastCleanupRunTime) < cleanupInterval) {
					return;
				}
				int start = 0;
				int end = numConnections - 1;
				if (!force) {
					start = rand.nextInt() % numConnections;
					end = rand.nextInt() % numConnections;
					int temp;
					if (end < start) {
						temp = start;
						start = end;
						end = temp;
					}
				}
				int i = start;
				int numNuked = 0;
				while (i <= end) {
					Connection c;
					synchronized (connectionList) {
						try {
							c = connectionList.get(i);
						} catch (Exception e) {
							return;
						}
					}
					if (c.timedOut(currentTime)) {
						
						closeConnection(c);
						numNuked++;
						end--;
						c = null;
						if (!force && numNuked == maxConnectionsToNuke)
							break;
					} else
						i++;
				}
				lastCleanupRunTime = System.currentTimeMillis();
			}
		}

		@Override
		public void run() {
			LOG.info(getName() + ": starting");
			SERVER.set(Server.this);
			while (running) {
				SelectionKey key = null;
				try {
					selector.select();
					Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
					while (iter.hasNext()) {
						key = iter.next();
						iter.remove();
						try {
							if (key.isValid()) {
								if (key.isAcceptable())
									doAccept(key);
								else if (key.isReadable())
									doRead(key);
							}
						} catch (IOException e) {
						}
						key = null;
					}
				} catch (OutOfMemoryError e) {
					// we can run out of memory if we have too many threads
					// log the event and sleep for a minute and give
					// some thread(s) a chance to finish
					LOG.warn("Out of Memory in server select", e);
					closeCurrentConnection(key, e);
					cleanupConnections(true);
					try {
						Thread.sleep(60000);
					} catch (Exception ie) {
					}
				} catch (InterruptedException e) {
					if (running) { // unexpected -- log it
						LOG.info(getName() + " caught: " + e.toString());
					}
				} catch (Exception e) {
					closeCurrentConnection(key, e);
				}
				cleanupConnections(false);
			}
			LOG.info("Stopping " + this.getName());

			synchronized (this) {
				try {
					acceptChannel.close();
					selector.close();
				} catch (IOException e) {
				}

				selector = null;
				acceptChannel = null;

				// clean up all connections
				while (!connectionList.isEmpty()) {
					closeConnection(connectionList.remove(0));
				}
			}

			this.shutDown = true;
		}

		public boolean isShutDown() {
			return this.shutDown;
		}

		private void closeCurrentConnection(SelectionKey key, Throwable e) {
			if (key != null) {
				Connection c = (Connection) key.attachment();
				if (c != null) {
					closeConnection(c);
					c = null;
				}
			}
		}

		InetSocketAddress getAddress() {
			return (InetSocketAddress) acceptChannel.socket().getLocalSocketAddress();
		}

		void doAccept(SelectionKey key) throws IOException, OutOfMemoryError {
			Connection c = null;
			ServerSocketChannel server = (ServerSocketChannel) key.channel();
			// accept up to 10 connections
			for (int i = 0; i < 10; i++) {
				SocketChannel channel = server.accept();
				if (channel == null)
					return;

				channel.configureBlocking(false);
				channel.socket().setTcpNoDelay(tcpNoDelay);
				SelectionKey readKey = channel.register(selector, SelectionKey.OP_READ);
				c = new Connection(readKey, channel, System.currentTimeMillis());
				readKey.attach(c);
				synchronized (connectionList) {
					connectionList.add(numConnections, c);
					numConnections++;
				}
			}
		}

		void doRead(SelectionKey key) throws InterruptedException {
			int count = 0;
			Connection c = (Connection) key.attachment();
			if (c == null) {
				return;
			}
			c.setLastContact(System.currentTimeMillis());

			try {
				count = c.readAndProcess();
			} catch (InterruptedException ieo) {
				LOG.info(getName() + ": readAndProcess caught InterruptedException", ieo);
				throw ieo;
			} catch (Exception e) {
				LOG.info(getName() + ": readAndProcess threw exception " + e + ". Count of bytes read: " + count, e);
				count = -1; // so that the (count < 0) block is executed
			}
			if (count < 0) {
				closeConnection(c);
				c = null;
			} else {
				c.setLastContact(System.currentTimeMillis());
			}
		}

		synchronized void doStop() {
			if (selector != null) {
				selector.wakeup();
				Thread.yield();
			}
			if (acceptChannel != null) {
				try {
					acceptChannel.socket().close();
				} catch (IOException e) {
					LOG.info(getName() + ":Exception in closing listener socket. " + e);
				}
			}
		}
	}

	// Sends responses of RPC back to clients.
	private class Responder extends Thread {
		private Selector writeSelector;

		private int pending; // connections waiting to register

		final static int PURGE_INTERVAL = 900000; // 15mins

		private volatile boolean shutDown = false;

		Responder()
					throws IOException {
			this.setName("IPC Server Responder");
			this.setDaemon(true);
			writeSelector = Selector.open(); // create a selector
			pending = 0;
		}

		@Override
		public void run() {
			LOG.info(getName() + ": starting");
			SERVER.set(Server.this);
			long lastPurgeTime = 0; // last check for old calls.

			while (running) {
				try {
					waitPending(); // If a channel is being registered, wait.
					writeSelector.select(PURGE_INTERVAL);
					Iterator<SelectionKey> iter = writeSelector.selectedKeys().iterator();
					while (iter.hasNext()) {
						SelectionKey key = iter.next();
						iter.remove();
						try {
							if (key.isValid() && key.isWritable()) {
								doAsyncWrite(key);
							}
						} catch (IOException e) {
							LOG.info(getName() + ": doAsyncWrite threw exception " + e);
						}
					}
					long now = System.currentTimeMillis();
					if (now < lastPurgeTime + PURGE_INTERVAL) {
						continue;
					}
					lastPurgeTime = now;
					//
					// If there were some calls that have not been sent out for a
					// long time, discard them.
					//
					ArrayList<Call> calls;

					// get the list of channels from list of keys.
					synchronized (writeSelector.keys()) {
						calls = new ArrayList<Call>(writeSelector.keys().size());
						iter = writeSelector.keys().iterator();
						while (iter.hasNext()) {
							SelectionKey key = iter.next();
							Call call = (Call) key.attachment();
							if (call != null && key.channel() == call.connection.channel) {
								calls.add(call);
							}
						}
					}

					for (Call call : calls) {
						try {
							doPurge(call, now);
						} catch (IOException e) {
							LOG.warn("Error in purging old calls " + e);
						}
					}
				} catch (OutOfMemoryError e) {
					//
					// we can run out of memory if we have too many threads
					// log the event and sleep for a minute and give
					// some thread(s) a chance to finish
					//
					LOG.warn("Out of Memory in server select", e);
					try {
						Thread.sleep(60000);
					} catch (Exception ie) {
					}
				} catch (Exception e) {
					LOG.warn("Exception in Responder " + e.toString());
				}
			}
			LOG.info("Stopping " + this.getName());

			this.shutDown = true;
		}

		public boolean isShutDown() {
			return this.shutDown;
		}

		private void doAsyncWrite(SelectionKey key) throws IOException {
			Call call = (Call) key.attachment();
			if (call == null) {
				return;
			}
			if (key.channel() != call.connection.channel) {
				throw new IOException("doAsyncWrite: bad channel");
			}

			synchronized (call.connection.responseQueue) {
				if (processResponse(call.connection.responseQueue, false)) {
					try {
						key.interestOps(0);
					} catch (CancelledKeyException e) {
						/*
						 * The Listener/reader might have closed the socket.
						 * We don't explicitly cancel the key, so not sure if this will
						 * ever fire.
						 * This warning could be removed.
						 */
						LOG.warn("Exception while changing ops : " + e);
					}
				}
			}
		}

		//
		// Remove calls that have been pending in the responseQueue
		// for a long time.
		//
		private void doPurge(Call call, long now) throws IOException {
			LinkedList<Call> responseQueue = call.connection.responseQueue;
			synchronized (responseQueue) {
				Iterator<Call> iter = responseQueue.listIterator(0);
				while (iter.hasNext()) {
					call = iter.next();
					if (now > call.timestamp + PURGE_INTERVAL) {
						closeConnection(call.connection);
						break;
					}
				}
			}
		}

		// Processes one response. Returns true if there are no more pending
		// data for this channel.
		//
		private boolean processResponse(LinkedList<Call> responseQueue, boolean inHandler) throws IOException {
			boolean error = true;
			boolean done = false; // there is more data for this channel.
			int numElements = 0;
			Call call = null;
			try {
				synchronized (responseQueue) {
					//
					// If there are no items for this channel, then we are done
					//
					numElements = responseQueue.size();
					if (numElements == 0) {
						error = false;
						return true; // no more data for this channel.
					}
					//
					// Extract the first call
					//
					call = responseQueue.removeFirst();
					SocketChannel channel = call.connection.channel;
					
					//
					// Send as much data as we can in the non-blocking fashion
					//
					int numBytes = channelWrite(channel, call.response);
					if (numBytes < 0) {
						return true;
					}
					if (!call.response.hasRemaining()) {
						call.connection.decRpcCount();
						if (numElements == 1) { // last call fully processes.
							done = true; // no more data for this channel.
						} else {
							done = false; // more calls pending to be sent.
						}
					} else {
						//
						// If we were unable to write the entire response out, then
						// insert in Selector queue.
						//
						call.connection.responseQueue.addFirst(call);

						if (inHandler) {
							// set the serve time when the response has to be sent later
							call.timestamp = System.currentTimeMillis();

							incPending();
							try {
								// Wakeup the thread blocked on select, only then can the call
								// to channel.register() complete.
								writeSelector.wakeup();
								channel.register(writeSelector, SelectionKey.OP_WRITE, call);
							} catch (ClosedChannelException e) {
								// Its ok. channel might be closed else where.
								done = true;
							} finally {
								decPending();
							}
						}
					}
					error = false; // everything went off well
				}
			} finally {
				if (error && call != null) {
					LOG.warn(getName() + ", call " + call + ": output error");
					done = true; // error. no more data for this channel.
					closeConnection(call.connection);
				}
			}
			return done;
		}

		//
		// Enqueue a response from the application.
		//
		void doRespond(Call call) throws IOException {
			synchronized (call.connection.responseQueue) {
				call.connection.responseQueue.addLast(call);
				if (call.connection.responseQueue.size() == 1) {
					processResponse(call.connection.responseQueue, true);
				}
			}
		}

		private synchronized void incPending() { // call waiting to be enqueued.
			pending++;
		}

		private synchronized void decPending() { // call done enqueueing.
			pending--;
			notify();
		}

		private synchronized void waitPending() throws InterruptedException {
			while (pending > 0) {
				wait();
			}
		}
	}

	/** Reads calls from a connection and queues them for handling. */
	private class Connection {
		private boolean headerRead = false; // if the connection header that

		// follows version is read.
		private boolean protocolRead = false;

		private SocketChannel channel;

		private ByteBuffer data;

		private ByteBuffer dataLengthBuffer;

		private LinkedList<Call> responseQueue;

		private volatile int rpcCount = 0; // number of outstanding rpcs

		private long lastContact;

		private int dataLength;

		private Socket socket;

		// Cache the remote host & port info so that even if the socket is
		// disconnected, we can say where it used to connect to.
		private String hostAddress;

		private int remotePort;

		ConnectionHeader header = new ConnectionHeader();

		Class<? extends VersionedProtocol> protocol;

		public Connection(SelectionKey key, SocketChannel channel, long lastContact) {
			this.channel = channel;
			this.lastContact = lastContact;
			this.data = null;
			this.dataLengthBuffer = ByteBuffer.allocate(4);
			this.socket = channel.socket();
			InetAddress addr = socket.getInetAddress();
			if (addr == null) {
				this.hostAddress = "*Unknown*";
			} else {
				this.hostAddress = addr.getHostAddress();
			}
			this.remotePort = socket.getPort();
			this.responseQueue = new LinkedList<Call>();
			if (socketSendBufferSize != 0) {
				try {
					socket.setSendBufferSize(socketSendBufferSize);
				} catch (IOException e) {
					LOG.warn("Connection: unable to set socket send buffer size to " + socketSendBufferSize);
				}
			}
		}

		@Override
		public String toString() {
			return getHostAddress() + ":" + remotePort;
		}

		public String getHostAddress() {
			return hostAddress;
		}

		public void setLastContact(long lastContact) {
			this.lastContact = lastContact;
		}

		/* Return true if the connection has no outstanding rpc */
		private boolean isIdle() {
			return rpcCount == 0;
		}

		/* Decrement the outstanding RPC count */
		private void decRpcCount() {
			rpcCount--;
		}

		/* Increment the outstanding RPC count */
		private void incRpcCount() {
			rpcCount++;
		}

		private boolean timedOut(long currentTime) {
			if (isIdle() && currentTime - lastContact > maxIdleTime)
				return true;
			return false;
		}

		public int readAndProcess() throws IOException, InterruptedException {
			while (true) {
				/*
				 * Read at most one RPC. If the header is not read completely yet
				 * then iterate until we read first RPC or until there is no data left.
				 */
				int count = -1;
				if (dataLengthBuffer.remaining() > 0) {
					count = channelRead(channel, dataLengthBuffer);
					if (count < 0 || dataLengthBuffer.remaining() > 0)
						return count;
				}

				if (!headerRead) {
					dataLengthBuffer.flip();
					if (!HEADER.equals(dataLengthBuffer)) {
						// Warning is ok since this is not supposed to happen.
						LOG.warn("Incorrect header from " + hostAddress + ":" + remotePort);
						return -1;
					}
					dataLengthBuffer.clear();
					headerRead = true;
					continue;
				}

				if (data == null) {
					dataLengthBuffer.flip();
					dataLength = dataLengthBuffer.getInt();

					if (dataLength == Client.PING_CALL_ID) {
						dataLengthBuffer.clear();
						return 0; // ping message
					}
					data = ByteBuffer.allocate(dataLength);
					incRpcCount(); // Increment the rpc count
				}

				count = channelRead(channel, data);

				if (data.remaining() == 0) {
					dataLengthBuffer.clear();
					data.flip();
					if (protocolRead) {
						processData();
						data = null;
						return count;
					} else {
						processProtocol();
						protocolRead = true;
						data = null;

						// Authorizitation is intenionally left out

						continue;
					}
				}
				return count;
			}
		}

		// / Reads the connection header following version
		private void processProtocol() throws IOException {
			DataInputStream in = new DataInputStream(new ByteArrayInputStream(data.array()));
			header.read(in);
			try {
				String protocolClassName = header.getProtocol();
				if (protocolClassName != null) {
					protocol = getProtocolClass(header.getProtocol());
				}
			} catch (ClassNotFoundException cnfe) {
				LOG.error(cnfe);
				throw new IOException("Unknown protocol: " + header.getProtocol());
			}

		}

		private void processData() throws IOException, InterruptedException {
			DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data.array()));
			int id = dis.readInt(); // try to read an id


			IOReadableWritable invocation = newInstance(invocationClass); // read param
			invocation.read(dis);

			Call call = new Call(id, invocation, this);
			callQueue.put(call); // queue the call; maybe blocked here
		}

		private synchronized void close() throws IOException {
			data = null;
			dataLengthBuffer = null;
			if (!channel.isOpen())
				return;
			try {
				socket.shutdownOutput();
			} catch (Exception e) {
			}
			if (channel.isOpen()) {
				try {
					channel.close();
				} catch (Exception e) {
				}
			}
			try {
				socket.close();
			} catch (Exception e) {
			}
		}
	}

	/** Handles queued calls . */
	private class Handler extends Thread {

		private volatile boolean shutDown = false;

		public Handler(int instanceNumber) {
			this.setDaemon(true);
			this.setName("IPC Server handler " + instanceNumber + " on " + port);
		}

		@Override
		public void run() {
			LOG.info(getName() + ": starting");
			SERVER.set(Server.this);
			ByteArrayOutputStream buf = new ByteArrayOutputStream(10240);
			while (running) {
				try {
					final Call call = callQueue.take(); // pop the queue; maybe blocked here

					String errorClass = null;
					String error = null;
					IOReadableWritable value = null;

					CurCall.set(call);

					value = call(call.connection.protocol, call.param, call.timestamp);

					CurCall.set(null);

					setupResponse(buf, call, (error == null) ? Status.SUCCESS : Status.ERROR, value, errorClass, error);
					responder.doRespond(call);
				} catch (InterruptedException e) {
					if (running) { // unexpected -- log it
						LOG.info(getName() + " caught: ", e);
					}
				} catch (Exception e) {
					LOG.info(getName() + " caught: ", e);
				}
			}
			LOG.info(getName() + ": exiting");

			this.shutDown = true;
		}

		public boolean isShutDown() {

			return this.shutDown;
		}
	}

	protected Server(String bindAddress, int port, Class<? extends IOReadableWritable> paramClass, int handlerCount)
																													throws IOException {
		this(bindAddress, port, paramClass, handlerCount, Integer.toString(port));
	}

	/**
	 * Constructs a server listening on the named port and address. Parameters passed must
	 * be of the named class. The <code>handlerCount</handlerCount> determines
	 * the number of handler threads that will be used to process calls.
	 */
	protected Server(String bindAddress, int port, Class<? extends IOReadableWritable> invocationClass,
			int handlerCount, String serverName)
												throws IOException {
		this.bindAddress = bindAddress;
		this.port = port;
		this.invocationClass = invocationClass;
		this.handlerCount = handlerCount;
		this.socketSendBufferSize = 0;
		this.maxQueueSize = handlerCount * MAX_QUEUE_SIZE_PER_HANDLER;
		this.callQueue = new LinkedBlockingQueue<Call>(maxQueueSize);
		this.maxIdleTime = 2 * 1000;
		this.maxConnectionsToNuke = 10;
		this.thresholdIdleConnections = 4000;

		// Start the listener here and let it bind to the port
		listener = new Listener();
		this.port = listener.getAddress().getPort();
		this.tcpNoDelay = false;

		// Create the responder here
		responder = new Responder();
	}

	private void closeConnection(Connection connection) {
		synchronized (connectionList) {
			if (connectionList.remove(connection))
				numConnections--;
		}
		try {
			connection.close();
		} catch (IOException e) {
		}
	}

	/**
	 * Setup response for the IPC Call.
	 * 
	 * @param response
	 *        buffer to serialize the response into
	 * @param call
	 *        {@link Call} to which we are setting up the response
	 * @param status
	 *        {@link Status} of the IPC call
	 * @param rv
	 *        return value for the IPC Call, if the call was successful
	 * @param errorClass
	 *        error class, if the the call failed
	 * @param error
	 *        error message, if the call failed
	 * @throws IOException
	 */
	private void setupResponse(ByteArrayOutputStream response, Call call, Status status, IOReadableWritable rv,
			String errorClass, String error) throws IOException {
		response.reset();
		DataOutputStream out = new DataOutputStream(response);
		out.writeInt(call.id); // write call id
		out.writeInt(status.state); // write status

		if (status == Status.SUCCESS) {
			if (rv == null) {
				out.writeBoolean(false);
			} else {
				out.writeBoolean(true);
				StringRecord.writeString(out, rv.getClass().getName());
				rv.write(out);
			}

		} else {
			StringRecord.writeString(out, errorClass);
			StringRecord.writeString(out, error);
		}
		call.setResponse(ByteBuffer.wrap(response.toByteArray()));
	}

	/** Sets the socket buffer size used for responding to RPCs */
	public void setSocketSendBufSize(int size) {
		this.socketSendBufferSize = size;
	}

	/** Starts the service. Must be called before any calls will be handled. */
	public synchronized void start() throws IOException {
		responder.start();
		listener.start();
		handlers = new Handler[handlerCount];

		for (int i = 0; i < handlerCount; i++) {
			handlers[i] = new Handler(i);
			handlers[i].start();
		}
	}

	/** Stops the service. No new calls will be handled after this is called. */
	public synchronized void stop() {
		LOG.info("Stopping server on " + port);
		running = false;
		if (handlers != null) {
			for (int i = 0; i < handlerCount; i++) {
				if (handlers[i] != null) {
					handlers[i].interrupt();
				}
			}
		}
		listener.interrupt();
		listener.doStop();
		responder.interrupt();
		notifyAll();

		// Wait until shut down of handlers is complete
		if (this.handlers != null) {

			while (true) {

				int i = 0;
				for (; i < this.handlerCount; i++) {
					if (this.handlers[i] != null) {
						if (!this.handlers[i].isShutDown()) {
							break;
						}
					}
				}

				if (i < this.handlerCount) {
					try {
						wait(100);
					} catch (InterruptedException e) {
						break;
					}
				} else {
					// exit while loop
					break;
				}
			}
		}

		// Wait until shut down of responder is complete
		while (!this.responder.isShutDown()) {
			try {
				wait(100);
			} catch (InterruptedException e) {
				break;
			}
		}

		// Wait until shut down of listener is complete
		while (!this.listener.isShutDown()) {
			try {
				wait(100);
			} catch (InterruptedException e) {
				break;
			}
		}
	}

	/**
	 * Wait for the server to be stopped.
	 * Does not wait for all subthreads to finish.
	 * See {@link #stop()}.
	 */
	public synchronized void join() throws InterruptedException {
		while (running) {
			wait();
		}
	}

	/**
	 * Return the socket (ip+port) on which the RPC server is listening to.
	 * 
	 * @return the socket (ip+port) on which the RPC server is listening to.
	 */
	public synchronized InetSocketAddress getListenerAddress() {
		return listener.getAddress();
	}

	/** Called for each call. */
	public abstract IOReadableWritable call(Class<?> protocol, IOReadableWritable param, long receiveTime)
			throws IOException;

	/**
	 * The number of open RPC conections
	 * 
	 * @return the number of open rpc connections
	 */
	public int getNumOpenConnections() {
		return numConnections;
	}

	/**
	 * The number of rpc calls in the queue.
	 * 
	 * @return The number of rpc calls in the queue.
	 */
	public int getCallQueueLen() {
		return callQueue.size();
	}

	/**
	 * When the read or write buffer size is larger than this limit, i/o will be
	 * done in chunks of this size. Most RPC requests and responses would be
	 * be smaller.
	 */
	private static int NIO_BUFFER_LIMIT = 8 * 1024; // should not be more than 64KB.

	/**
	 * This is a wrapper around {@link WritableByteChannel#write(ByteBuffer)}.
	 * If the amount of data is large, it writes to channel in smaller chunks.
	 * This is to avoid jdk from creating many direct buffers as the size of
	 * buffer increases. This also minimizes extra copies in NIO layer
	 * as a result of multiple write operations required to write a large
	 * buffer.
	 * 
	 * @see WritableByteChannel#write(ByteBuffer)
	 */
	private static int channelWrite(WritableByteChannel channel, ByteBuffer buffer) throws IOException {

		return (buffer.remaining() <= NIO_BUFFER_LIMIT) ? channel.write(buffer) : channelIO(null, channel, buffer);
	}

	/**
	 * This is a wrapper around {@link ReadableByteChannel#read(ByteBuffer)}.
	 * If the amount of data is large, it writes to channel in smaller chunks.
	 * This is to avoid jdk from creating many direct buffers as the size of
	 * ByteBuffer increases. There should not be any performance degredation.
	 * 
	 * @see ReadableByteChannel#read(ByteBuffer)
	 */
	private static int channelRead(ReadableByteChannel channel, ByteBuffer buffer) throws IOException {

		return (buffer.remaining() <= NIO_BUFFER_LIMIT) ? channel.read(buffer) : channelIO(channel, null, buffer);
	}

	/**
	 * Helper for {@link #channelRead(ReadableByteChannel, ByteBuffer)} and
	 * {@link #channelWrite(WritableByteChannel, ByteBuffer)}. Only
	 * one of readCh or writeCh should be non-null.
	 * 
	 * @see #channelRead(ReadableByteChannel, ByteBuffer)
	 * @see #channelWrite(WritableByteChannel, ByteBuffer)
	 */
	private static int channelIO(ReadableByteChannel readCh, WritableByteChannel writeCh, ByteBuffer buf)
			throws IOException {

		int originalLimit = buf.limit();
		int initialRemaining = buf.remaining();
		int ret = 0;

		while (buf.remaining() > 0) {
			try {
				int ioSize = Math.min(buf.remaining(), NIO_BUFFER_LIMIT);
				buf.limit(buf.position() + ioSize);

				ret = (readCh == null) ? writeCh.write(buf) : readCh.read(buf);

				if (ret < ioSize) {
					break;
				}

			} finally {
				buf.limit(originalLimit);
			}
		}

		int nBytes = initialRemaining - buf.remaining();
		return (nBytes > 0) ? nBytes : ret;
	}

	public static <T> T newInstance(Class<T> theClass) {
		T result;
		Constructor<T> meth = null;
		try {
			meth = theClass.getDeclaredConstructor(EMPTY_ARRAY);
			meth.setAccessible(true);
			result = meth.newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		return result;
	}
}
