/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/**
 * This file is based on source code from the Hadoop Project (http://hadoop.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. 
 */

package org.apache.flink.runtime.ipc;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Map;

import javax.net.SocketFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.io.StringRecord;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.protocols.VersionedProtocol;
import org.apache.flink.runtime.net.NetUtils;
import org.apache.flink.types.JavaToValueConverter;
import org.apache.flink.types.Value;
import org.apache.flink.util.ClassUtils;


public class RPC {

	private static final Logger LOG = LoggerFactory.getLogger(RPC.class);

	private RPC() {}

	/** A method invocation, including the method name and its parameters. */
	private static class Invocation implements IOReadableWritable {

		private String methodName;

		private Class<?>[] parameterClasses;

		private Object[] parameters;

		@SuppressWarnings("unused")
		public Invocation() {
		}

		public Invocation(Method method, Object[] parameters) {
			this.methodName = method.getName();
			this.parameterClasses = method.getParameterTypes();
			this.parameters = parameters;
		}

		/** The name of the method invoked. */
		public String getMethodName() {
			return methodName;
		}

		/** The parameter classes. */
		public Class<?>[] getParameterClasses() {
			return parameterClasses;
		}

		/** The parameter instances. */
		public Object[] getParameters() {
			return parameters;
		}


		@SuppressWarnings("unchecked")
		public void read(DataInputView in) throws IOException {

			this.methodName = StringRecord.readString(in);
			this.parameters = new Object[in.readInt()];
			this.parameterClasses = new Class[parameters.length];

			for (int i = 0; i < parameters.length; i++) {

				// Read class name for parameter and try to get class to that name
				final String className = StringRecord.readString(in);
				try {
					parameterClasses[i] = ClassUtils.resolveClassPrimitiveAware(className);
				} 
				catch (ClassNotFoundException e) {
					throw new IOException(e);
				}

				// See if parameter is null
				if (in.readBoolean()) {
					IOReadableWritable value;
					try {
						final String parameterClassName = StringRecord.readString(in);
						final Class<? extends IOReadableWritable> parameterClass =
								(Class<? extends IOReadableWritable>) ClassUtils.resolveClassPrimitiveAware(parameterClassName);
						
						value = parameterClass.newInstance();
						parameters[i] = value;
					}
					catch (Exception e) {
						throw new IOException(e);
					}
					// Object will do everything else on its own
					value.read(in);
				} else {
					parameters[i] = null;
				}
			}
		}

		public void write(DataOutputView out) throws IOException {
			StringRecord.writeString(out, methodName);
			out.writeInt(parameterClasses.length);

			// at this point, type conversion should have happened
			for (int i = 0; i < parameterClasses.length; i++) {
				StringRecord.writeString(out, parameterClasses[i].getName());
				if (parameters[i] == null) {
					out.writeBoolean(false);
				} else {
					out.writeBoolean(true);
					StringRecord.writeString(out, parameters[i].getClass().getName());
					((IOReadableWritable) parameters[i]).write(out);
				}
			}
		}
		
		public void doTypeConversion() throws IOException {
			try {
				for (int i = 0; i < parameterClasses.length; i++) {
					if (!IOReadableWritable.class.isAssignableFrom(parameterClasses[i])) {
						try {
							parameters[i] = JavaToValueConverter.convertBoxedJavaType(parameters[i]);
						}
						catch (IllegalArgumentException e) {
							throw new IOException("Argument " + i + " of method " + methodName
									+ " is not a primitive type (or boxed primitive) and not of type IOReadableWriteable");
						}
					}
				}
			}
			catch (IOException e) {
				LOG.error(e.getMessage(), e);
				throw e;
			}
			catch (Exception e) {
				LOG.error(e.getMessage(), e);
				throw new IOException(e);
			}
		}
		
		public void undoTypeConversion() {
			for (int i = 0; i < parameterClasses.length; i++) {
				if (!IOReadableWritable.class.isAssignableFrom(parameterClasses[i])) {
					parameters[i] = JavaToValueConverter.convertValueType((Value) parameters[i]);
				}
			}
		}

		public String toString() {
			StringBuffer buffer = new StringBuffer();
			buffer.append(methodName);
			buffer.append("(");
			for (int i = 0; i < parameters.length; i++) {
				if (i != 0) {
					buffer.append(", ");
				}
				buffer.append(parameters[i]);
			}
			buffer.append(")");
			return buffer.toString();
		}

	}

	/* Cache a client using its socket factory as the hash key */
	static private class ClientCache {
		private Map<SocketFactory, Client> clients = new HashMap<SocketFactory, Client>();

		private synchronized Client getClient(SocketFactory factory) {
			// Construct & cache client. The configuration is only used for timeout,
			// and Clients have connection pools. So we can either (a) lose some
			// connection pooling and leak sockets, or (b) use the same timeout for all
			// configurations. Since the IPC is usually intended globally, not
			// per-job, we choose (a).
			Client client = clients.get(factory);
			if (client == null) {
				client = new Client(factory);
				clients.put(factory, client);
			} else {
				client.incCount();
			}
			return client;
		}

		/**
		 * Stop a RPC client connection
		 * A RPC client is closed only when its reference count becomes zero.
		 */
		private void stopClient(Client client) {
			synchronized (this) {
				client.decCount();
				if (client.isZeroReference()) {
					clients.remove(client.getSocketFactory());
				}
			}
			if (client.isZeroReference()) {
				client.stop();
			}
		}
	}

	private static ClientCache CLIENTS = new ClientCache();

	private static class Invoker implements InvocationHandler {
		private InetSocketAddress address;

		private Client client;

		private boolean isClosed = false;

		public Invoker(InetSocketAddress address, SocketFactory factory) {
			this.address = address;
			this.client = CLIENTS.getClient(factory);
		}

		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			Invocation invocation = new Invocation(method, args);
			invocation.doTypeConversion();
			
			Object retValue = this.client.call(invocation, this.address, method.getDeclaringClass());
			
			if (IOReadableWritable.class.isAssignableFrom(method.getReturnType())) {
				return retValue;
			}
			else {
				return JavaToValueConverter.convertValueType((Value) retValue);
			}
		}

		/* close the IPC client that's responsible for this invoker's RPCs */
		synchronized private void close() {
			if (!this.isClosed) {
				this.isClosed = true;
				CLIENTS.stopClient(this.client);
			}
		}
	}

	public static VersionedProtocol waitForProxy(Class<? extends VersionedProtocol> protocol, InetSocketAddress addr)
			throws IOException {
		return waitForProxy(protocol, addr, Long.MAX_VALUE);
	}

	/**
	 * Get a proxy connection to a remote server
	 * 
	 * @param protocol
	 *        protocol class
	 * @param addr
	 *        remote address
	 * @param timeout
	 *        time in milliseconds before giving up
	 * @return the proxy
	 * @throws IOException
	 *         if the far end through a RemoteException
	 */
	static <V extends VersionedProtocol> V waitForProxy(Class<V> protocol, InetSocketAddress addr,
			long timeout) throws IOException {
		long startTime = System.currentTimeMillis();
		IOException ioe;
		while (true) {
			try {
				return getProxy(protocol, addr);
			} catch (ConnectException se) { // namenode has not been started
				LOG.info("Server at " + addr + " not available yet, Zzzzz...");
				ioe = se;
			} catch (SocketTimeoutException te) { // namenode is busy
				LOG.info("Problem connecting to server: " + addr);
				ioe = te;
			}
			// check if timed out
			if (System.currentTimeMillis() - timeout >= startTime) {
				throw ioe;
			}

			// wait for retry
			try {
				Thread.sleep(1000);
			} catch (InterruptedException ie) {
				// IGNORE
			}
		}
	}

	/**
	 * Construct a client-side proxy object that implements the named protocol,
	 * talking to a server at the named address.
	 */
	public static <V extends VersionedProtocol> V getProxy(Class<V> protocol, InetSocketAddress addr,
			SocketFactory factory) throws IOException {

		@SuppressWarnings("unchecked")
		V proxy = (V) Proxy.newProxyInstance(protocol.getClassLoader(), new Class[] { protocol }, new Invoker(addr, factory));

		return proxy;
	}

	/**
	 * Construct a client-side proxy object with the default SocketFactory
	 */
	public static <V extends VersionedProtocol> V getProxy(Class<V> protocol, InetSocketAddress addr)
			throws IOException {

		return getProxy(protocol, addr, NetUtils.getDefaultSocketFactory());
	}

	/**
	 * Stop this proxy and release its invoker's resource
	 * 
	 * @param proxy
	 *        the proxy to be stopped
	 */
	public static void stopProxy(VersionedProtocol proxy) {
		if (proxy != null) {
			((Invoker) Proxy.getInvocationHandler(proxy)).close();
		}
	}

	/**
	 * Construct a server for a protocol implementation instance listening on a
	 * port and address.
	 */
	public static Server getServer(final Object instance, final String bindAddress, final int port,
			final int numHandlers) throws IOException {
		return new Server(instance, bindAddress, port, numHandlers);
	}

	/** An RPC Server. */
	public static class Server extends org.apache.flink.runtime.ipc.Server {
		private Object instance;

		/**
		 * Construct an RPC server.
		 * 
		 * @param instance
		 *        the instance whose methods will be called
		 * @param bindAddress
		 *        the address to bind on to listen for connection
		 * @param port
		 *        the port to listen for connections on
		 */
		public Server(Object instance, String bindAddress, int port)
																	throws IOException {
			this(instance, bindAddress, port, 1);
		}

		private static String classNameBase(String className) {
			String[] names = className.split("\\.", -1);
			if (names == null || names.length == 0) {
				return className;
			}
			return names[names.length - 1];
		}

		/**
		 * Construct an RPC server.
		 * 
		 * @param instance
		 *        the instance whose methods will be called
		 * @param bindAddress
		 *        the address to bind on to listen for connection
		 * @param port
		 *        the port to listen for connections on
		 * @param numHandlers
		 *        the number of method handler threads to run
		 */
		public Server(Object instance, String bindAddress, int port, int numHandlers) throws IOException {
			super(bindAddress, port, Invocation.class, numHandlers, classNameBase(instance.getClass().getName()));
			this.instance = instance;
		}

		public IOReadableWritable call(Class<?> protocol, IOReadableWritable param, long receivedTime)
				throws IOException {
			
			try {
				
				final Invocation call = (Invocation) param;
				call.undoTypeConversion();
				
				final Method method = protocol.getMethod(call.getMethodName(), call.getParameterClasses());
				method.setAccessible(true);

				final Object value = method.invoke((Object) instance, (Object[]) call.getParameters());

				if (IOReadableWritable.class.isAssignableFrom(method.getReturnType())) {
					return (IOReadableWritable) value;
				}
				else {
					try {
						return JavaToValueConverter.convertBoxedJavaType(value);
					}
					catch (IllegalArgumentException e) {
						throw new IOException("The return type of method " + method.getName()
								+ " is not a primitive type (or boxed primitive) and not of type IOReadableWriteable");
					}
				}

			} catch (InvocationTargetException e) {
				
				final Throwable target = e.getTargetException();
				if (target instanceof IOException) {
					throw (IOException) target;
				} else {
					final IOException ioe = new IOException(target.toString());
					ioe.setStackTrace(target.getStackTrace());
					throw ioe;
				}
			}
			catch (Throwable e) {
				final IOException ioe = new IOException(e.toString());
				ioe.setStackTrace(e.getStackTrace());
				throw ioe;
			}
		}
	}
}
