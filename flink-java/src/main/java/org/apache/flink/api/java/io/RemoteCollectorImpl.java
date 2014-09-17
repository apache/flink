/**
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

package org.apache.flink.api.java.io;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.configuration.Configuration;

/**
 * This class provides a counterpart implementation for the
 * {@link RemoteCollectorOutputFormat}.
 */

public class RemoteCollectorImpl<T> extends UnicastRemoteObject implements
		IRemoteCollector<T> {

	private static final long serialVersionUID = 1L;

	/**
	 * Instance of an implementation of a {@link IRemoteCollectorConsumer}. This
	 * instance will get the records passed.
	 */

	private IRemoteCollectorConsumer<T> consumer;

	/**
	 * This factory method creates an instance of the
	 * {@link RemoteCollectorImpl} and binds it in the local RMI
	 * {@link Registry}.
	 * 
	 * @param port
	 *            The port where the local colector is listening.
	 * @param consumer
	 *            The consumer instance.
	 * @return
	 */
	public static <T> void createAndBind(Integer port,
			IRemoteCollectorConsumer<T> consumer) {
		RemoteCollectorImpl<T> collectorInstance = null;

		try {
			collectorInstance = new RemoteCollectorImpl<T>();

			Registry registry;

			registry = LocateRegistry.createRegistry(port);
			registry.bind(RemoteCollectorOutputFormat.ID, collectorInstance);
		} catch (Throwable t) {
			throw new RuntimeException(t);
		}

		collectorInstance.setConsumer(consumer);
	}

	/**
	 * Writes a DataSet to a {@link IRemoteCollectorConsumer} through an
	 * {@link IRemoteCollector} remotely called from the
	 * {@link RemoteCollectorOutputFormat}.<br/>
	 * 
	 * @return The DataSink that writes the DataSet.
	 */
	public static <T> DataSink<T> collectLocal(DataSet<T> source,
			IRemoteCollectorConsumer<T> consumer) {
		// if the RMI parameter was not set by the user make a "good guess"
		String ip = System.getProperty("java.rmi.server.hostname");
		if (ip == null) {
			Enumeration<NetworkInterface> networkInterfaces = null;
			try {
				networkInterfaces = NetworkInterface.getNetworkInterfaces();
			} catch (Throwable t) {
				throw new RuntimeException(t);
			}
			while (networkInterfaces.hasMoreElements()) {
				NetworkInterface networkInterface = (NetworkInterface) networkInterfaces
						.nextElement();
				Enumeration<InetAddress> inetAddresses = networkInterface
						.getInetAddresses();
				while (inetAddresses.hasMoreElements()) {
					InetAddress inetAddress = (InetAddress) inetAddresses
							.nextElement();
					if (!inetAddress.isLoopbackAddress()
							&& inetAddress instanceof Inet4Address) {
						ip = inetAddress.getHostAddress();
						System.setProperty("java.rmi.server.hostname", ip);
					}
				}
			}
		}

		// get some random free port
		Integer randomPort = 0;
		try {
			ServerSocket tmp = new ServerSocket(0);
			randomPort = tmp.getLocalPort();
			tmp.close();
		} catch (Throwable t) {
			throw new RuntimeException(t);
		}

		// create the local listening object and bind it to the RMI registry
		RemoteCollectorImpl.createAndBind(randomPort, consumer);

		// create and configure the output format
		OutputFormat<T> remoteCollectorOutputFormat = new RemoteCollectorOutputFormat<T>();

		Configuration remoteCollectorConfiguration = new Configuration();
		remoteCollectorConfiguration.setString(
				RemoteCollectorOutputFormat.REMOTE, ip);
		remoteCollectorConfiguration.setInteger(
				RemoteCollectorOutputFormat.PORT, randomPort);
		remoteCollectorOutputFormat.configure(remoteCollectorConfiguration);

		// create sink
		return source.output(remoteCollectorOutputFormat);
	}

	/**
	 * Writes a DataSet to a local {@link Collection} through an
	 * {@link IRemoteCollector} and a standard {@link IRemoteCollectorConsumer}
	 * implementation remotely called from the
	 * {@link RemoteCollectorOutputFormat}.<br/>
	 * 
	 * @param local
	 * @param port
	 * @param collection
	 */
	public static <T> void collectLocal(DataSet<T> source,
			Collection<T> collection) {
		final Collection<T> synchronizedCollection = Collections
				.synchronizedCollection(collection);
		collectLocal(source, new IRemoteCollectorConsumer<T>() {
			@Override
			public void collect(T element) {
				synchronizedCollection.add(element);
			}
		});
	}

	/**
	 * Necessary private default constructor.
	 * 
	 * @throws RemoteException
	 */
	private RemoteCollectorImpl() throws RemoteException {
		super();
	}

	/**
	 * This method is called by the remote to collect records.
	 */
	@Override
	public void collect(T element) throws RemoteException {
		this.consumer.collect(element);
	}

	@Override
	public IRemoteCollectorConsumer<T> getConsumer() {
		return this.consumer;
	}

	@Override
	public void setConsumer(IRemoteCollectorConsumer<T> consumer) {
		this.consumer = consumer;
	}
}