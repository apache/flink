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

package org.apache.flink.api.java.io;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.rmi.AccessException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.UUID;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSink;

/**
 * This class provides a counterpart implementation for the
 * {@link RemoteCollectorOutputFormat}.
 */

public class RemoteCollectorImpl<T> extends UnicastRemoteObject implements
		RemoteCollector<T> {

	private static final long serialVersionUID = 1L;

	/**
	 * Instance of an implementation of a {@link RemoteCollectorConsumer}. This
	 * instance will get the records passed.
	 */

	private RemoteCollectorConsumer<T> consumer;
	
        /**
         * This list stores all created {@link Registry}s to unbind and unexport all
         * exposed {@link Remote} objects ({@link RemoteCollectorConsumer} in our
         * case) in the shutdown phase.
         */
	private static List<Registry> registries = new ArrayList<Registry>();

	/**
	 * This factory method creates an instance of the
	 * {@link RemoteCollectorImpl} and binds it in the local RMI
	 * {@link Registry}.
	 * 
	 * @param port
	 *            The port where the local colector is listening.
	 * @param consumer
	 *            The consumer instance.
	 * @param rmiId 
	 * 	          An ID to register the collector in the RMI registry.
	 */
	public static <T> void createAndBind(Integer port, RemoteCollectorConsumer<T> consumer, String rmiId) {
		RemoteCollectorImpl<T> collectorInstance = null;

		try {
			collectorInstance = new RemoteCollectorImpl<T>();

			Registry registry;

			registry = LocateRegistry.createRegistry(port);
			registry.bind(rmiId, collectorInstance);
			
			registries.add(registry);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (AlreadyBoundException e) {
			e.printStackTrace();
		}

		collectorInstance.setConsumer(consumer);
	}

	/**
	 * Writes a DataSet to a {@link RemoteCollectorConsumer} through an
	 * {@link RemoteCollector} remotely called from the
	 * {@link RemoteCollectorOutputFormat}.<br/>
	 * 
	 * @return The DataSink that writes the DataSet.
	 */
	public static <T> DataSink<T> collectLocal(DataSet<T> source,
			RemoteCollectorConsumer<T> consumer) {
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

		// create an ID for this output format instance
		String rmiId = String.format("%s-%s", RemoteCollectorOutputFormat.class.getName(), UUID.randomUUID());
		
		// create the local listening object and bind it to the RMI registry
		RemoteCollectorImpl.createAndBind(randomPort, consumer, rmiId);

		// create and configure the output format
		OutputFormat<T> remoteCollectorOutputFormat = new RemoteCollectorOutputFormat<T>(ip, randomPort, rmiId);

		// create sink
		return source.output(remoteCollectorOutputFormat);
	}

	/**
	 * Writes a DataSet to a local {@link Collection} through an
	 * {@link RemoteCollector} and a standard {@link RemoteCollectorConsumer}
	 * implementation remotely called from the
	 * {@link RemoteCollectorOutputFormat}.<br/>
	 * 
	 * @param source the source data set
	 * @param collection the local collection
	 */
	public static <T> void collectLocal(DataSet<T> source,
			Collection<T> collection) {
		final Collection<T> synchronizedCollection = Collections
				.synchronizedCollection(collection);
		collectLocal(source, new RemoteCollectorConsumer<T>() {
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
	public RemoteCollectorConsumer<T> getConsumer() {
		return this.consumer;
	}

	@Override
	public void setConsumer(RemoteCollectorConsumer<T> consumer) {
		this.consumer = consumer;
	}

	/**
	 * This method unbinds and unexports all exposed {@link Remote} objects
	 * 
	 * @throws AccessException
	 * @throws RemoteException
	 * @throws NotBoundException
	 */
	public static void shutdownAll() throws AccessException, RemoteException, NotBoundException {
		for (Registry registry : registries) {
			for (String id : registry.list()) {
				Remote remote = registry.lookup(id);
				registry.unbind(id);
				UnicastRemoteObject.unexportObject(remote, true);
			}

		}
	}
}
