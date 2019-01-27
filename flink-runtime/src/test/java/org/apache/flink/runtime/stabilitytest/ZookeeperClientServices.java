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

package org.apache.flink.runtime.stabilitytest;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.util.ZooKeeperUtils.ZkClientACLMode;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.imps.DefaultACLProvider;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.runtime.stabilitytest.Constants.LOG_PREFIX_DEFAULT;

/**
 * Auxiliary class for stability test, it is responsible for the connection with zookeeper
 * and the listen on zookeeper path nodes.
 */
public final class ZookeeperClientServices implements AutoCloseable, Cloneable {
	private static final Logger LOG = LoggerFactory.getLogger(ZookeeperClientServices.class);

	/** Connection to the used ZooKeeper quorum. */
	private static volatile CuratorFramework client;

	/** Curator recipe to watch changes of a specific ZooKeeper node. */
	private static final ConcurrentHashMap<NodeRetrievalListener, NodeChangeListener> listenerMap = new ConcurrentHashMap<NodeRetrievalListener, NodeChangeListener>();

	/** Current state of the services. */
	private static final AtomicReference<State> state = new AtomicReference<State>(State.STOPPED);

	/** The counter for registering and unregistering listener. */
	private static final AtomicInteger changingListenrCounter = new AtomicInteger(0);

	/** The connection state to ZK. */
	private static volatile ConnectionState connState = ConnectionState.LOST;

	/** Executor for listener's callback execution, eg. the ZK connection state listen. */
	private static volatile ScheduledThreadPoolExecutor listenerCallbackExecutor = null;

	/** Listener which will be notified about connection status changes. */
	private static final ConnectionStateListener connectionStateListener = new ConnectionStateListener() {
		@Override
		public void stateChanged(CuratorFramework client, ConnectionState newState) {
			handleStateChange(newState);
		}
	};

	/** Parameters for retry to start or stop. */
	private static final long waitStartOrStopMs = 50;
	private static final int maxRetryStartOrStop = 600;


	/**
	 * Register ZookeeperClientServices to the shutdown manager
	 */
	static {
		ShutdownManager.register(new ZookeeperClientServices());
	}

	/**
	 *
	 */
	private ZookeeperClientServices() {

	}

	/**
	 * Starts a {@link CuratorFramework} instance and connects it to the given ZooKeeper
	 * quorum.
	 *
	 * @param configuration {@link Configuration} object containing the configuration values
	 * @param isWaitedUntilConnected
	 */
	public static void start(Configuration configuration, boolean isWaitedUntilConnected) throws Throwable {
		if (state.get() == State.STARTED) {
			return;
		}

		while (!state.compareAndSet(State.STOPPED, State.STARTING)) {
			if (state.get() == State.STARTED) {
				return;
			}
		}

		try {
			// Create and start CuratorFramework instance
			String zkQuorum = configuration.getValue(Configuration.ZOOKEEPER_QUORUM);
			if (zkQuorum == null || StringUtils.isBlank(zkQuorum)) {
				throw new RuntimeException(LOG_PREFIX_DEFAULT +
					" No valid ZooKeeper quorum has been specified. " +
					"You can specify the quorum via the configuration key '" +
					Configuration.ZOOKEEPER_QUORUM.key() + "'.");
			}

			String pathRoot = configuration.getValue(Configuration.ZOOKEEPER_PATH_ROOT);
			if (pathRoot == null || StringUtils.isBlank(pathRoot)) {
				throw new RuntimeException(LOG_PREFIX_DEFAULT +
					" No valid ZooKeeper root has been specified. " +
					"You can specify the root via the configuration key '" +
					Configuration.ZOOKEEPER_PATH_ROOT.key() + "'.");
			}

			int sessionTimeout = configuration.getInteger(Configuration.ZOOKEEPER_SESSION_TIMEOUT);
			int connectionTimeout = configuration.getInteger(Configuration.ZOOKEEPER_CONNECTION_TIMEOUT);
			int retryWait = configuration.getInteger(Configuration.ZOOKEEPER_RETRY_WAIT);
			int maxRetryAttempts = configuration.getInteger(Configuration.ZOOKEEPER_MAX_RETRY_ATTEMPTS);
			boolean disableSaslClient = configuration.getBoolean(Configuration.ZOOKEEPER_SASL_DISABLE);

			String aclModeName = configuration.getValue(Configuration.ZOOKEEPER_CLIENT_ACL);
			ZkClientACLMode aclMode = ZkClientACLMode.valueOf(aclModeName);

			ACLProvider aclProvider;
			if (disableSaslClient && aclMode == ZooKeeperUtils.ZkClientACLMode.CREATOR) {
				String errorMessage = LOG_PREFIX_DEFAULT +
					" Cannot set ACL role to " + aclMode + "  since SASL authentication is " +
					"disabled through the " + Configuration.ZOOKEEPER_SASL_DISABLE + " property";
				LOG.warn(errorMessage);
				throw new IllegalConfigurationException(errorMessage);
			}

			if (aclMode == ZooKeeperUtils.ZkClientACLMode.CREATOR) {
				LOG.info("{} Enforcing creator for ZK connections", LOG_PREFIX_DEFAULT);
				aclProvider = new ZooKeeperUtils.SecureAclProvider();
			} else {
				LOG.info("{} Enforcing default ACL for ZK connections", LOG_PREFIX_DEFAULT);
				aclProvider = new DefaultACLProvider();
			}

			LOG.info("{} Using '{}' as Zookeeper root on the quorum '{}'", LOG_PREFIX_DEFAULT, pathRoot, zkQuorum);

			client = CuratorFrameworkFactory.builder()
					.connectString(zkQuorum)
					.sessionTimeoutMs(sessionTimeout)
					.connectionTimeoutMs(connectionTimeout)
					.retryPolicy(new ExponentialBackoffRetry(retryWait, maxRetryAttempts))
					.namespace(getCanonicalPath(pathRoot))
					.aclProvider(aclProvider)
					.build();

			client.start();

			listenerCallbackExecutor = new ScheduledThreadPoolExecutor(2);
			client.getConnectionStateListenable().addListener(connectionStateListener, listenerCallbackExecutor);
			LOG.info("{} ZookeeperClientServices started.", LOG_PREFIX_DEFAULT);

			// Wait until connect to ZK successfully
			if (isWaitedUntilConnected) {
				LOG.info("{} Wating until connect to ZK successfully......", LOG_PREFIX_DEFAULT);
				while (!isConnected()) {
					Thread.sleep(10);
				}

				LOG.info("{} Connect to ZK success.", LOG_PREFIX_DEFAULT);
			}

			// Update state from STARTING to STARTED
			boolean success = state.compareAndSet(State.STARTING, State.STARTED);
			Preconditions.checkState(success, LOG_PREFIX_DEFAULT + " ZookeeperClientServices is not in STARTING and update to STARTED failed.");
		} catch (Throwable t) {
			// Stop service
			try {
				stopInternal();
			} catch (Throwable error) {
				LOG.error(LOG_PREFIX_DEFAULT + " Stop zookeeper services failed.", error);
			}

			// Update state from STARTING to STOPPED
			boolean success = state.compareAndSet(State.STARTING, State.STOPPED);
			Preconditions.checkState(success, LOG_PREFIX_DEFAULT + " ZookeeperClientServices is not in STARTING and update to STOPPED failed.");

			// Throw exception upwards
			throw t;
		}
	}

	static void stop() {
		if (state.get() == State.STOPPED) {
			return;
		}

		int retryNum = 0;
		while (!state.compareAndSet(State.STARTED, State.STOPPING)) {
			if (state.get() == State.STOPPED) {
				return;
			} else if (retryNum >= maxRetryStartOrStop) {
				throw new IllegalStateException(LOG_PREFIX_DEFAULT + " ZookeeperClientServices is not in STARTED and can't stop it.");
			}
		}

		// Wait listener register or unregister to finish
		while (changingListenrCounter.get() > 0) {}

		try {
			stopInternal();
		} catch (Throwable error) {
			LOG.error(LOG_PREFIX_DEFAULT + "Stop zookeeper services failed.", error);
		} finally {
			// Update state from STOPPING to STOPPED
			if (!state.compareAndSet(State.STOPPING, State.STOPPED)) {
				LOG.warn("{} ZookeeperClientServices is not in STOPPING and update to STOPPED failed.", LOG_PREFIX_DEFAULT);
			}
		}
	}

	public static void registerListener(NodeRetrievalListener listener, String retrievalPath, boolean isWaitedUntilListened) throws Exception {
		Preconditions.checkNotNull(listener, LOG_PREFIX_DEFAULT + " Listener must not be null.");
		Preconditions.checkNotNull(retrievalPath, LOG_PREFIX_DEFAULT + " Retrieval path must not be null.");

		// Increment registering counter
		changingListenrCounter.incrementAndGet();

		try {
			NodeChangeListener changeListener = listenerMap.get(listener);
			if (changeListener != null) {
				waitUntilNodeListened(isWaitedUntilListened, changeListener);
				return;
			}

			if (state.get() != State.STARTED) {
				throw new IllegalStateException(
						String.format("%s ZookeeperClientServices is not in STARTED and can't register listener on the path '/%ss' for %s."
							, LOG_PREFIX_DEFAULT, client.getNamespace(), retrievalPath, listener)
				);
			}

			final NodeCacheWithPath newCache = new NodeCacheWithPath(client, retrievalPath);
			final NodeChangeListener newChangeListener = new NodeChangeListener(listener, newCache);
			newCache.getListenable().addListener(newChangeListener, listenerCallbackExecutor);
			newCache.start();

			changeListener = listenerMap.putIfAbsent(listener, newChangeListener);
			if (changeListener == null) {
				changeListener = newChangeListener;
				LOG.info("{} Start NodeRetrievalListener on the path '/{}{}' for {}."
					, LOG_PREFIX_DEFAULT, client.getNamespace(), newCache.getPath(), listener);
			} else {
				// Listener has started and close the just created one
				try {
					newCache.close();
				} catch (Throwable t) {
					LOG.error(LOG_PREFIX_DEFAULT + " Close NodeCache error.", t);
				}
			}

			waitUntilNodeListened(isWaitedUntilListened, changeListener);
		} finally {
			// Decrement registering counter
			changingListenrCounter.decrementAndGet();
		}
	}

	public static void unregisterListener(NodeRetrievalListener listener) {
		Preconditions.checkNotNull(listener, LOG_PREFIX_DEFAULT + " Listener must not be null.");

		// Increment unregistering counter
		changingListenrCounter.incrementAndGet();

		try {
			NodeChangeListener changeListener = listenerMap.get(listener);
			if (changeListener == null) {
				return;
			}

			if (state.get() != State.STARTED) {
				NodeCacheWithPath cache = changeListener.getNodeCache();

				LOG.warn("{} ZookeeperClientServices is not in STARTED and can't unregister listener on the path '/{}{}' for {}."
					, LOG_PREFIX_DEFAULT, client.getNamespace(), cache.getPath(), listener);
			} else {
				changeListener = listenerMap.remove(listener);
				if (changeListener != null) {
					NodeCacheWithPath cache = changeListener.getNodeCache();
					try {
						cache.close();
					} catch (Throwable t) {
						LOG.error(LOG_PREFIX_DEFAULT + "Close NodeCache error.", t);
					} finally {
						LOG.info("{} Stop NodeRetrievalListener on the path '/{}{}' for {}."
							, LOG_PREFIX_DEFAULT, client.getNamespace(), cache.getPath(), listener);
					}
				}
			}

		} catch (Throwable t) {
			// Here do nothing
		} finally {
			// Decrement unregistering counter
			changingListenrCounter.decrementAndGet();
		}
	}

	public static boolean isStarted() {
		return (client != null);
	}

	public static boolean isConnected() {
		return connState.isConnected();
	}

	public static boolean isListenerRegistered(NodeRetrievalListener listener) {
		return (listenerMap.get(listener) != null);
	}

	private static String getCanonicalPath(String namespace) {
		// Curator prepends a '/' manually and throws an Exception if the
		// namespace starts with a '/'.
		if (namespace.startsWith("/")) {
			namespace = namespace.substring(1);
		}

		if (namespace.endsWith("/")) {
			namespace = namespace.substring(0, namespace.length() - 1);
		}

		return namespace;
	}

	private static void stopInternal() {
		try {
			// Stop all node listeners
			Iterator<Map.Entry<NodeRetrievalListener, NodeChangeListener>> it = listenerMap.entrySet().iterator();
			while (it.hasNext()) {
				try {
					NodeCacheWithPath nodeCache = it.next().getValue().getNodeCache();
					nodeCache.close();
				} catch (Throwable t) {
					LOG.error(LOG_PREFIX_DEFAULT + " Close NodeCache error.", t);
				}
			}

			if (client != null) {
				// Close the connection-state listener
				try {
					client.getConnectionStateListenable().removeListener(connectionStateListener);
				} catch (Throwable t) {
					LOG.error(LOG_PREFIX_DEFAULT + " Close ConnectionStateListener failed.", t);
				}

				// Close the zookeeper client
				try {
					client.close();
				} catch (Throwable t) {
					LOG.error(LOG_PREFIX_DEFAULT + " Close CuratorFramework failed.", t);
				}

				// Shutdown the thread-pool executor which is used to execute listen notify
				try {
					listenerCallbackExecutor.shutdownNow();
				} catch (Throwable t) {
					LOG.error(LOG_PREFIX_DEFAULT + " Shutdown the thread-pool executor failed.", t);
				}
			}
		} finally {
			listenerMap.clear();
			client = null;
			connState = ConnectionState.LOST;
			listenerCallbackExecutor = null;

			LOG.info("{} ZookeeperClientServices stopped.", LOG_PREFIX_DEFAULT);
		}
	}

	private static void waitUntilNodeListened(boolean isWait, NodeChangeListener changeListener)
		throws InterruptedException {
		// Wait until listen the first data successfully
		if (isWait) {
			NodeCacheWithPath nodeCache = changeListener.getNodeCache();
			NodeRetrievalListener retrievalListener = changeListener.getRetrievalListener();
			String retrievalPath = nodeCache.getPath();

			Thread currentThread = Thread.currentThread();
			LOG.info("{}-[Thread(name:'{}', id:{})] Waiting until listened the first data on the path '/{}{}' for {}."
				, LOG_PREFIX_DEFAULT, currentThread.getName(), currentThread.getId(), client.getNamespace(), retrievalPath, retrievalListener);

			boolean isPathExists = false;
			while (changeListener.getNotifyCount() <= 0) {
				try {
					Stat pathStat = client.checkExists().forPath(retrievalPath);
					if (pathStat == null) {
						isPathExists = false;
						break;
					} else {
						isPathExists = true;
					}
				} catch (Throwable t) {
					LOG.warn(
						String.format("%s-[Thread(name:'%s', id:%d)] Error when checking the exists of the path '/%s%s' for %s."
							, LOG_PREFIX_DEFAULT, currentThread.getName(), currentThread.getId(), client.getNamespace(), retrievalPath, retrievalListener)
						, t);
				}

				Thread.sleep(100);
			}

			if (!isPathExists) {
				LOG.info("{}-[Thread(name:'{}', id:{})] Do not wait listened the first data on the nonexistent path '/{}{}' for {}."
					, LOG_PREFIX_DEFAULT, currentThread.getName(), currentThread.getId(), client.getNamespace(), retrievalPath, retrievalListener);
			} else {
				LOG.info("{}-[Thread(name:'{}', id:{})] Have listened the first data on the path '/{}{}' for {}."
					, LOG_PREFIX_DEFAULT, currentThread.getName(), currentThread.getId(), client.getNamespace(), retrievalPath, retrievalListener);
			}
		}
	}

	private static void handleStateChange(ConnectionState newState) {
		connState = newState;

		// Log connection state
		switch (newState) {
			case CONNECTED:
				LOG.debug("{} Connected to ZooKeeper quorum. Nodes retrieval can start.", LOG_PREFIX_DEFAULT);
				break;
			case SUSPENDED:
				LOG.warn("{} Connection to ZooKeeper suspended. Can no longer retrieve nodes from ZooKeeper.", LOG_PREFIX_DEFAULT);
				break;
			case RECONNECTED:
				LOG.info("{} Connection to ZooKeeper was reconnected. Nodes retrieval can be restarted.", LOG_PREFIX_DEFAULT);
				break;
			case LOST:
				LOG.warn("{} Connection to ZooKeeper lost. Can no longer retrieve nodes from ZooKeeper.", LOG_PREFIX_DEFAULT);
				break;
			case READ_ONLY:
				LOG.warn("{} Connection to ZooKeeper read only. Nodes retrieval can start.", LOG_PREFIX_DEFAULT);
				break;
			default:
				LOG.warn("{} Unknown connection state to ZooKeeper: %s", LOG_PREFIX_DEFAULT, connState);
				break;
		}
	}

	@Override
	public void close() {
		stop();
	}

	/**
	 * Listener interface when zookeeper's node changed.
	 */
	public interface NodeRetrievalListener {
		void notify(String data, Throwable throwable);
	}

	/**
	 * Configuration object which stores key/value pairs.
	 */
	public static final class Configuration extends org.apache.flink.configuration.Configuration {
		public static final String STABILITY_PREFIX = "stability-test.";

		/** ZooKeeper servers. */
		public static final ConfigOption<String> ZOOKEEPER_QUORUM = ConfigOptions.key("stability-test.zookeeper.quorum")
				.noDefaultValue();

		/** The root path under which stores fault-injection control command in ZooKeeper. */
		public static final ConfigOption<String> ZOOKEEPER_PATH_ROOT = ConfigOptions.key("stability-test.zookeeper.path.root")
				.noDefaultValue();

		// ------------------------------------------------------------------------
		//  ZooKeeper Client Settings
		// ------------------------------------------------------------------------
		public static final ConfigOption<Integer> ZOOKEEPER_SESSION_TIMEOUT = ConfigOptions.key("stability-test.zookeeper.client.session-timeout")
				.defaultValue(60000);

		public static final ConfigOption<Integer> ZOOKEEPER_CONNECTION_TIMEOUT = ConfigOptions.key("stability-test.zookeeper.client.connection-timeout")
				.defaultValue(15000);

		public static final ConfigOption<Integer> ZOOKEEPER_RETRY_WAIT = ConfigOptions.key("stability-test.zookeeper.client.retry-wait")
				.defaultValue(5000);

		public static final ConfigOption<Integer> ZOOKEEPER_MAX_RETRY_ATTEMPTS = ConfigOptions.key("stability-test.zookeeper.client.max-retry-attempts")
				.defaultValue(Integer.MAX_VALUE);

		public static final ConfigOption<Boolean> ZOOKEEPER_SASL_DISABLE = ConfigOptions.key("stability-test.zookeeper.sasl.disable")
				.defaultValue(true);

		public static final ConfigOption<ZkClientACLMode> ZOOKEEPER_CLIENT_ACL = ConfigOptions.key("stability-test.zookeeper.client.acl")
				.defaultValue(ZkClientACLMode.OPEN);

		/** Stores all ConfigOption(s) of this configuration object. */
		private static final HashMap<String, ConfigOption<?>> optionMap = new HashMap<String, ConfigOption<?>>();

		public Configuration(Properties properties, String replacedPrefix) {
			Iterator<Map.Entry<Object, Object>> it = properties.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<Object, Object> entry = it.next();
				Object oKey = entry.getKey();
				if (oKey == null || !(oKey instanceof String)) {
					continue;
				}

				String key = (String) oKey;
				if (replacedPrefix != null && !replacedPrefix.isEmpty()) {
					if (key.startsWith(replacedPrefix)) {
						key = STABILITY_PREFIX + key.substring(replacedPrefix.length());
					}
				}

				Object oValue = entry.getValue();
				String value = (oValue == null) ? null : oValue.toString();
				if (ZOOKEEPER_QUORUM.key().equals(key)) {
					// Support zookeeper quorum list to separated by '#'
					value = value.replace("#", ",");
				}

				this.setString(key, value);
			}
		}
	}

	private static final class NodeCacheWithPath extends NodeCache {
		private final String path;

		public NodeCacheWithPath(CuratorFramework client, String path) {
			super(client, path);
			this.path = path;
		}

		public String getPath() {
			return this.path;
		}

		@Override
		public void close() throws IOException {
			getListenable().clear();
			super.close();
		}
	}

	private static final class NodeChangeListener implements NodeCacheListener {
		private final NodeCacheWithPath nodeCache;
		private final NodeRetrievalListener retrievalListener;
		private AtomicLong counter;

		NodeChangeListener(NodeRetrievalListener listener, NodeCacheWithPath cache) {
			this.retrievalListener = listener;
			this.nodeCache = cache;
			this.counter = new AtomicLong(0);
		}

		@Override
		public void nodeChanged() throws Exception {
			String data = null;
			Throwable throwable = null;

			try {
				ChildData childData = nodeCache.getCurrentData();
				if (childData != null) {
					byte[] buffer = childData.getData();
					if (buffer != null && buffer.length > 0) {
						data = new String(buffer, Charset.forName("UTF-8"));
					}
				}
			} catch (Throwable t) {
				throwable = t;
			}

			try {
				retrievalListener.notify(data, throwable);
				counter.incrementAndGet();
			} catch (Throwable t) {
				LOG.error("", t);
			}
			if (throwable != null) {
				throw new Exception(throwable);
			}
		}

		public NodeCacheWithPath getNodeCache() {
			return nodeCache;
		}

		public NodeRetrievalListener getRetrievalListener() {
			return retrievalListener;
		}

		public long getNotifyCount() {
			return counter.get();
		}
	}

	/**
	 * Possible states of zookeeper services.
	 */
	private enum State {
		STARTING,
		STARTED,
		STOPPING,
		STOPPED
	}
}
