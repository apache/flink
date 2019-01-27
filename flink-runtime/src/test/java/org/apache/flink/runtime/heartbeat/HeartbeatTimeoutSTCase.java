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

package org.apache.flink.runtime.heartbeat;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.stabilitytest.ShutdownManager;
import org.apache.flink.runtime.stabilitytest.StabilityTestException;
import org.apache.flink.runtime.stabilitytest.ZookeeperClientServices;
import org.apache.flink.runtime.stabilitytest.ZookeeperClientServices.NodeRetrievalListener;
import org.apache.flink.util.Preconditions;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.lang.reflect.Field;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.apache.flink.runtime.stabilitytest.Constants.LOG_PREFIX_DEFAULT;
import static org.apache.flink.runtime.stabilitytest.Constants.LOG_PREFIX_FAULT_PROBABILITY;
import static org.apache.flink.runtime.stabilitytest.Constants.LOG_PREFIX_FAULT_TRIGGER;

/**
 * Stability test case class for heartbeat timeout, it simulate heartbeat's full-timeout and
 * half-timeout via the given probability.
 *
 * <p>A stability test case class what be called ST-class must implements the static method
 * void initialize(Object arg0, Object arg1).
 *
 * <p>A stability test case what be called ST-case is one static method or the assemble of more
 * static methods in a ST-class. The first parameter of every ST-case method must be Boolean[],
 * it is a array contained one element and tell the caller whether step over all behind codes of
 * the injectable target method after invoke the ST-case method.
 */
public final class HeartbeatTimeoutSTCase implements AutoCloseable, Closeable {
	private static final Logger LOG = LoggerFactory.getLogger(HeartbeatTimeoutSTCase.class);

	/** The Random() instance for full timeout. */
	private static final Random fullTimeoutRandom = new Random();

	/** The Random() instance for half timeout. */
	private static final Random halfTimeoutRandom = new Random();

	/** The upper bound of random. */
	private static final int RANDOM_BOUND = Integer.MAX_VALUE;

	/** Probability of full-timeout. */
	private static volatile double fullTimeoutProb = 0.0;

	/** Probability of half-timeout. */
	private static volatile double halfTimeoutProb = 0.0;

	/** Zookeeper node for heartbeat's full-timeout control. */
	public static final String FULL_TIMEOUT_ZKNODE_PATH = "/heartbeat_timeout/full";

	/** Zookeeper node for heartbeat's half-timeout control. */
	public static final String HALF_TIMEOUT_ZKNODE_PATH = "/heartbeat_timeout/half";

	/** Mapping from the target resource id to TimeoutHit. */
	private static final ConcurrentHashMap<String, TimeoutHit> HIT_MAP = new ConcurrentHashMap<String, TimeoutHit>();

	/** Vars for load fields of HeartbeatManagerImpl. */
	private static final AtomicReference<LoadState> loadState = new AtomicReference<LoadState>(LoadState.UNLOAD);
	private static Field heartbeatTimeoutIntervalMsField = null;
	private static Field ownResourceIDField = null;

	/** Heartbeat timeout interval of HeartbeatManagerImpl in milli seconds. */
	private static volatile long heartbeatTimeoutIntervalMs = -1;

	/** Executor to clean the hit map. */
	private static final ScheduledThreadPoolExecutor timerExecutor = new ScheduledThreadPoolExecutor(1);


	private static NodeRetrievalListener fullProbListener = new NodeRetrievalListener() {
		@Override
		public void notify(String data, Throwable throwable) {
			double oldFailProb = fullTimeoutProb;

			if (data == null || throwable != null) {
				fullTimeoutProb = 0.0;
			} else {
				try {
					double prob = Double.parseDouble(data);
					fullTimeoutProb = Math.max(0.0, Math.min(1.0, prob));
				} catch (NumberFormatException nfe) {
					LOG.error("{} update heartbeat-timeout[{}] probability failed, ignore this value: {}."
						, LOG_PREFIX_FAULT_PROBABILITY, HitType.FULL, data);

					return;
				}
			}

			LOG.info("{} update heartbeat-timeout[{}] probability from {} to {}."
				, LOG_PREFIX_FAULT_PROBABILITY, HitType.FULL, oldFailProb, fullTimeoutProb);
		}
	};

	private static NodeRetrievalListener halfProbListener = new NodeRetrievalListener() {
		@Override
		public void notify(String data, Throwable throwable) {
			double oldFailProb = halfTimeoutProb;

			if (data == null || throwable != null) {
				halfTimeoutProb = 0.0;
			} else {
				try {
					double prob = Double.parseDouble(data);
					halfTimeoutProb = Math.max(0.0, Math.min(1.0, prob));
				} catch (NumberFormatException nfe) {
					LOG.error("{} update heartbeat-timeout[{}] probability failed, ignore this value: {}."
						, LOG_PREFIX_FAULT_PROBABILITY, HitType.HALF, data);

					return;
				}
			}

			LOG.info("{} update heartbeat-timeout[{}] probability from {} to {}."
				, LOG_PREFIX_FAULT_PROBABILITY, HitType.HALF, oldFailProb, halfTimeoutProb);
		}
	};

	/**
	 * Register HeartbeatTimeoutSTCase to the shutdown manager,
	 * and start to clean the hit map
	 */
	static {
		ShutdownManager.register(new HeartbeatTimeoutSTCase());
		startCleanThread();
	}

	/**
	 *
	 */
	private HeartbeatTimeoutSTCase(){

	}

	/**
	 * Connection to the ZooKeeper quorum, register full-timeout and half-timout listeners.
	 * @param arg0 is a Properties object to initialize Zookeeper client settings
	 * @param arg1 is the prefix of property key which should be replaced
	 * @throws Exception
	 */
	public static void initialize(Object arg0, Object arg1) throws StabilityTestException {
		Preconditions.checkArgument(arg0 != null && arg0 instanceof Properties);
		Preconditions.checkArgument(arg1 == null || arg1 instanceof String);
		Properties properties = (Properties) arg0;
		String replacedPrefix = (String) arg1;

		try {
			if (!ZookeeperClientServices.isStarted()) {
				ZookeeperClientServices.start(new ZookeeperClientServices.Configuration(properties, replacedPrefix), true);
			}

			if (!ZookeeperClientServices.isListenerRegistered(fullProbListener)) {
				ZookeeperClientServices.registerListener(fullProbListener, FULL_TIMEOUT_ZKNODE_PATH, true);
			}

			if (!ZookeeperClientServices.isListenerRegistered(halfProbListener)) {
				ZookeeperClientServices.registerListener(halfProbListener, HALF_TIMEOUT_ZKNODE_PATH, true);
			}
		} catch (Throwable t) {
			throw new StabilityTestException(t);
		}
	}

	/**
	 * Simulate heartbeat full timeout via the given probability.
	 * @param isEarlyReturn tell the ST-Case helper whether return directly after invoke ST-Case method
	 * @param arg0 is a HeartbeatManagerImpl object
	 * @param arg1 the resource id of the heartbeat target
	 * @param arg2 the payload
	 */
	public static void fullTimeout(Boolean[] isEarlyReturn, Object arg0, Object arg1, Object arg2) throws StabilityTestException {
		Preconditions.checkArgument(arg0 != null && arg0 instanceof HeartbeatManagerImpl);
		Preconditions.checkArgument(arg1 != null && arg1 instanceof ResourceID);
		HeartbeatManagerImpl heartbeatManager = (HeartbeatManagerImpl) arg0;
		ResourceID resourceID = (ResourceID) arg1;
		Object payload = arg2;

		isEarlyReturn[0] = heartbeatTimeout(heartbeatManager, resourceID, payload, fullTimeoutRandom, fullTimeoutProb, HitType.FULL);
	}

	/**
	 * Simulate heartbeat half timeout via the given probability.
	 * @param isEarlyReturn tell the ST-Case helper whether return directly after invoke ST-Case method
	 * @param arg0 is a HeartbeatManagerImpl object
	 * @param arg1 the resource id of the heartbeat target
	 * @param arg2 the payload
	 */
	public static void halfTimeout(Boolean[] isEarlyReturn, Object arg0, Object arg1, Object arg2) throws StabilityTestException {
		Preconditions.checkArgument(arg0 != null && arg0 instanceof HeartbeatManagerImpl);
		Preconditions.checkArgument(arg1 != null && arg1 instanceof ResourceID);
		HeartbeatManagerImpl heartbeatManager = (HeartbeatManagerImpl) arg0;
		ResourceID resourceID = (ResourceID) arg1;
		Object payload = arg2;

		isEarlyReturn[0] = heartbeatTimeout(heartbeatManager, resourceID, payload, halfTimeoutRandom, halfTimeoutProb, HitType.HALF);
	}

	private static boolean heartbeatTimeout(HeartbeatManagerImpl heartbeatManager, ResourceID resourceID, Object payload
		, final Random random, final double timeoutProb, final HitType hitType) throws StabilityTestException {

		if (timeoutProb <= 0) {
			return false;
		}

		final long currentTime = System.currentTimeMillis();

		// Load used fields of HeartbeatManagerImpl
		loadFields();

		try {
			heartbeatTimeoutIntervalMs = heartbeatTimeoutIntervalMsField.getLong(heartbeatManager);

			ResourceID sourceID = resourceID;
			ResourceID targetID = (ResourceID) ownResourceIDField.get(heartbeatManager);
			if (hitType == HitType.HALF) {
				ResourceID tmpId = sourceID;
				sourceID = targetID;
				targetID = tmpId;
			}

			// Check whether or not return true continuously
			TimeoutHit timeoutHit = HIT_MAP.get(resourceID.toString());
			if (timeoutHit != null) {
				if (timeoutHit.getType() != hitType) {
					// Not in the current hit type
					return false;
				}

				long durationMs = currentTime - timeoutHit.getTime();
				if (durationMs <= heartbeatTimeoutIntervalMs) {
					LOG.info("{} heartbeat-timeout[{}] duration {}ms (from {} to {})."
							, LOG_PREFIX_FAULT_TRIGGER, hitType, durationMs, sourceID, targetID);

					return true;
				} else if (HIT_MAP.remove(resourceID.toString()) != null) {
					LOG.info("{} heartbeat-timeout[{}] reseted (from {} to {})."
							, LOG_PREFIX_FAULT_TRIGGER, hitType, sourceID, targetID);
				}
			}

			// Trigger timeout randomly
			int rnd = random.nextInt(RANDOM_BOUND);
			if (rnd <= RANDOM_BOUND * timeoutProb) {
				if (!isHeartbeatBetweenTaskManagerAndOthers(sourceID, targetID)) {
					LOG.info("{} skip heartbeat-timeout[{}] triggering (from {} to {})."
						, LOG_PREFIX_FAULT_TRIGGER, hitType, sourceID, targetID);
					return false;
				}

				// Hit the probability and trigger timeout on purpose
				TimeoutHit newTimeoutHit = new TimeoutHit(sourceID, targetID, currentTime, hitType);
				TimeoutHit prevTimeoutHit = HIT_MAP.putIfAbsent(resourceID.toString(), newTimeoutHit);
				if (prevTimeoutHit == null) {
					LOG.info("{} heartbeat-timeout[{}] started (from {} to {})."
							, LOG_PREFIX_FAULT_TRIGGER, hitType, sourceID, targetID);

					return true;
				}
			}
		} catch (IllegalAccessException e) {
			throw new StabilityTestException(e);
		}

		return false;
	}

	private static void loadFields() throws StabilityTestException {
		if (loadState.get() == LoadState.LOADED) {
			return;
		}

		while (!loadState.compareAndSet(LoadState.UNLOAD, LoadState.LOADING)) {
			if (loadState.get() == LoadState.LOADED) {
				return;
			}
		}

		try {
			heartbeatTimeoutIntervalMsField = HeartbeatManagerImpl.class.getDeclaredField("heartbeatTimeoutIntervalMs");
			ownResourceIDField = HeartbeatManagerImpl.class.getDeclaredField("ownResourceID");

			heartbeatTimeoutIntervalMsField.setAccessible(true);
			ownResourceIDField.setAccessible(true);

			// Update state from LOADING to LOADED
			boolean success = loadState.compareAndSet(LoadState.LOADING, LoadState.LOADED);
			Preconditions.checkState(success, LOG_PREFIX_DEFAULT + " LoadState is not in LOADING and update to LOADED failed.");
		} catch (Throwable t) {
			// Update state from LOADING to UNLOAD
			boolean success = loadState.compareAndSet(LoadState.LOADING, LoadState.UNLOAD);
			Preconditions.checkState(success, LOG_PREFIX_DEFAULT + " LoadState is not in LOADING and update to UNLOAD failed.");

			throw new StabilityTestException(t);
		}

	}

	private static void startCleanThread() {
		// Clean the hit map at regular intervals
		timerExecutor.scheduleAtFixedRate(new Runnable(){
			@Override
			public void run() {
				if (heartbeatTimeoutIntervalMs < 0) {
					return;
				}

				HIT_MAP.forEach(new BiConsumer<String, TimeoutHit>(){
					@Override
					public void accept(String resourceID, TimeoutHit timeoutHit) {
						if ((System.currentTimeMillis() - timeoutHit.getTime()) >= 1.5 * heartbeatTimeoutIntervalMs) {
							if (HIT_MAP.remove(resourceID) != null) {
								LOG.info("{} heartbeat-timeout[{}] reseted by cleaner (from {} to {})."
									, LOG_PREFIX_FAULT_TRIGGER, timeoutHit.getType(), timeoutHit.getSourceID(), timeoutHit.getTargetID());
							}
						}
					}
				});

			}
		}, 5, 5, TimeUnit.SECONDS);
	}

	private static void stopCleanThread() {
		try {
			timerExecutor.shutdownNow();
		} catch (Throwable t) {
			LOG.error(LOG_PREFIX_DEFAULT + " Stop clean thread failed.", t);
		}

		LOG.info("{}, Clean thread stopped.", LOG_PREFIX_DEFAULT);
	}

	@Override
	public void close() {
		stopCleanThread();

		HIT_MAP.clear();
	}

	private static class TimeoutHit {
		private final ResourceID sourceID;
		private final ResourceID targetID;
		private final long time;
		private final HitType type;

		TimeoutHit(ResourceID sourceID, ResourceID targetID, long time, HitType type) {
			this.sourceID = sourceID;
			this.targetID = targetID;
			this.time = time;
			this.type = type;
		}

		ResourceID getSourceID() {
			return sourceID;
		}

		ResourceID getTargetID() {
			return targetID;
		}

		long getTime() {
			return time;
		}

		HitType getType() {
			return type;
		}
	}

	/**
	 * Timeout hit types.
	 */
	private enum HitType {
		FULL,
		HALF
	}

	/**
	 * Possible states of loading fields.
	 */
	private enum LoadState {
		UNLOAD,
		LOADING,
		LOADED
	}

	private static boolean isHeartbeatBetweenTaskManagerAndOthers(ResourceID sourceID, ResourceID targetID) {
		return sourceID.toString().startsWith("container") || targetID.toString().startsWith("container");
	}

	public static void main(String[] args) {
		Assert.assertTrue(isHeartbeatBetweenTaskManagerAndOthers(new ResourceID("container_1525695259137_0009_01_000002"), ResourceID.generate()));
		Assert.assertTrue(isHeartbeatBetweenTaskManagerAndOthers(ResourceID.generate(), new ResourceID("container_1525695259137_0009_01_000002")));
		Assert.assertFalse(isHeartbeatBetweenTaskManagerAndOthers(ResourceID.generate(), ResourceID.generate()));

		System.exit(0);
	}
}
