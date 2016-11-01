/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackendFactory;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.util.InstantiationUtil;
import org.slf4j.Logger;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Helpers for {@link StateObject} related code.
 */
public class StateUtil {

	private static final String STATE_BACKEND = "statebackend";

	private StateUtil() {
		throw new AssertionError();
	}

	/**
	 * Iterates through the passed state handles and calls discardState() on each handle that is not null. All
	 * occurring exceptions are suppressed and collected until the iteration is over and emitted as a single exception.
	 *
	 * @param handlesToDiscard State handles to discard. Passed iterable is allowed to deliver null values.
	 * @throws Exception exception that is a collection of all suppressed exceptions that were caught during iteration
	 */
	public static void bestEffortDiscardAllStateObjects(
			Iterable<? extends StateObject> handlesToDiscard) throws Exception {

		if (handlesToDiscard != null) {

			Exception suppressedExceptions = null;

			for (StateObject state : handlesToDiscard) {

				if (state != null) {
					try {
						state.discardState();
					} catch (Exception ex) {
						//best effort to still cleanup other states and deliver exceptions in the end
						if (suppressedExceptions == null) {
							suppressedExceptions = new Exception(ex);
						}
						suppressedExceptions.addSuppressed(ex);
					}
				}
			}

			if (suppressedExceptions != null) {
				throw suppressedExceptions;
			}
		}
	}

	/**
	 * Creates a state backend from the configuration.
	 *
	 * If no state backend is configured, the memory state backend will be used.
	 *
	 * @param logger Logger to sue.
	 * @param flinkConfig Configuration to parse state backend configuration.
	 * @param userCodeLoader User code class loader.
	 * @return The configured state backend.
	 * @throws IllegalConfigurationException If the configured backend cannot be instantiated.
	 * @throws Exception State backend instantiation failures are forwarded.
	 */
	public static AbstractStateBackend createStateBackend(
			Logger logger,
			Configuration flinkConfig,
			ClassLoader userCodeLoader) throws Exception {

		checkNotNull(logger, "Logger");
		checkNotNull(flinkConfig, "Flink Configuration");
		checkNotNull(logger, "User Code ClassLoader");

		// see if we have a backend specified in the configuration
		String backendName = flinkConfig.getString(ConfigConstants.STATE_BACKEND, null);

		if (backendName == null) {
			logger.warn("No state backend has been specified, using default state backend (Memory / JobManager)");
			backendName = "jobmanager";
		}

		AbstractStateBackend stateBackend;
		switch (backendName.toLowerCase()) {
			case "jobmanager":
				logger.info("State backend is set to heap memory (checkpoint to jobmanager)");
				stateBackend = new MemoryStateBackend();
				break;

			case "filesystem":
				FsStateBackend backend = new FsStateBackendFactory().createFromConfig(flinkConfig);
				logger.info("State backend is set to heap memory (checkpoints to filesystem \""
						+ backend.getBasePath() + "\")");
				stateBackend = backend;
				break;

			case "rocksdb":
				backendName = "org.apache.flink.contrib.streaming.state.RocksDBStateBackendFactory";
				// fall through to the 'default' case that uses reflection to load the backend
				// that way we can keep RocksDB in a separate module

			default:
				try {
					@SuppressWarnings("rawtypes")
					Class<? extends StateBackendFactory> clazz =
							Class.forName(backendName, false, userCodeLoader).
									asSubclass(StateBackendFactory.class);

					stateBackend = clazz.newInstance().createFromConfig(flinkConfig);
				} catch (ClassNotFoundException e) {
					throw new IllegalConfigurationException("Cannot find configured state backend: " + backendName);
				} catch (ClassCastException e) {
					throw new IllegalConfigurationException("The class configured under '" +
							ConfigConstants.STATE_BACKEND + "' is not a valid state backend factory (" +
							backendName + ')');
				} catch (Throwable t) {
					throw new IllegalConfigurationException("Cannot create configured state backend", t);
				}
		}

		return stateBackend;
	}

	/**
	 * Serializes a state backend into a config instance.
	 *
	 * @param config Configuration to serialized backend to.
	 * @param stateBackend State backend to serialize.
	 */
	public static void serializeStateBackend(Configuration config, AbstractStateBackend stateBackend) {
		if (stateBackend != null) {
			try {
				InstantiationUtil.writeObjectToConfig(stateBackend, config, StateUtil.STATE_BACKEND);
			} catch (Exception e) {
				throw new RuntimeException("Could not serialize stateHandle provider.", e);
			}
		}
	}

	/**
	 * Deserialize a state backend from a config instance.
	 *
	 * @param config Configuration to deserialize backend from.
	 * @param userCodeLoader User code class loader.
	 * @return The deserialized state backend
	 */
	public static AbstractStateBackend deserializeStateBackend(Configuration config, ClassLoader userCodeLoader) {
		try {
			return InstantiationUtil.readObjectFromConfig(config, STATE_BACKEND, userCodeLoader);
		} catch (Exception e) {
			throw new RuntimeException("Could not instantiate statehandle provider.", e);
		}
	}

}
