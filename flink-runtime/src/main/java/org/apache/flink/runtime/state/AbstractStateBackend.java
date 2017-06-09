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

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackendFactory;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.util.DynamicCodeLoadingException;

import org.slf4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An abstract base implementation of the {@link StateBackend} interface.
 * 
 * <p>
 */
@PublicEvolving
public abstract class AbstractStateBackend implements StateBackend, java.io.Serializable {

	private static final long serialVersionUID = 4620415814639230247L;

	// ------------------------------------------------------------------------
	//  Configuration shortcut names
	// ------------------------------------------------------------------------

	/** The shortcut configuration name for the MemoryState backend that checkpoints to the JobManager */
	public static final String MEMORY_STATE_BACKEND_NAME = "jobmanager";

	/** The shortcut configuration name for the FileSystem State backend */ 
	public static final String FS_STATE_BACKEND_NAME = "filesystem";

	/** The shortcut configuration name for the RocksDB State Backend */
	public static final String ROCKSDB_STATE_BACKEND_NAME = "rocksdb";

	// ------------------------------------------------------------------------
	//  State Backend - Persisting Byte Storage
	// ------------------------------------------------------------------------

	@Override
	public abstract CheckpointStreamFactory createStreamFactory(
			JobID jobId,
			String operatorIdentifier) throws IOException;

	@Override
	public abstract CheckpointStreamFactory createSavepointStreamFactory(
			JobID jobId,
			String operatorIdentifier,
			@Nullable String targetLocation) throws IOException;

	// ------------------------------------------------------------------------
	//  State Backend - State-Holding Backends
	// ------------------------------------------------------------------------

	@Override
	public abstract <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
			Environment env,
			JobID jobID,
			String operatorIdentifier,
			TypeSerializer<K> keySerializer,
			int numberOfKeyGroups,
			KeyGroupRange keyGroupRange,
			TaskKvStateRegistry kvStateRegistry) throws IOException;

	@Override
	public abstract OperatorStateBackend createOperatorStateBackend(
			Environment env,
			String operatorIdentifier) throws Exception;

	// ------------------------------------------------------------------------
	//  Loading the state backend from a configuration 
	// ------------------------------------------------------------------------

	/**
	 * Loads the state backend from the configuration, from the parameter 'state.backend', as defined
	 * in {@link CoreOptions#STATE_BACKEND}.
	 * 
	 * <p>The state backends can be specified either via their shortcut name, or via the class name
	 * of a {@link StateBackendFactory}. If a StateBackendFactory class name is specified, the factory
	 * is instantiated (via its zero-argument constructor) and its
	 * {@link StateBackendFactory#createFromConfig(Configuration)} method is called.
	 *
	 * <p>Recognized shortcut names are '{@value AbstractStateBackend#MEMORY_STATE_BACKEND_NAME}',
	 * '{@value AbstractStateBackend#FS_STATE_BACKEND_NAME}', and
	 * '{@value AbstractStateBackend#ROCKSDB_STATE_BACKEND_NAME}'.
	 * 
	 * @param config The configuration to load the state backend from
	 * @param classLoader The class loader that should be used to load the state backend
	 * @param logger Optionally, a logger to log actions to (may be null)
	 * 
	 * @return The instantiated state backend.
	 * 
	 * @throws DynamicCodeLoadingException
	 *             Thrown if a state backend factory is configured and the factory class was not
	 *             found or the factory could not be instantiated
	 * @throws IllegalConfigurationException
	 *             May be thrown by the StateBackendFactory when creating / configuring the state
	 *             backend in the factory
	 * @throws IOException
	 *             May be thrown by the StateBackendFactory when instantiating the state backend
	 */
	public static StateBackend loadStateBackendFromConfig(
			Configuration config,
			ClassLoader classLoader,
			@Nullable Logger logger) throws IllegalConfigurationException, DynamicCodeLoadingException, IOException {

		checkNotNull(config, "config");
		checkNotNull(classLoader, "classLoader");

		final String backendName = config.getString(CoreOptions.STATE_BACKEND);
		if (backendName == null) {
			return null;
		}

		// by default the factory class is the backend name 
		String factoryClassName = backendName;

		switch (backendName.toLowerCase()) {
			case MEMORY_STATE_BACKEND_NAME:
				if (logger != null) {
					logger.info("State backend is set to heap memory (checkpoint to JobManager)");
				}
				return new MemoryStateBackend();

			case FS_STATE_BACKEND_NAME:
				FsStateBackend fsBackend = new FsStateBackendFactory().createFromConfig(config);
				if (logger != null) {
					logger.info("State backend is set to heap memory (checkpoints to filesystem \"{}\")",
							fsBackend.getBasePath());
				}
				return fsBackend;

			case ROCKSDB_STATE_BACKEND_NAME:
				factoryClassName = "org.apache.flink.contrib.streaming.state.RocksDBStateBackendFactory";
				// fall through to the 'default' case that uses reflection to load the backend
				// that way we can keep RocksDB in a separate module

			default:
				if (logger != null) {
					logger.info("Loading state backend via factory {}", factoryClassName);
				}

				StateBackendFactory<?> factory;
				try {
					@SuppressWarnings("rawtypes")
					Class<? extends StateBackendFactory> clazz = 
							Class.forName(factoryClassName, false, classLoader)
									.asSubclass(StateBackendFactory.class);

					factory = clazz.newInstance();
				}
				catch (ClassNotFoundException e) {
					throw new DynamicCodeLoadingException(
							"Cannot find configured state backend factory class: " + backendName, e);
				}
				catch (ClassCastException | InstantiationException | IllegalAccessException e) {
					throw new DynamicCodeLoadingException("The class configured under '" +
							CoreOptions.STATE_BACKEND.key() + "' is not a valid state backend factory (" +
							backendName + ')', e);
				}
				
				return factory.createFromConfig(config);
		}
	}

	/**
	 * Loads the state backend from the configuration, from the parameter 'state.backend', as defined
	 * in {@link CoreOptions#STATE_BACKEND}. If no state backend is configures, this instantiates the
	 * default state backend (the {@link MemoryStateBackend}). 
	 *
	 * <p>Refer to {@link #loadStateBackendFromConfig(Configuration, ClassLoader, Logger)} for details on
	 * how the state backend is loaded from the configuration.
	 *
	 * @param config The configuration to load the state backend from
	 * @param classLoader The class loader that should be used to load the state backend
	 * @param logger Optionally, a logger to log actions to (may be null)
	 *
	 * @return The instantiated state backend.
	 *
	 * @throws DynamicCodeLoadingException
	 *             Thrown if a state backend factory is configured and the factory class was not
	 *             found or the factory could not be instantiated
	 * @throws IllegalConfigurationException
	 *             May be thrown by the StateBackendFactory when creating / configuring the state
	 *             backend in the factory
	 * @throws IOException
	 *             May be thrown by the StateBackendFactory when instantiating the state backend
	 */
	public static StateBackend loadStateBackendFromConfigOrCreateDefault(
			Configuration config,
			ClassLoader classLoader,
			@Nullable Logger logger) throws IllegalConfigurationException, DynamicCodeLoadingException, IOException {

		final StateBackend fromConfig = loadStateBackendFromConfig(config, classLoader, logger);

		if (fromConfig != null) {
			return fromConfig;
		}
		else {
			if (logger != null) {
				logger.info("No state backend has been configured, using default state backend (Memory / JobManager)");
			}
			return new MemoryStateBackend();
		}
	}
}
