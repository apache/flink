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

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.util.DynamicCodeLoadingException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * This class contains utility methods to load checkpoint storage from configurations.
 */
public class CheckpointStorageLoader {

	public static final String JOB_MANAGER_STORAGE_NAME = "jobmanager";

	public static final String FILE_SYSTEM_STORAGE_NAME = "filesystem";

	/**
	 * Loads the checkpoint storage from the configuration, from the parameter 'state.checkpoint-storage',
	 * as defined in {@link CheckpointingOptions#CHECKPOINT_STORAGE}.
	 *
	 * <p>The state backends can be specified either via their shortcut name, or via the class name
	 * of a {@link CheckpointStorageFactory}. If a CheckpointStorageFactory class name is specified, the factory
	 * is instantiated (via its zero-argument constructor) and its
	 * {@link CheckpointStorageFactory#createFromConfig(ReadableConfig, ClassLoader)} method is called.
	 *
	 * <p>Recognized shortcut names are '{@value #JOB_MANAGER_STORAGE_NAME}', and
	 * '{@value #FILE_SYSTEM_STORAGE_NAME}'.
	 *
	 * @param config The configuration to load the checkpoint storage from
	 * @param classLoader The class loader that should be used to load the checkpoint storage
	 * @param logger Optionally, a logger to log actions to (may be null)
	 *
	 * @return The instantiated checkpoint storage.
	 *
	 * @throws DynamicCodeLoadingException
	 *             Thrown if a checkpoint storage factory is configured and the factory class was not
	 *             found or the factory could not be instantiated
	 * @throws IllegalConfigurationException
	 *             May be thrown by the CheckpointStorageFactory when creating / configuring the checkpoint
	 *             storage in the factory
	 * @throws IOException
	 *             May be thrown by the CheckpointStorageFactory when instantiating the checkpoint storage
	 */
	public static CheckpointStorage loadCheckpointStorageFromConfig(
			ReadableConfig config,
			ClassLoader classLoader,
			@Nullable Logger logger) throws IllegalStateException, DynamicCodeLoadingException, IOException {

		Preconditions.checkNotNull(config, "config");
		Preconditions.checkNotNull(classLoader, "classLoader");

		final String storageName = config.get(CheckpointingOptions.CHECKPOINT_STORAGE);
		if (storageName == null) {
			return null;
		}

		switch (storageName.toLowerCase()) {
			case JOB_MANAGER_STORAGE_NAME:
				throw new IllegalStateException("JobManagerCheckpointStorage is not yet implemented");

			case FILE_SYSTEM_STORAGE_NAME:
				throw new IllegalStateException("FileSystemCheckpointStorage is not yet implemented");

			default:
				if (logger != null) {
					logger.info("Loading state backend via factory {}", storageName);
				}

				CheckpointStorageFactory<?> factory;
				try {
					@SuppressWarnings("rawtypes")
					Class<? extends CheckpointStorageFactory> clazz =
							Class.forName(storageName, false, classLoader)
								.asSubclass(CheckpointStorageFactory.class);

					factory = clazz.newInstance();
				} catch (ClassNotFoundException e) {
					throw new DynamicCodeLoadingException(
							"Cannot find configured state backend factory class: " + storageName, e);
				} catch (ClassCastException | InstantiationException | IllegalAccessException e) {
					throw new DynamicCodeLoadingException("The class configured under '" +
							CheckpointingOptions.CHECKPOINT_STORAGE.key() + "' is not a valid checkpoint storage factory (" +
							storageName + ')', e);
				}

				return factory.createFromConfig(config, classLoader);
		}
	}

	public static CheckpointStorage fromApplicationOrConfigOrDefault(
			@Nullable CheckpointStorage fromApplication,
			@Nullable String baseSavepointDir,
			StateBackend configuredStateBackend,
			Configuration config,
			ClassLoader classLoader,
			@Nullable Logger logger) throws IllegalConfigurationException, DynamicCodeLoadingException, IOException {

		Preconditions.checkNotNull(config, "config");
		Preconditions.checkNotNull(classLoader, "classLoader");
		Preconditions.checkNotNull(configuredStateBackend, "statebackend");

		// (1) Legacy state backends always take precedence
		// for backwards compatibility.
		if (configuredStateBackend instanceof CheckpointStorage) {
			if (logger != null) {
				logger.info("Using legacy state backend {} as Job checkpoint storage", configuredStateBackend);
			}

			return (CheckpointStorage) configuredStateBackend;
		}

		if (baseSavepointDir != null) {
			config = new Configuration(config);
			config.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, baseSavepointDir);
		}

		CheckpointStorage storage;

		// (2) Application defined checkpoint storage
		if (fromApplication != null) {
			// see if this is supposed to pick up additional configuration parameters
			if (fromApplication instanceof ConfigurableCheckpointStorage) {
				// needs to pick up configuration
				if (logger != null) {
					logger.info("Using job/cluster config to configure application-defined checkpoint storage: {}", fromApplication);
				}

				storage = ((ConfigurableCheckpointStorage) fromApplication).configure(config, classLoader);
			} else {
				// keep as is!
				storage = fromApplication;
			}

			if (logger != null) {
				logger.info("Using application defined checkpoint storage: {}", storage);
			}
		} else {
			// (3) check if the config defines a checkpoint storage instance
			final CheckpointStorage fromConfig = loadCheckpointStorageFromConfig(config, classLoader, logger);
			if (fromConfig != null) {
				storage = fromConfig;
			} else {
				// (4) use the default
				throw new IllegalStateException("No checkpoint storage defined. Flink currently only supports legacy "
						+ "state backends, this case should never be reached.");
			}
		}

		return storage;
	}
}
