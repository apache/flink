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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A helper class to create {@link InternalTimeServiceManager} from configuration.
 */
public class InternalTimeServiceManagerLoader {

	public static final String HEAP_NAME = "heap";

	public static final String ROCKSDB_NAME = "rocksdb";

	/**
	 * Loads the {@link InternalTimeServiceManager} from the configuration, from the parameter 'timer.service', as defined
	 * in {@link CheckpointingOptions#TIMER_SERVICE}.
	 *
	 * <p>The timer service can be specified either via their shortcut name. Recognized shortcut names
	 * are '{@value InternalTimeServiceManagerLoader#HEAP_NAME}',
	 * '{@value InternalTimeServiceManagerLoader#ROCKSDB_NAME}'.
	 *
	 * @param config The configuration to load the time service manager from
	 * @param classLoader The class loader that should be used to load the time service manager
	 * @param logger Optionally, a logger to log actions to (may be null)
	 *
	 * @return The instantiated time service manager.
	 *
	 * @throws ClassNotFoundException Thrown if the class of the time service manager cannot be found in the class loader.
	 * @throws IllegalAccessException Thrown if the constructor of the time service manager cannot be accessed.
	 * @throws InstantiationException Thrown if failing to instantiate the time service manager.
	 */
	public static <K> InternalTimeServiceManager<K, ?> loadStateBackendFromConfig(
			Configuration config,
			ClassLoader classLoader,
			@Nullable Logger logger) throws ClassNotFoundException, IllegalAccessException, InstantiationException {

		checkNotNull(config, "config");
		checkNotNull(classLoader, "classLoader");

		final String timerServiceName = config.getString(CheckpointingOptions.TIMER_SERVICE, HEAP_NAME);

		switch (timerServiceName.toLowerCase()) {
			case HEAP_NAME:
				HeapInternalTimeServiceManager<K, ?> heapTimeServiceManager =
					new HeapInternalTimeServiceManager<>();

				if (logger != null) {
					logger.info("Internal time service manager is set to heap.");
				}

				return heapTimeServiceManager;

			case ROCKSDB_NAME:
				String managerClassName = "org.apache.flink.contrib.streaming.timerservice.RocksDBInternalTimeServiceManager";

				Class<? extends InternalTimeServiceManager> managerClass =
						Class.forName(managerClassName, false, classLoader)
								.asSubclass(InternalTimeServiceManager.class);

				InternalTimeServiceManager<K, ?> rocksdDbTimeServiceManager = managerClass.newInstance();

				if (logger != null) {
					logger.info("Loading time service manager is set to rocksdb.");
				}

				return rocksdDbTimeServiceManager;
			default:
				throw new IllegalConfigurationException("Unknown time service manager type.");
		}
	}
}

