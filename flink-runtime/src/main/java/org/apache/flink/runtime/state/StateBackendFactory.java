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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;

import java.io.IOException;

/**
 * A factory to create a specific state backend. The state backend creation gets a Configuration
 * object that can be used to read further config values.
 * 
 * <p>The state backend factory is typically specified in the configuration to produce a
 * configured state backend.
 * 
 * @param <T> The type of the state backend created.
 */
@PublicEvolving
public interface StateBackendFactory<T extends StateBackend> {

	/**
	 * Creates the state backend, optionally using the given configuration.
	 * 
	 * @param config The Flink configuration (loaded by the TaskManager).
	 * @param classLoader The class loader that should be used to load the state backend.
	 * @return The created state backend. 
	 * 
	 * @throws IllegalConfigurationException
	 *             If the configuration misses critical values, or specifies invalid values
	 * @throws IOException
	 *             If the state backend initialization failed due to an I/O exception
	 */
	T createFromConfig(Configuration config, ClassLoader classLoader) throws IllegalConfigurationException, IOException;
}
