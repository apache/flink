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

package org.apache.flink.runtime.schedule;

import org.apache.flink.configuration.Configuration;

import java.io.Serializable;

/**
 * Configuration class which holds scheduling related configuration.
 */
public class SchedulingConfig implements Serializable {

	private static final long serialVersionUID = -2497684322680066372L;

	// the actual configuration holding the values
	private final Configuration config;

	private final ClassLoader userClassLoader;

	/**
	 * Creates a new SchedulingConfig that wraps the given configuration.
	 *
	 * @param config The configuration holding the actual values.
	 * @param userClassLoader to deserialize user defined objects
	 */
	public SchedulingConfig(Configuration config, ClassLoader userClassLoader) {
		this.config = config;
		this.userClassLoader = userClassLoader;
	}

	/**
	 * Gets the configuration that holds the actual values encoded.
	 *
	 * @return The configuration that holds the actual values
	 */
	public Configuration getConfiguration() {
		return this.config;
	}

	/**
	 * Gets the class loader that have context of user classes and can help to deserialize user defined objects.
	 *
	 * @return userClassLoader to deserialize user defined objects
	 */
	public ClassLoader getUserClassLoader() {
		return this.userClassLoader;
	}

}
