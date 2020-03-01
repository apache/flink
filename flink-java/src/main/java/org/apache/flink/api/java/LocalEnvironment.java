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

package org.apache.flink.api.java;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An {@link ExecutionEnvironment} that runs the program locally, multi-threaded, in the JVM where the
 * environment is instantiated.
 *
 * <p>When this environment is instantiated, it uses a default parallelism of {@code 1}. The default
 * parallelism can be set via {@link #setParallelism(int)}.
 *
 * <p>Local environments can also be instantiated through {@link ExecutionEnvironment#createLocalEnvironment()}
 * and {@link ExecutionEnvironment#createLocalEnvironment(int)}. The former version will pick a
 * default parallelism equal to the number of hardware contexts in the local machine.
 */
@Public
public class LocalEnvironment extends ExecutionEnvironment {

	/**
	 * Creates a new local environment.
	 */
	public LocalEnvironment() {
		this(new Configuration());
	}

	/**
	 * Creates a new local environment that configures its local executor with the given configuration.
	 *
	 * @param config The configuration used to configure the local executor.
	 */
	public LocalEnvironment(Configuration config) {
		super(validateAndGetConfiguration(config));
	}

	private static Configuration validateAndGetConfiguration(final Configuration configuration) {
		if (!ExecutionEnvironment.areExplicitEnvironmentsAllowed()) {
			throw new InvalidProgramException(
					"The LocalEnvironment cannot be instantiated when running in a pre-defined context " +
							"(such as Command Line Client, Scala Shell, or TestEnvironment)");
		}

		final Configuration effectiveConfiguration = new Configuration(checkNotNull(configuration));
		effectiveConfiguration.set(DeploymentOptions.TARGET, "local");
		effectiveConfiguration.set(DeploymentOptions.ATTACHED, true);
		return effectiveConfiguration;
	}

	@Override
	public String toString() {
		return "Local Environment (parallelism = " + (getParallelism() == ExecutionConfig.PARALLELISM_DEFAULT ? "default" : getParallelism()) + ").";
	}
}
