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

package org.apache.flink.api.common;

import java.io.Serializable;

/**
 * A configuration config for configuring behavior of the system, such as whether to use
 * the closure cleaner, object-reuse mode...
 */
public class ExecutionConfig implements Serializable {
	
	private static final long serialVersionUID = 1L;

	// Key for storing it in the Job Configuration
	public static final String CONFIG_KEY = "runtime.config";

	private boolean useClosureCleaner = true;
	private int degreeOfParallelism = -1;
	private int numberOfExecutionRetries = -1;

	// For future use...
//	private boolean forceGenericSerializer = false;
	private boolean objectReuse = false;

	/**
	 * Enables the ClosureCleaner. This analyzes user code functions and sets fields to null
	 * that are not used. This will in most cases make closures or anonymous inner classes
	 * serializable that where not serializable due to some Scala or Java implementation artifact.
	 * User code must be serializable because it needs to be sent to worker nodes.
	 */
	public ExecutionConfig enableClosureCleaner() {
		useClosureCleaner = true;
		return this;
	}

	/**
	 * Disables the ClosureCleaner. @see #enableClosureCleaner()
	 */
	public ExecutionConfig disableClosureCleaner() {
		useClosureCleaner = false;
		return this;
	}

	/**
	 * Returns whether the ClosureCleaner is enabled. @see #enableClosureCleaner()
	 */
	public boolean isClosureCleanerEnabled() {
		return useClosureCleaner;
	}

	/**
	 * Gets the degree of parallelism with which operation are executed by default. Operations can
	 * individually override this value to use a specific degree of parallelism.
	 * Other operations may need to run with a different
	 * degree of parallelism - for example calling
	 * a reduce operation over the entire
	 * set will insert eventually an operation that runs non-parallel (degree of parallelism of one).
	 *
	 * @return The degree of parallelism used by operations, unless they override that value. This method
	 *         returns {@code -1}, if the environments default parallelism should be used.
	 */

	public int getDegreeOfParallelism() {
		return degreeOfParallelism;
	}

	/**
	 * Sets the degree of parallelism (DOP) for operations executed through this environment.
	 * Setting a DOP of x here will cause all operators (such as join, map, reduce) to run with
	 * x parallel instances.
	 * <p>
	 * This method overrides the default parallelism for this environment.
	 * The local execution environment uses by default a value equal to the number of hardware
	 * contexts (CPU cores / threads). When executing the program via the command line client
	 * from a JAR file, the default degree of parallelism is the one configured for that setup.
	 *
	 * @param degreeOfParallelism The degree of parallelism
	 */

	public ExecutionConfig setDegreeOfParallelism(int degreeOfParallelism) {
		if (degreeOfParallelism < 1) {
			throw new IllegalArgumentException("Degree of parallelism must be at least one.");
		}
		this.degreeOfParallelism = degreeOfParallelism;
		return this;
	}

	/**
	 * Gets the number of times the system will try to re-execute failed tasks. A value
	 * of {@code -1} indicates that the system default value (as defined in the configuration)
	 * should be used.
	 *
	 * @return The number of times the system will try to re-execute failed tasks.
	 */
	public int getNumberOfExecutionRetries() {
		return numberOfExecutionRetries;
	}

	/**
	 * Sets the number of times that failed tasks are re-executed. A value of zero
	 * effectively disables fault tolerance. A value of {@code -1} indicates that the system
	 * default value (as defined in the configuration) should be used.
	 *
	 * @param numberOfExecutionRetries The number of times the system will try to re-execute failed tasks.
	 */
	public ExecutionConfig setNumberOfExecutionRetries(int numberOfExecutionRetries) {
		if (numberOfExecutionRetries < -1) {
			throw new IllegalArgumentException("The number of execution retries must be non-negative, or -1 (use system default)");
		}
		this.numberOfExecutionRetries = numberOfExecutionRetries;
		return this;
	}

	// These are for future use...
//	public ExecutionConfig forceGenericSerializer() {
//		forceGenericSerializer = true;
//		return this;
//	}
//
//	public ExecutionConfig disableForceGenericSerializer() {
//		forceGenericSerializer = false;
//		return this;
//	}
//
//	public boolean isForceGenericSerializerEnabled() {
//		return forceGenericSerializer;
//	}
//

	/**
	 * Enables reusing objects that Flink internally uses for deserialization and passing
	 * data to user-code functions. Keep in mind that this can lead to bugs when the
	 * user-code function of an operation is not aware of this behaviour.
	 */
	public ExecutionConfig enableObjectReuse() {
		objectReuse = true;
		return this;
	}

	/**
	 * Disables reusing objects that Flink internally uses for deserialization and passing
	 * data to user-code functions. @see #enableObjectReuse()
	 */
	public ExecutionConfig disableObjectReuse() {
		objectReuse = false;
		return this;
	}

	/**
	 * Returns whether object reuse has been enabled or disabled. @see #enableObjectReuse()
	 */
	public boolean isObjectReuseEnabled() {
		return objectReuse;
	}
}
