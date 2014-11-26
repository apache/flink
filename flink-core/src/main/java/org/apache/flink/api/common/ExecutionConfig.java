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

import com.esotericsoftware.kryo.Serializer;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
	private boolean forceKryo = false;

	private boolean objectReuse = false;


	// Serializers and types registered with Kryo and the PojoSerializer
	private final Map<Class<?>, Serializer<?>> registeredKryoSerializers = new HashMap<Class<?>, Serializer<?>>();
	private final Map<Class<?>, Class<? extends Serializer<?>>> registeredKryoSerializersClasses = new HashMap<Class<?>, Class<? extends Serializer<?>>>();
	private final Set<Class<?>> registeredKryoTypes = new HashSet<Class<?>>();
	private final Set<Class<?>> registeredPojoTypes = new HashSet<Class<?>>();

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

	/**
	 * Force TypeExtractor to use Kryo serializer for POJOS even though we could analyze as POJO.
	 * In some cases this might be preferable. For example, when using interfaces
	 * with subclasses that cannot be analyzed as POJO.
	 */
	public void enableForceKryo() {
		forceKryo = true;
	}


	/**
	 * Disable use of Kryo serializer for all POJOs.
	 */
	public void disableForceKryo() {
		forceKryo = false;
	}

	public boolean isForceKryoEnabled() {
		return forceKryo;
	}

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

	// --------------------------------------------------------------------------------------------
	//  Registry for types and serializers
	// --------------------------------------------------------------------------------------------

	/**
	 * Registers the given Serializer as a default serializer for the given type at the
	 * {@link org.apache.flink.api.common.typeutils.runtime.KryoSerializer}.
	 *
	 * Note that the serializer instance must be serializable (as defined by java.io.Serializable),
	 * because it may be distributed to the worker nodes by java serialization.
	 *
	 * @param type The class of the types serialized with the given serializer.
	 * @param serializer The serializer to use.
	 */
	public void registerKryoSerializer(Class<?> type, Serializer<?> serializer) {
		if (type == null || serializer == null) {
			throw new NullPointerException("Cannot register null class or serializer.");
		}
		if (!(serializer instanceof java.io.Serializable)) {
			throw new IllegalArgumentException("The serializer instance must be serializable, (for distributing it in the cluster), "
					+ "as defined by java.io.Serializable. For stateless serializers, you can use the "
					+ "'registerSerializer(Class, Class)' method to register the serializer via its class.");
		}

		registeredKryoSerializers.put(type, serializer);
	}

	/**
	 * Registers the given Serializer via its class as a serializer for the given type at the
	 * {@link org.apache.flink.api.common.typeutils.runtime.KryoSerializer}.
	 *
	 * @param type The class of the types serialized with the given serializer.
	 * @param serializerClass The class of the serializer to use.
	 */
	public void registerKryoSerializer(Class<?> type, Class<? extends Serializer<?>> serializerClass) {
		if (type == null || serializerClass == null) {
			throw new NullPointerException("Cannot register null class or serializer.");
		}

		registeredKryoSerializersClasses.put(type, serializerClass);
	}

	/**
	 * Registers the given type with the serialization stack. If the type is eventually
	 * serialized as a POJO, then the type is registered with the POJO serializer. If the
	 * type ends up being serialized with Kryo, then it will be registered at Kryo to make
	 * sure that only tags are written.
	 *
	 * @param type The class of the type to register.
	 */
	public void registerPojoType(Class<?> type) {
		if (type == null) {
			throw new NullPointerException("Cannot register null type class.");
		}
		registeredPojoTypes.add(type);
	}

	/**
	 * Registers the given type with the serialization stack. If the type is eventually
	 * serialized as a POJO, then the type is registered with the POJO serializer. If the
	 * type ends up being serialized with Kryo, then it will be registered at Kryo to make
	 * sure that only tags are written.
	 *
	 * @param type The class of the type to register.
	 */
	public void registerKryoType(Class<?> type) {
		if (type == null) {
			throw new NullPointerException("Cannot register null type class.");
		}
		registeredKryoTypes.add(type);
	}

	/**
	 * Returns the registered Kryo Serializers.
	 */
	public Map<Class<?>, Serializer<?>> getRegisteredKryoSerializers() {
		return registeredKryoSerializers;
	}

	/**
	 * Returns the registered Kryo Serializer classes.
	 */
	public Map<Class<?>, Class<? extends Serializer<?>>> getRegisteredKryoSerializersClasses() {
		return registeredKryoSerializersClasses;
	}

	/**
	 * Returns the registered Kryo types.
	 */
	public Set<Class<?>> getRegisteredKryoTypes() {
		if (isForceKryoEnabled()) {
			// if we force kryo, we must also return all the types that
			// were previously only registered as POJO
			Set<Class<?>> result = new HashSet<Class<?>>();
			result.addAll(registeredKryoTypes);
			result.addAll(registeredPojoTypes);
			return result;
		} else {
			return registeredKryoTypes;
		}
	}

	/**
	 * Returns the registered POJO types.
	 */
	public Set<Class<?>> getRegisteredPojoTypes() {
		return registeredPojoTypes;
	}
}
