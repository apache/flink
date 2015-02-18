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
import java.util.ArrayList;
import java.util.List;

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

	private boolean disableAutoTypeRegistration = false;

	private boolean serializeGenericTypesWithAvro = false;



	// Serializers and types registered with Kryo and the PojoSerializer
	// we store them in lists to ensure they are registered in order in all kryo instances.
	private final List<Entry<Class<?>, Serializer<?>>> registeredTypesWithKryoSerializers = new ArrayList<Entry<Class<?>, Serializer<?>>>();
	private final List<Entry<Class<?>, Class<? extends Serializer<?>>>> registeredTypesWithKryoSerializerClasses = new ArrayList<Entry<Class<?>, Class<? extends Serializer<?>>>>();
	private final List<Entry<Class<?>, Serializer<?>>> defaultKryoSerializers = new ArrayList<Entry<Class<?>, Serializer<?>>>();
	private final List<Entry<Class<?>, Class<? extends Serializer<?>>>> defaultKryoSerializerClasses = new ArrayList<Entry<Class<?>, Class<? extends Serializer<?>>>>();
	private final List<Class<?>> registeredKryoTypes = new ArrayList<Class<?>>();
	private final List<Class<?>> registeredPojoTypes = new ArrayList<Class<?>>();

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

	public void enableGenericTypeSerializationWithAvro() {
		serializeGenericTypesWithAvro = true;
	}

	public void disableGenericTypeSerializationWithAvro() {
		serializeGenericTypesWithAvro = true;
	}

	public boolean serializeGenericTypesWithAvro() {
		return serializeGenericTypesWithAvro;
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
	 * Adds a new Kryo default serializer to the Runtime.
	 *
	 * Note that the serializer instance must be serializable (as defined by java.io.Serializable),
	 * because it may be distributed to the worker nodes by java serialization.
	 *
	 * @param type The class of the types serialized with the given serializer.
	 * @param serializer The serializer to use.
	 */
	public void addDefaultKryoSerializer(Class<?> type, Serializer<?> serializer) {
		if (type == null || serializer == null) {
			throw new NullPointerException("Cannot register null class or serializer.");
		}
		if (!(serializer instanceof java.io.Serializable)) {
			throw new IllegalArgumentException("The serializer instance must be serializable, (for distributing it in the cluster), "
					+ "as defined by java.io.Serializable. For stateless serializers, you can use the "
					+ "'registerSerializer(Class, Class)' method to register the serializer via its class.");
		}
		Entry<Class<?>, Serializer<?>> e = new Entry<Class<?>, Serializer<?>>(type, serializer);
		if(!defaultKryoSerializers.contains(e)) {
			defaultKryoSerializers.add(e);
		}
	}

	/**
	 * Adds a new Kryo default serializer to the Runtime.
	 *
	 * @param type The class of the types serialized with the given serializer.
	 * @param serializerClass The class of the serializer to use.
	 */
	public void addDefaultKryoSerializer(Class<?> type, Class<? extends Serializer<?>> serializerClass) {
		if (type == null || serializerClass == null) {
			throw new NullPointerException("Cannot register null class or serializer.");
		}
		Entry<Class<?>, Class<? extends Serializer<?>>> e = new Entry<Class<?>, Class<? extends Serializer<?>>>(type, serializerClass);
		if(!defaultKryoSerializerClasses.contains(e)) {
			defaultKryoSerializerClasses.add(e);
		}
	}

	/**
	 * Registers the given type with a Kryo Serializer.
	 *
	 * Note that the serializer instance must be serializable (as defined by java.io.Serializable),
	 * because it may be distributed to the worker nodes by java serialization.
	 *
	 * @param type The class of the types serialized with the given serializer.
	 * @param serializer The serializer to use.
	 */
	public void registerTypeWithKryoSerializer(Class<?> type, Serializer<?> serializer) {
		if (type == null || serializer == null) {
			throw new NullPointerException("Cannot register null class or serializer.");
		}
		if (!(serializer instanceof java.io.Serializable)) {
			throw new IllegalArgumentException("The serializer instance must be serializable, (for distributing it in the cluster), "
					+ "as defined by java.io.Serializable. For stateless serializers, you can use the "
					+ "'registerSerializer(Class, Class)' method to register the serializer via its class.");
		}
		Entry<Class<?>, Serializer<?>> e = new Entry<Class<?>, Serializer<?>>(type, serializer);
		if(!registeredTypesWithKryoSerializers.contains(e)) {
			registeredTypesWithKryoSerializers.add(e);
		}
	}

	/**
	 * Registers the given Serializer via its class as a serializer for the given type at the KryoSerializer
	 *
	 * @param type The class of the types serialized with the given serializer.
	 * @param serializerClass The class of the serializer to use.
	 */
	public void registerTypeWithKryoSerializer(Class<?> type, Class<? extends Serializer<?>> serializerClass) {
		if (type == null || serializerClass == null) {
			throw new NullPointerException("Cannot register null class or serializer.");
		}
		Entry<Class<?>, Class<? extends Serializer<?>>> e = new Entry<Class<?>, Class<? extends Serializer<?>>>(type, serializerClass);
		if(!registeredTypesWithKryoSerializerClasses.contains(e)) {
			registeredTypesWithKryoSerializerClasses.add(e);
		}
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
		if(!registeredPojoTypes.contains(type)) {
			registeredPojoTypes.add(type);
		}
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
	 * Returns the registered types with Kryo Serializers.
	 */
	public List<Entry<Class<?>, Serializer<?>>> getRegisteredTypesWithKryoSerializers() {
		return registeredTypesWithKryoSerializers;
	}

	/**
	 * Returns the registered types with their Kryo Serializer classes.
	 */
	public List<Entry<Class<?>, Class<? extends Serializer<?>>>> getRegisteredTypesWithKryoSerializerClasses() {
		return registeredTypesWithKryoSerializerClasses;
	}


	/**
	 * Returns the registered default Kryo Serializers.
	 */
	public List<Entry<Class<?>, Serializer<?>>> getDefaultKryoSerializers() {
		return defaultKryoSerializers;
	}

	/**
	 * Returns the registered default Kryo Serializer classes.
	 */
	public List<Entry<Class<?>, Class<? extends Serializer<?>>>> getDefaultKryoSerializerClasses() {
		return defaultKryoSerializerClasses;
	}

	/**
	 * Returns the registered Kryo types.
	 */
	public List<Class<?>> getRegisteredKryoTypes() {
		if (isForceKryoEnabled()) {
			// if we force kryo, we must also return all the types that
			// were previously only registered as POJO
			List<Class<?>> result = new ArrayList<Class<?>>();
			result.addAll(registeredKryoTypes);
			for(Class<?> t : registeredPojoTypes) {
				if (!result.contains(t)) {
					result.add(t);
				}
			}
			return result;
		} else {
			return registeredKryoTypes;
		}
	}

	/**
	 * Returns the registered POJO types.
	 */
	public List<Class<?>> getRegisteredPojoTypes() {
		return registeredPojoTypes;
	}


	public boolean isAutoTypeRegistrationDisabled() {
		return disableAutoTypeRegistration;
	}

	/**
	 * Control whether Flink is automatically registering all types in the user programs with
	 * Kryo.
	 *
	 */
	public void disableAutoTypeRegistration() {
		this.disableAutoTypeRegistration = false;
	}


	public static class Entry<K, V> implements Serializable {
		private final K k;
		private final V v;
		public Entry(K k, V v) {
			this.k = k;
			this.v = v;
		}

		public K getKey() {
			return k;
		}

		public V getValue() {
			return v;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			Entry entry = (Entry) o;

			if (k != null ? !k.equals(entry.k) : entry.k != null) {
				return false;
			}
			if (v != null ? !v.equals(entry.v) : entry.v != null) {
				return false;
			}

			return true;
		}

		@Override
		public int hashCode() {
			int result = k != null ? k.hashCode() : 0;
			result = 31 * result + (v != null ? v.hashCode() : 0);
			return result;
		}

		@Override
		public String toString() {
			return "Entry{" +
					"k=" + k +
					", v=" + v +
					'}';
		}
	}
}
