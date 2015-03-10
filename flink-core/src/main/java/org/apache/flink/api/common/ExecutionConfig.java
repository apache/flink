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
 * A config to define the behavior of the program execution. It allows to define (among other
 * options) the following settings:
 *
 * <ul>
 *     <li>The default parallelism of the program, i.e., how many parallel tasks to use for
 *         all functions that do not define a specific value directly.</li>
 *     <li>The number of retries in the case of failed executions.</li>
 *     <li>The {@link ExecutionMode} of the program: Batch or Pipelined.
 *         The default execution mode is {@link ExecutionMode#PIPELINED}</li>
 *     <li>Enabling or disabling the "closure cleaner". The closure cleaner pre-processes
 *         the implementations of functions. In case they are (anonymous) inner classes,
 *         it removes unused references to the enclosing class to fix certain serialization-related
 *         problems and to reduce the size of the closure.</li>
 *     <li>The config allows to register types and serializers to increase the efficiency of
 *         handling <i>generic types</i> and <i>POJOs</i>. This is usually only needed
 *         when the functions return not only the types declared in their signature, but
 *         also subclasses of those types.</li>
 * </ul>
 */
public class ExecutionConfig implements Serializable {
	
	private static final long serialVersionUID = 1L;

	// Key for storing it in the Job Configuration
	public static final String CONFIG_KEY = "runtime.config";

	/**
	 * The constant to use for the parallelism, if the system should use the number
	 *  of currently available slots.
	 */
	public static final int PARALLELISM_AUTO_MAX = Integer.MAX_VALUE;

	// --------------------------------------------------------------------------------------------

	/** Defines how data exchange happens - batch or pipelined */
	private ExecutionMode executionMode = ExecutionMode.PIPELINED;

	private boolean useClosureCleaner = true;

	private int parallelism = -1;

	private int numberOfExecutionRetries = -1;

	private boolean forceKryo = false;

	private boolean objectReuse = false;

	private boolean disableAutoTypeRegistration = false;

	private boolean forceAvro = false;

	/** If set to true, progress updates are printed to System.out during execution */
	private boolean printProgressDuringExecution = true;

	// Serializers and types registered with Kryo and the PojoSerializer
	// we store them in lists to ensure they are registered in order in all kryo instances.

	private final List<Entry<Class<?>, Serializer<?>>> registeredTypesWithKryoSerializers =
			new ArrayList<Entry<Class<?>, Serializer<?>>>();

	private final List<Entry<Class<?>, Class<? extends Serializer<?>>>> registeredTypesWithKryoSerializerClasses =
			new ArrayList<Entry<Class<?>, Class<? extends Serializer<?>>>>();

	private final List<Entry<Class<?>, Serializer<?>>> defaultKryoSerializers =
			new ArrayList<Entry<Class<?>, Serializer<?>>>();

	private final List<Entry<Class<?>, Class<? extends Serializer<?>>>> defaultKryoSerializerClasses =
			new ArrayList<Entry<Class<?>, Class<? extends Serializer<?>>>>();

	private final List<Class<?>> registeredKryoTypes = new ArrayList<Class<?>>();

	private final List<Class<?>> registeredPojoTypes = new ArrayList<Class<?>>();

	// --------------------------------------------------------------------------------------------

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
	 * Disables the ClosureCleaner.
	 *
	 * @see #enableClosureCleaner()
	 */
	public ExecutionConfig disableClosureCleaner() {
		useClosureCleaner = false;
		return this;
	}

	/**
	 * Returns whether the ClosureCleaner is enabled.
	 *
	 * @see #enableClosureCleaner()
	 */
	public boolean isClosureCleanerEnabled() {
		return useClosureCleaner;
	}

	/**
	 * Gets the parallelism with which operation are executed by default. Operations can
	 * individually override this value to use a specific parallelism.
	 *
	 * Other operations may need to run with a different parallelism - for example calling
	 * a reduce operation over the entire data set will involve an operation that runs
	 * with a parallelism of one (the final reduce to the single result value).
	 *
	 * @return The parallelism used by operations, unless they override that value. This method
	 *         returns {@code -1}, if the environment's default parallelism should be used.
	 * @deprecated Please use {@link #getParallelism}
	 */
	@Deprecated
	public int getDegreeOfParallelism() {
		return getParallelism();
	}

	/**
	 * Gets the parallelism with which operation are executed by default. Operations can
	 * individually override this value to use a specific parallelism.
	 *
	 * Other operations may need to run with a different parallelism - for example calling
	 * a reduce operation over the entire data set will involve an operation that runs
	 * with a parallelism of one (the final reduce to the single result value).
	 *
	 * @return The parallelism used by operations, unless they override that value. This method
	 *         returns {@code -1}, if the environment's default parallelism should be used.
	 */
	public int getParallelism() {
		return parallelism;
	}

	/**
	 * Sets the parallelism for operations executed through this environment.
	 * Setting a parallelism of x here will cause all operators (such as join, map, reduce) to run with
	 * x parallel instances.
	 * <p>
	 * This method overrides the default parallelism for this environment.
	 * The local execution environment uses by default a value equal to the number of hardware
	 * contexts (CPU cores / threads). When executing the program via the command line client
	 * from a JAR file, the default parallelism is the one configured for that setup.
	 *
	 * @param parallelism The parallelism to use
	 * @deprecated Please use {@link #setParallelism}
	 */
	@Deprecated
	public ExecutionConfig setDegreeOfParallelism(int parallelism) {
		return setParallelism(parallelism);
	}

	/**
	 * Sets the parallelism for operations executed through this environment.
	 * Setting a parallelism of x here will cause all operators (such as join, map, reduce) to run with
	 * x parallel instances.
	 * <p>
	 * This method overrides the default parallelism for this environment.
	 * The local execution environment uses by default a value equal to the number of hardware
	 * contexts (CPU cores / threads). When executing the program via the command line client
	 * from a JAR file, the default parallelism is the one configured for that setup.
	 *
	 * @param parallelism The parallelism to use
	 */
	public ExecutionConfig setParallelism(int parallelism) {
		if (parallelism < 1 && parallelism != -1) {
			throw new IllegalArgumentException(
					"Parallelism must be at least one, or -1 (use system default).");
		}
		this.parallelism = parallelism;
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
			throw new IllegalArgumentException(
					"The number of execution retries must be non-negative, or -1 (use system default)");
		}
		this.numberOfExecutionRetries = numberOfExecutionRetries;
		return this;
	}

	/**
	 * Sets the execution mode to execute the program. The execution mode defines whether
	 * data exchanges are performed in a batch or on a pipelined manner.
	 *
	 * The default execution mode is {@link ExecutionMode#PIPELINED}.
	 *
	 * @param executionMode The execution mode to use.
	 */
	public void setExecutionMode(ExecutionMode executionMode) {
		this.executionMode = executionMode;
	}

	/**
	 * Gets the execution mode used to execute the program. The execution mode defines whether
	 * data exchanges are performed in a batch or on a pipelined manner.
	 *
	 * The default execution mode is {@link ExecutionMode#PIPELINED}.
	 *
	 * @return The execution mode for the program.
	 */
	public ExecutionMode getExecutionMode() {
		return executionMode;
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
	 * Force Flink to use the AvroSerializer for POJOs.
	 */
	public void enableForceAvro() {
		forceAvro = true;
	}

	public void disableForceAvro() {
		forceAvro = false;
	}

	public boolean isForceAvroEnabled() {
		return forceAvro;
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

	/**
	 * Enables the printing of progress update messages to {@code System.out}
	 * 
	 * @return The ExecutionConfig object, to allow for function chaining.
	 */
	public ExecutionConfig enableSysoutLogging() {
		this.printProgressDuringExecution = true;
		return this;
	}

	/**
	 * Disables the printing of progress update messages to {@code System.out}
	 *
	 * @return The ExecutionConfig object, to allow for function chaining.
	 */
	public ExecutionConfig disableSysoutLogging() {
		this.printProgressDuringExecution = false;
		return this;
	}

	/**
	 * Gets whether progress update messages should be printed to {@code System.out}
	 * 
	 * @return True, if progress update messages should be printed, false otherwise.
	 */
	public boolean isSysoutLoggingEnabled() {
		return this.printProgressDuringExecution;
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

		private static final long serialVersionUID = 1L;

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
