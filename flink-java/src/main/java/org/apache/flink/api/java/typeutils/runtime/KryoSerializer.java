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

package org.apache.flink.api.java.typeutils.runtime;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.factories.ReflectionSerializerFactory;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.twitter.chill.ScalaKryoInstantiator;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A type serializer that serializes its type using the Kryo serialization
 * framework (https://github.com/EsotericSoftware/kryo).
 * 
 * This serializer is intended as a fallback serializer for the cases that are
 * not covered by the basic types, tuples, and POJOs.
 *
 * @param <T> The type to be serialized.
 */
public class KryoSerializer<T> extends TypeSerializer<T> {
	
	private static final long serialVersionUID = 3L;

	private static Map<Class<?>, Serializer<?>> staticRegisteredSerializers = new HashMap<Class<?>, Serializer<?>>();
	private static Map<Class<?>, Class<? extends Serializer<?>>> staticRegisteredSerializersClasses = new HashMap<Class<?>, Class<? extends Serializer<?>>>();
	
	private static Set<Class<?>> staticRegisteredTypes = new HashSet<Class<?>>();
	
	// ------------------------------------------------------------------------
	
	private final Map<Class<?>, Serializer<?>> registeredSerializers;
	private final Map<Class<?>, Class<? extends Serializer<?>>> registeredSerializersClasses;
	private final Set<Class<?>> registeredTypes;

	private final Class<T> type;
	
	// ------------------------------------------------------------------------
	// The fields below are lazily initialized after duplication or deserialization.

	private transient Kryo kryo;
	private transient T copyInstance;
	
	private transient DataOutputView previousOut;
	private transient DataInputView previousIn;
	
	private transient Input input;
	private transient Output output;
	
	// ------------------------------------------------------------------------

	public KryoSerializer(Class<T> type){
		if(type == null){
			throw new NullPointerException("Type class cannot be null.");
		}
		this.type = type;

		// create copies of the statically registered serializers
		// we use static synchronization to safeguard against concurrent use
		// of the static collections.
		synchronized (KryoSerializer.class) {
			this.registeredSerializers = staticRegisteredSerializers.isEmpty() ?
				Collections.<Class<?>, Serializer<?>>emptyMap() :
				new HashMap<Class<?>, Serializer<?>>(staticRegisteredSerializers);
		
			this.registeredSerializersClasses = staticRegisteredSerializersClasses.isEmpty() ?
				Collections.<Class<?>, Class<? extends Serializer<?>>>emptyMap() :
				new HashMap<Class<?>, Class<? extends Serializer<?>>>(staticRegisteredSerializersClasses);
				
			this.registeredTypes = staticRegisteredTypes.isEmpty() ?
				Collections.<Class<?>>emptySet() :
				new HashSet<Class<?>>(staticRegisteredTypes);
		}
		
	}

	/**
	 * Copy-constructor that does not copy transient fields. They will be initialized once required.
	 */
	protected KryoSerializer(KryoSerializer<T> toCopy) {
		registeredSerializers = toCopy.registeredSerializers;
		registeredSerializersClasses = toCopy.registeredSerializersClasses;
		registeredTypes = toCopy.registeredTypes;

		type = toCopy.type;
		if(type == null){
			throw new NullPointerException("Type class cannot be null.");
		}
	}

	// ------------------------------------------------------------------------

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public KryoSerializer<T> duplicate() {
		return new KryoSerializer<T>(this);
	}

	@Override
	public T createInstance() {
		if(Modifier.isAbstract(type.getModifiers()) || Modifier.isInterface(type.getModifiers()) ) {
			return null;
		} else {
			checkKryoInitialized();
			try {
				return kryo.newInstance(type);
			} catch(Throwable e) {
				return null;
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public T copy(T from) {
		if (from == null) {
			return null;
		}
		checkKryoInitialized();
		try {
			return kryo.copy(from);
		}
		catch(KryoException ke) {
			// kryo was unable to copy it, so we do it through serialization:
			ByteArrayOutputStream baout = new ByteArrayOutputStream();
			Output output = new Output(baout);

			kryo.writeObject(output, from);

			output.close();

			ByteArrayInputStream bain = new ByteArrayInputStream(baout.toByteArray());
			Input input = new Input(bain);

			return (T)kryo.readObject(input, from.getClass());
		}
	}
	
	@Override
	public T copy(T from, T reuse) {
		return copy(from);
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(T record, DataOutputView target) throws IOException {
		checkKryoInitialized();
		if (target != previousOut) {
			DataOutputViewStream outputStream = new DataOutputViewStream(target);
			output = new Output(outputStream);
			previousOut = target;
		}

		try {
			kryo.writeClassAndObject(output, record);
			output.flush();
		}
		catch (KryoException ke) {
			Throwable cause = ke.getCause();
			if (cause instanceof EOFException) {
				throw (EOFException) cause;
			}
			else {
				throw ke;
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public T deserialize(DataInputView source) throws IOException {
		checkKryoInitialized();
		if (source != previousIn) {
			DataInputViewStream inputStream = new DataInputViewStream(source);
			input = new NoFetchingInput(inputStream);
			previousIn = source;
		}
		return (T) kryo.readClassAndObject(input);
	}
	
	@Override
	public T deserialize(T reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		checkKryoInitialized();
		if(this.copyInstance == null){
			this.copyInstance = createInstance();
		}

		T tmp = deserialize(copyInstance, source);
		serialize(tmp, target);
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public int hashCode() {
		return type.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj != null && obj instanceof KryoSerializer) {
			KryoSerializer<?> other = (KryoSerializer<?>) obj;
			return other.type == this.type;
		} else {
			return false;
		}
	}
	
	// --------------------------------------------------------------------------------------------

	private void checkKryoInitialized() {
		if (this.kryo == null) {
			this.kryo = new ScalaKryoInstantiator().newKryo();

			// Throwable and all subclasses should be serialized via java serialization
			kryo.addDefaultSerializer(Throwable.class, new JavaSerializer());

			// register the type of our class
			kryo.register(type);
			
			// register given types. we do this first so that any registration of a
			// more specific serializer overrides this
			for (Class<?> type : registeredTypes) {
				kryo.register(type);
			}
			
			// register given serializer classes
			for (Map.Entry<Class<?>, Class<? extends Serializer<?>>> e : registeredSerializersClasses.entrySet()) {
				Class<?> typeClass = e.getKey();
				Class<? extends Serializer<?>> serializerClass = e.getValue();
				
				Serializer<?> serializer = 
						ReflectionSerializerFactory.makeSerializer(kryo, serializerClass, typeClass);
				kryo.register(typeClass, serializer);
			}

			// register given serializers
			for (Map.Entry<Class<?>, Serializer<?>> e : registeredSerializers.entrySet()) {
				kryo.register(e.getKey(), e.getValue());
			}
			
			kryo.setRegistrationRequired(false);
			kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
		}
	}
	
	// --------------------------------------------------------------------------------------------
	// For registering custom serializers and types
	// --------------------------------------------------------------------------------------------

	/**
	 * Registers the given Serializer as a default serializer for the given class at the Kryo
	 * instance.
	 * Note that the serializer instance must be serializable (as defined by java.io.Serializable),
	 * because it may be distributed to the worker nodes by java serialization.
	 * 
	 * @param clazz The class of the types serialized with the given serializer.
	 * @param serializer The serializer to use.
	 * @throws IllegalArgumentException Thrown, if the serializer is not serializable.
	 */
	public static void registerSerializer(Class<?> clazz, Serializer<?> serializer) {
		if (clazz == null || serializer == null) {
			throw new NullPointerException("Cannot register null class or serializer.");
		}
		if (!(serializer instanceof java.io.Serializable)) {
			throw new IllegalArgumentException("The serializer instance must be serializable, (for distributing it in the cluster), "
					+ "as defined by java.io.Serializable. For stateless serializers, you can use the "
					+ "'registerSerializer(Class, Class)' method to register the serializer via its class.");
		}
		
		synchronized (KryoSerializer.class) {
			staticRegisteredSerializers.put(clazz, serializer);
		}
	}

	/**
	 * Registers a serializer via its class as a default serializer for the given class at the Kryo
	 * instance.
	 * 
	 * @param clazz The class of the types serialized with the given serializer.
	 * @param serializerClass The serializer to use.
	 */
	public static void registerSerializer(Class<?> clazz, Class<? extends Serializer<?>> serializerClass) {
		if (clazz == null || serializerClass == null) {
			throw new NullPointerException("Cannot register null class or serializer.");
		}
		
		synchronized (KryoSerializer.class) {
			staticRegisteredSerializersClasses.put(clazz, serializerClass);
		}
	}
	
	/**
	 * Registers the given type with Kryo. Registering the type allows Kryo to write abbreviated
	 * name tags, rather than full class names, thereby vastly increasing the serialization
	 * performance in many cases.
	 *  
	 * @param type The class of the type to register.
	 */
	public static void registerType(Class<?> type) {
		if (type == null) {
			throw new NullPointerException("Cannot register null type class.");
		}
		
		synchronized (KryoSerializer.class) {
			staticRegisteredTypes.add(type);
		}
	}

	// --------------------------------------------------------------------------------------------
	// For testing
	// --------------------------------------------------------------------------------------------
	
	Kryo getKryo() {
		checkKryoInitialized();
		return this.kryo;
	}
}
