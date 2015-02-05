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
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.CollectionSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.google.protobuf.Message;
import com.twitter.chill.ScalaKryoInstantiator;

import com.twitter.chill.protobuf.ProtobufSerializer;
import com.twitter.chill.thrift.TBaseSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.thrift.TBase;
import scala.reflect.ClassTag;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.lang.reflect.Modifier;

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
	private static final long serialVersionUID = 2L;

	private final Class<T> type;

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
	}

	// ------------------------------------------------------------------------

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public boolean isStateful() {
		return true;
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

			// add serializers for popular other serialization frameworks
			// Google Protobuf (FLINK-1392)
			this.kryo.addDefaultSerializer(Message.class, ProtobufSerializer.class);
			// thrift
			this.kryo.addDefaultSerializer(TBase.class, TBaseSerializer.class);

			// If the type we have to serialize as a GenricType is implementing SpecificRecordBase,
			// we have to register the avro serializer
			// This rule only applies if users explicitly use the GenericTypeInformation for the avro types
			// usually, we are able to handle Avro POJOs with the POJO serializer.
			if(SpecificRecordBase.class.isAssignableFrom(type)) {
				ClassTag<SpecificRecordBase> tag = scala.reflect.ClassTag$.MODULE$.apply(type);
				this.kryo.register(type, com.twitter.chill.avro.AvroSerializer.SpecificRecordSerializer(tag));

			}
			// Avro POJOs contain java.util.List which have GenericData.Array as their runtime type
			// because Kryo is not able to serialize them properly, we use this serializer for them
			this.kryo.register(GenericData.Array.class, new SpecificInstanceCollectionSerializer(ArrayList.class));
			// We register this serializer for users who want to use untyped Avro records (GenericData.Record).
			// Kryo is able to serialize everything in there, except for the Schema.
			// This serializer is very slow, but using the GenericData.Records of Kryo is in general a bad idea.
			// we add the serializer as a default serializer because Avro is using a private sub-type at runtime.
			this.kryo.addDefaultSerializer(Schema.class, new AvroSchemaSerializer());


			// register the type of our class
			kryo.register(type);

			kryo.setRegistrationRequired(false);
			kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
		}
	}

	// --------------------------------------------------------------------------------------------
	// Custom Serializers
	// --------------------------------------------------------------------------------------------

	/**
	 * Special serializer for Java collections enforcing certain instance types.
	 * Avro is serializing collections with an "GenericData.Array" type. Kryo is not able to handle
	 * this type, so we use ArrayLists.
	 */
	public static class SpecificInstanceCollectionSerializer<T extends Collection> extends CollectionSerializer {
		Class<T> type;
		public SpecificInstanceCollectionSerializer(Class<T> type) {
			this.type = type;
		}

		@Override
		protected Collection create(Kryo kryo, Input input, Class<Collection> type) {
			return kryo.newInstance(this.type);
		}

		@Override
		protected Collection createCopy(Kryo kryo, Collection original) {
			return kryo.newInstance(this.type);
		}
	}

	/**
	 * Slow serialization approach for Avro schemas.
	 * This is only used with {{@link org.apache.avro.generic.GenericData.Record}} types.
	 * Having this serializer, we are able to handle avro Records.
	 */
	public static class AvroSchemaSerializer extends Serializer<Schema> {
		@Override
		public void write(Kryo kryo, Output output, Schema object) {
			String schemaAsString = object.toString(false);
			output.writeString(schemaAsString);
		}

		@Override
		public Schema read(Kryo kryo, Input input, Class<Schema> type) {
			String schemaAsString = input.readString();
			// the parser seems to be stateful, to we need a new one for every type.
			Schema.Parser sParser = new Schema.Parser();
			return sParser.parse(schemaAsString);
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
