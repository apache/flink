/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.java.typeutils.runtime;

import java.io.IOException;

import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;

import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import eu.stratosphere.util.InstantiationUtil;


/**
 * General purpose serialization using Apache Avro's Reflect-serializers.
 * For deep object copies, this class is using Kryo.
 *
 * @param <T> The type serialized.
 */
public class AvroSerializer<T> extends TypeSerializer<T> {

	private static final long serialVersionUID = 1L;
	
	private final Class<T> type;
	
	private transient ReflectDatumWriter<T> writer;
	private transient ReflectDatumReader<T> reader;
	
	private transient DataOutputEncoder encoder;
	private transient DataInputDecoder decoder;
	
	private transient Kryo kryo;
	
	private transient Serializer<T> serializer;
	
	private transient T deepCopyInstance;
	
	// --------------------------------------------------------------------------------------------
	
	public AvroSerializer(Class<T> type) {
		this.type = type;
	}

	// --------------------------------------------------------------------------------------------
	
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
		return InstantiationUtil.instantiate(type, Object.class);
	}

	@Override
	public T copy(T from, T reuse) {
		checkKryoInitialized();
		reuse = serializer.copy(this.kryo, from);
		return reuse;
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(T value, DataOutputView target) throws IOException {
		checkAvroInitialized();
		this.encoder.setOut(target);
		this.writer.write(value, this.encoder);
	}

	@Override
	public T deserialize(T reuse, DataInputView source) throws IOException {
		checkAvroInitialized();
		this.decoder.setIn(source);
		return this.reader.read(reuse, this.decoder);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		checkAvroInitialized();
		
		if (this.deepCopyInstance == null) {
			this.deepCopyInstance = InstantiationUtil.instantiate(type, Object.class);
		}
		
		this.decoder.setIn(source);
		this.encoder.setOut(target);
		
		T tmp = this.reader.read(this.deepCopyInstance, this.decoder);
		this.writer.write(tmp, this.encoder);
	}
	
	
	private final void checkAvroInitialized() {
		if (this.reader == null) {
			this.reader = new ReflectDatumReader<T>(type);
			this.writer = new ReflectDatumWriter<T>(type);
			this.encoder = new DataOutputEncoder();
			this.decoder = new DataInputDecoder();
		}
	}
	
	@SuppressWarnings("unchecked")
	private final void checkKryoInitialized() {
		if (this.kryo == null) {
			this.kryo = new Kryo();
			this.kryo.setAsmEnabled(true);
			this.kryo.register(type);
			this.serializer = this.kryo.getSerializer(this.type);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	// serialization
	// --------------------------------------------------------------------------------------------
	
	private void readObject(java.io.ObjectInputStream s) throws java.io.IOException, ClassNotFoundException {
		// read basic object and the type
		s.defaultReadObject();
		
		this.reader = null;
		this.writer = null;
		this.encoder = null;
		this.decoder = null;
		this.kryo = null;
	}
}
