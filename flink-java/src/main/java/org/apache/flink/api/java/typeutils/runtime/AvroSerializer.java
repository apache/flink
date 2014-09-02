/**
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

import java.io.IOException;

import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;

import com.esotericsoftware.kryo.Kryo;


/**
 * General purpose serialization. Currently using Apache Avro's Reflect-serializers for serialization and
 * Kryo for deep object copies. We want to change this to Kryo-only.
 *
 * @param <T> The type serialized.
 */
public class AvroSerializer<T> extends TypeSerializer<T> {

	private static final long serialVersionUID = 1L;
	
	private final Class<T> type;
	
	private final Class<? extends T> typeToInstantiate;
	
	private transient ReflectDatumWriter<T> writer;
	private transient ReflectDatumReader<T> reader;
	
	private transient DataOutputEncoder encoder;
	private transient DataInputDecoder decoder;
	
	private transient Kryo kryo;
	
	private transient T deepCopyInstance;
	
	// --------------------------------------------------------------------------------------------
	
	public AvroSerializer(Class<T> type) {
		this(type, type);
	}
	
	public AvroSerializer(Class<T> type, Class<? extends T> typeToInstantiate) {
		if (type == null || typeToInstantiate == null) {
			throw new NullPointerException();
		}
		
		InstantiationUtil.checkForInstantiation(typeToInstantiate);
		
		this.type = type;
		this.typeToInstantiate = typeToInstantiate;
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
		return InstantiationUtil.instantiate(this.typeToInstantiate);
	}

	@Override
	public T copy(T from, T reuse) {
		checkKryoInitialized();
		reuse = this.kryo.copy(from);
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
	
	private final void checkKryoInitialized() {
		if (this.kryo == null) {
			this.kryo = new Kryo();
			this.kryo.setAsmEnabled(true);
			this.kryo.register(type);
		}
	}
}
