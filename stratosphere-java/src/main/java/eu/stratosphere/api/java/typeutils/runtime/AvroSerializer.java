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

import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import eu.stratosphere.util.InstantiationUtil;


public class AvroSerializer<T> extends TypeSerializer<T> {

	private static final long serialVersionUID = 1L;
	
	private transient ReflectDatumWriter<T> writer;
	private transient ReflectDatumReader<T> reader;
	
	private transient DataOutputEncoder encoder = new DataOutputEncoder();
	private transient DataInputDecoder decoder = new DataInputDecoder();

	private final Class<T> type;
	
	// --------------------------------------------------------------------------------------------
	
	public AvroSerializer(Class<T> type) {
		this.type = type;
		
		this.writer = new ReflectDatumWriter<T>(type);
		this.reader = new ReflectDatumReader<T>(type);
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
		throw new UnsupportedOperationException();
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(T value, DataOutputView target) throws IOException {
		this.encoder.setOut(target);
		this.writer.write(value, this.encoder);
	}

	@Override
	public T deserialize(T reuse, DataInputView source) throws IOException {
		this.decoder.setIn(source);
		return this.reader.read(reuse, this.decoder);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		throw new UnsupportedOperationException();
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public int hashCode() {
		return type.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		return obj != null && (obj instanceof AvroSerializer) && ((AvroSerializer<?>) obj).type == this.type;
	}
	
	// --------------------------------------------------------------------------------------------
	// serialization
	// --------------------------------------------------------------------------------------------
	
	private void readObject(java.io.ObjectInputStream s) throws java.io.IOException, ClassNotFoundException {
		// read basic object and the type
		s.defaultReadObject();
		
		this.reader = new ReflectDatumReader<T>(type);
		this.writer = new ReflectDatumWriter<T>(type);
		this.encoder = new DataOutputEncoder();
		this.decoder = new DataInputDecoder();
	}
}
