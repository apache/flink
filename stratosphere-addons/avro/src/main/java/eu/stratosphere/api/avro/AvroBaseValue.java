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
package eu.stratosphere.api.avro;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.avro.mapred.AvroValue;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

import eu.stratosphere.types.Key;
import eu.stratosphere.util.ReflectionUtil;


public abstract class AvroBaseValue<T> extends AvroValue<T> implements Key {
	
	private static final long serialVersionUID = 1L;


	public AvroBaseValue() {}
	
	public AvroBaseValue(T datum) {
		super(datum);
	}

	
	// --------------------------------------------------------------------------------------------
	//  Serialization / Deserialization
	// --------------------------------------------------------------------------------------------
	
	private ReflectDatumWriter<T> writer;
	private ReflectDatumReader<T> reader;
	
	private DataOutputEncoder encoder;
	private DataInputDecoder decoder;
	
	
	@Override
	public void write(DataOutput out) throws IOException {
		// the null flag
		if (datum() == null) {
			out.writeBoolean(false);
		} else {
			out.writeBoolean(true);
			
			DataOutputEncoder encoder = getEncoder();
			encoder.setOut(out);
			getWriter().write(datum(), encoder);
		}
	}

	@Override
	public void read(DataInput in) throws IOException {
		// the null flag
		if (in.readBoolean()) {
			
			DataInputDecoder decoder = getDecoder();
			decoder.setIn(in);
			datum(getReader().read(datum(), decoder));
		}
	}
	
	private ReflectDatumWriter<T> getWriter() {
		if (this.writer == null) {
			@SuppressWarnings("unchecked")
			Class<T> clazz = (Class<T>) datum().getClass();
			this.writer = new ReflectDatumWriter<T>(clazz);
		}
		return this.writer;
	}
	
	private ReflectDatumReader<T> getReader() {
		if (this.reader == null) {
			Class<T> datumClass = ReflectionUtil.getTemplateType1(getClass());
			this.reader = new ReflectDatumReader<T>(datumClass);
		}
		return this.reader;
	}
	
	private DataOutputEncoder getEncoder() {
		if (this.encoder == null) {
			this.encoder = new DataOutputEncoder();
		}
		return this.encoder;
	}
	
	private DataInputDecoder getDecoder() {
		if (this.decoder == null) {
			this.decoder = new DataInputDecoder();
		}
		return this.decoder;
	}
	
	// --------------------------------------------------------------------------------------------
	//  Hashing / Equality
	// --------------------------------------------------------------------------------------------
	
	@Override
	public int hashCode() {
		return datum() == null ? 0 : datum().hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj.getClass() == this.getClass()) {
			Object otherDatum = ((AvroBaseValue<?>) obj).datum();
			Object thisDatum = datum();
			
			if (thisDatum == null) {
				return otherDatum == null;
			} else {
				return thisDatum.equals(otherDatum);
			}
		} else {
			return false;
		}
	}
	
	@Override
	public String toString() {
		return "SAvroValue (" + datum() + ")";
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public int compareTo(Key o) {
		Object otherDatum = ((AvroBaseValue<?>) o).datum();
		Object thisDatum = datum();
		
		if (thisDatum == null) {
			return otherDatum == null ? 0 : -1;
		} else {
			return otherDatum == null ? 1: ((Comparable<Object>) thisDatum).compareTo(otherDatum);
		}
	}
}
