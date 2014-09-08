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


package org.apache.flink.api.java.record.io.avro;

import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.avro.AvroBaseValue;
import org.apache.flink.api.avro.FSDataInputStreamWrapper;
import org.apache.flink.api.java.record.io.FileInputFormat;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.types.Record;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.ReflectionUtil;


public class AvroInputFormat<E> extends FileInputFormat {
	
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(AvroInputFormat.class);
	
	
	private final Class<? extends AvroBaseValue<E>> avroWrapperTypeClass;
	
	private final Class<E> avroValueType;
	

	private transient FileReader<E> dataFileReader;
	
	private transient E reuseAvroValue;
	
	private transient AvroBaseValue<E> wrapper;
	
	
	public AvroInputFormat(Class<? extends AvroBaseValue<E>> wrapperClass) {
		this.avroWrapperTypeClass = wrapperClass;
		this.avroValueType = ReflectionUtil.getTemplateType1(wrapperClass);
		this.unsplittable = true;
	}
	
	public AvroInputFormat(Class<? extends AvroBaseValue<E>> wrapperClass, Class<E> avroType) {
		this.avroValueType = avroType;
		this.avroWrapperTypeClass = wrapperClass;
		this.unsplittable = true;
	}

	@Override
	public void open(FileInputSplit split) throws IOException {
		super.open(split);
		
		this.wrapper = InstantiationUtil.instantiate(avroWrapperTypeClass, AvroBaseValue.class);
		
		DatumReader<E> datumReader;
		if (org.apache.avro.specific.SpecificRecordBase.class.isAssignableFrom(avroValueType)) {
			datumReader = new SpecificDatumReader<E>(avroValueType);
		} else {
			datumReader = new ReflectDatumReader<E>(avroValueType);
		}
		
		LOG.info("Opening split " + split);
		
		SeekableInput in = new FSDataInputStreamWrapper(stream, (int) split.getLength());
		
		dataFileReader = DataFileReader.openReader(in, datumReader);
		dataFileReader.sync(split.getStart());
		
		reuseAvroValue = null;
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return !dataFileReader.hasNext();
	}

	@Override
	public Record nextRecord(Record record) throws IOException {
		if (!dataFileReader.hasNext()) {
			return null;
		}
		
		reuseAvroValue = dataFileReader.next(reuseAvroValue);
		wrapper.datum(reuseAvroValue);
		record.setField(0, wrapper);
		return record;
	}
}
