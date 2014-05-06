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

package eu.stratosphere.api.java.io;

import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.core.fs.FileInputSplit;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.api.avro.FSDataInputStreamWrapper;
import eu.stratosphere.api.common.io.FileInputFormat;
import eu.stratosphere.api.java.typeutils.ResultTypeQueryable;
import eu.stratosphere.api.java.typeutils.TypeExtractor;
import eu.stratosphere.api.java.typeutils.TypeInformation;
import eu.stratosphere.util.InstantiationUtil;


public class AvroInputFormat<E> extends FileInputFormat<E> implements ResultTypeQueryable<E> {
	
	private static final long serialVersionUID = 1L;

	private static final Log LOG = LogFactory.getLog(AvroInputFormat.class);
	
	
	private final Class<E> avroValueType;
	
	private boolean reuseAvroValue = true;
	

	private transient FileReader<E> dataFileReader;

	
	public AvroInputFormat(Path filePath, Class<E> type) {
		super(filePath);
		this.avroValueType = type;
		this.unsplittable = true;
	}
	
	
	/**
	 * Sets the flag whether to reuse the Avro value instance for all records.
	 * By default, the input format reuses the Avro value.
	 *
	 * @param reuseAvroValue True, if the input format should reuse the Avro value instance, false otherwise.
	 */
	public void setReuseAvroValue(boolean reuseAvroValue) {
		this.reuseAvroValue = reuseAvroValue;
	}
	
	// --------------------------------------------------------------------------------------------
	// Typing
	// --------------------------------------------------------------------------------------------
	
	@Override
	public TypeInformation<E> getProducedType() {
		return TypeExtractor.getForClass(this.avroValueType);
	}
	
	// --------------------------------------------------------------------------------------------
	// Input Format Methods
	// --------------------------------------------------------------------------------------------

	@Override
	public void open(FileInputSplit split) throws IOException {
		super.open(split);

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
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return !dataFileReader.hasNext();
	}

	@Override
	public E nextRecord(E reuseValue) throws IOException {
		if (!dataFileReader.hasNext()) {
			return null;
		}
		
		if (!reuseAvroValue) {
			reuseValue = InstantiationUtil.instantiate(avroValueType, Object.class);
		}
		
		reuseValue = dataFileReader.next(reuseValue);
		return reuseValue;
	}
}
