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


package org.apache.flink.api.java.io;

import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flink.api.common.io.CheckpointableInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.avro.FSDataInputStreamWrapper;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.InstantiationUtil;

public class AvroInputFormat<E> extends FileInputFormat<E> implements ResultTypeQueryable<E>,
	CheckpointableInputFormat<FileInputSplit, Tuple2<Long, Long>> {
	
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(AvroInputFormat.class);

	private final Class<E> avroValueType;
	
	private boolean reuseAvroValue = true;

	private transient DataFileReader<E> dataFileReader;

	private transient FileInputSplit currSplit;

	private transient long end;

	private transient long recordsReadSinceLastSync;

	private transient long lastSync;

	private transient FileInputSplit restoredSplit;

	private transient Tuple2<Long, Long> restoredState;

	public AvroInputFormat(Path filePath, Class<E> type) {
		super(filePath);
		this.avroValueType = type;
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

	/**
	 * If set, the InputFormat will only read entire files.
	 */
	public void setUnsplittable(boolean unsplittable) {
		this.unsplittable = unsplittable;
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

		this.currSplit = split;

		SeekableInput in = new FSDataInputStreamWrapper(stream, split.getPath().getFileSystem().getFileStatus(split.getPath()).getLen());
		dataFileReader = (DataFileReader) DataFileReader.openReader(in, datumReader);
		end = split.getStart() + split.getLength();
		recordsReadSinceLastSync = 0;

		if(this.restoredState == null) {
			dataFileReader.sync(split.getStart());
			lastSync = dataFileReader.previousSync();
		} else {

			if (!this.restoredSplit.equals(split)) {
				throw new RuntimeException("Tried to open at the wrong split after recovery.");
			}

			// go to the block we stopped
			currSplit = this.restoredSplit;
			lastSync = this.restoredState.f0;
			dataFileReader.seek(lastSync);

			// read until the record we were before the checkpoint
			// and discard the values
			for(int i = 0; i < this.restoredState.f1; i++) {
				dataFileReader.next(null);
				recordsReadSinceLastSync++;
			}
		}
		this.restoredSplit = null;
		this.restoredState = null;
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return !dataFileReader.hasNext() || dataFileReader.pastSync(end);
	}

	public long getRecordsReadFromBlock() {
		return this.recordsReadSinceLastSync;
	}

	@Override
	public E nextRecord(E reuseValue) throws IOException {
		if (reachedEnd()) {
			return null;
		}

		// if we start a new block, then register the event, and
		// restart the counter.
		if(dataFileReader.previousSync() != lastSync) {
			lastSync = dataFileReader.previousSync();
			recordsReadSinceLastSync = 0;
		}

		if (!reuseAvroValue) {
			reuseValue = InstantiationUtil.instantiate(avroValueType, Object.class);
		}
		reuseValue = dataFileReader.next(reuseValue);
		recordsReadSinceLastSync++;

		return reuseValue;
	}

	// --------------------------------------------------------------------------------------------
	//  Checkpointing
	// --------------------------------------------------------------------------------------------

	@Override
	public Tuple2<FileInputSplit, Tuple2<Long, Long>> getCurrentChannelState() throws IOException {
		Tuple2<Long, Long> state = new Tuple2<>(this.lastSync, this.recordsReadSinceLastSync);
		return new Tuple2<>(currSplit, state);
	}

	@Override
	public void restore(FileInputSplit split, Tuple2<Long, Long> state) throws IOException {
		this.restoredSplit = split;
		this.restoredState = state;
	}
}
