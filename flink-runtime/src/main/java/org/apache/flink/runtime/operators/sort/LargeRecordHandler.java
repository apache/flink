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

package org.apache.flink.runtime.operators.sort;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.runtime.RuntimeSerializerFactory;
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.FileChannelInputView;
import org.apache.flink.runtime.io.disk.FileChannelOutputView;
import org.apache.flink.runtime.io.disk.InputViewIterator;
import org.apache.flink.runtime.io.disk.SeekableFileChannelInputView;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.types.NullKeyFieldException;
import org.apache.flink.util.MutableObjectIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkArgument;

public class LargeRecordHandler<T> {
	
	private static final Logger LOG = LoggerFactory.getLogger(LargeRecordHandler.class);
	
	private static final int MIN_SEGMENTS_FOR_KEY_SPILLING = 1;
	
	private static final int MAX_SEGMENTS_FOR_KEY_SPILLING = 4;
	
	// --------------------------------------------------------------------------------------------
	
	private final TypeSerializer<T> serializer;
	
	private final TypeComparator<T> comparator;
	
	private TupleSerializer<Tuple> keySerializer;
	
	private TupleComparator<Tuple> keyComparator;
	
	private FileChannelOutputView recordsOutFile;
	
	private FileChannelOutputView keysOutFile;
	
	private Tuple keyTuple;
	
	private FileChannelInputView keysReader;
	
	private SeekableFileChannelInputView recordsReader;
	
	private FileIOChannel.ID recordsChannel;
	
	private FileIOChannel.ID keysChannel;
	
	private final IOManager ioManager;
	
	private final MemoryManager memManager;
	
	private final List<MemorySegment> memory;
	
	private TypeSerializerFactory<Tuple> keySerializerFactory;
	
	private UnilateralSortMerger<Tuple> keySorter;
	
	private final AbstractInvokable memoryOwner;
	
	private long recordCounter;
	
	private int numKeyFields;
	
	private final int maxFilehandles;
	
	private volatile boolean closed;

	private final ExecutionConfig executionConfig;

	// --------------------------------------------------------------------------------------------
	
	public LargeRecordHandler(TypeSerializer<T> serializer, TypeComparator<T> comparator, 
			IOManager ioManager, MemoryManager memManager, List<MemorySegment> memory,
			AbstractInvokable memoryOwner, int maxFilehandles)
	{
		this.serializer = checkNotNull(serializer);
		this.comparator = checkNotNull(comparator);
		this.ioManager = checkNotNull(ioManager);
		this.memManager = checkNotNull(memManager);
		this.memory = checkNotNull(memory);
		this.memoryOwner = checkNotNull(memoryOwner);
		this.maxFilehandles = maxFilehandles;

		this.executionConfig = memoryOwner.getExecutionConfig();

		checkArgument(maxFilehandles >= 2);
	}
	
	
	// --------------------------------------------------------------------------------------------
	
	@SuppressWarnings("unchecked")
	public long addRecord(T record) throws IOException {

		if (recordsOutFile == null) {
			
			if (closed) {
				throw new IllegalStateException("The large record handler has been closed.");
			}
			if (recordsReader != null) {
				throw new IllegalStateException("The handler has already switched to sorting.");
			}
			
			LOG.debug("Initializing the large record spilling...");
			
			// initialize the utilities
			{
				final TypeComparator<?>[] keyComps = comparator.getFlatComparators();
				numKeyFields = keyComps.length;
				Object[] keyHolder = new Object[numKeyFields];
				
				comparator.extractKeys(record, keyHolder, 0);
				
				TypeSerializer<?>[] keySers = new TypeSerializer<?>[numKeyFields];
				TypeSerializer<?>[] tupleSers = new TypeSerializer<?>[numKeyFields + 1];
				
				int[] keyPos = new int[numKeyFields];
				
				for (int i = 0; i < numKeyFields; i++) {
					keyPos[i] = i;
					keySers[i] = createSerializer(keyHolder[i], i);
					tupleSers[i] = keySers[i];
				}
				// add the long serializer for the offset
				tupleSers[numKeyFields] = LongSerializer.INSTANCE;
				
				keySerializer = new TupleSerializer<Tuple>((Class<Tuple>) Tuple.getTupleClass(numKeyFields+1), tupleSers);
				keyComparator = new TupleComparator<Tuple>(keyPos, keyComps, keySers);
				
				keySerializerFactory = new RuntimeSerializerFactory<Tuple>(keySerializer, keySerializer.getTupleClass());

				keyTuple = keySerializer.createInstance();
			}
			
			// initialize the spilling
			final int totalNumSegments = memory.size();
			final int segmentsForKeys = (totalNumSegments >= 2*MAX_SEGMENTS_FOR_KEY_SPILLING) ? MAX_SEGMENTS_FOR_KEY_SPILLING : 
				Math.max(MIN_SEGMENTS_FOR_KEY_SPILLING, totalNumSegments - MAX_SEGMENTS_FOR_KEY_SPILLING);
				
			List<MemorySegment> recordsMemory = new ArrayList<MemorySegment>();
			List<MemorySegment> keysMemory = new ArrayList<MemorySegment>();
			
			for (int i = 0; i < segmentsForKeys; i++) {
				keysMemory.add(memory.get(i));
			}
			for (int i = segmentsForKeys; i < totalNumSegments; i++) {
				recordsMemory.add(memory.get(i));
			}
			
			recordsChannel = ioManager.createChannel();
			keysChannel = ioManager.createChannel();
			
			recordsOutFile = new FileChannelOutputView(
					ioManager.createBlockChannelWriter(recordsChannel), memManager,
					recordsMemory, memManager.getPageSize());
			
			keysOutFile = new FileChannelOutputView(
					ioManager.createBlockChannelWriter(keysChannel), memManager,
					keysMemory, memManager.getPageSize());
		}
		
		final long offset = recordsOutFile.getWriteOffset();
		if (offset < 0) {
			throw new RuntimeException("wrong offset");
		}
		
		Object[] keyHolder = new Object[numKeyFields];
		
		comparator.extractKeys(record, keyHolder, 0);
		for (int i = 0; i < numKeyFields; i++) {
			keyTuple.setField(keyHolder[i], i);
		}
		keyTuple.setField(offset, numKeyFields);
		
		keySerializer.serialize(keyTuple, keysOutFile);
		serializer.serialize(record, recordsOutFile);
		
		recordCounter++;
		
		return offset;
	}
	
	public MutableObjectIterator<T> finishWriteAndSortKeys(List<MemorySegment> memory) throws IOException {
		if (recordsOutFile == null || keysOutFile == null) {
			throw new IllegalStateException("The LargeRecordHandler has not spilled any records");
		}
		
		// close the writers and 
		final int lastBlockBytesKeys;
		final int lastBlockBytesRecords;
		
		recordsOutFile.close();
		keysOutFile.close();
		lastBlockBytesKeys = keysOutFile.getBytesInLatestSegment();
		lastBlockBytesRecords = recordsOutFile.getBytesInLatestSegment();
		recordsOutFile = null;
		keysOutFile = null;
		
		final int pagesForReaders = Math.max(3*MIN_SEGMENTS_FOR_KEY_SPILLING, Math.min(2*MAX_SEGMENTS_FOR_KEY_SPILLING, memory.size() / 50));
		final int pagesForKeyReader = Math.min(pagesForReaders - MIN_SEGMENTS_FOR_KEY_SPILLING, MAX_SEGMENTS_FOR_KEY_SPILLING);
		final int pagesForRecordReader = pagesForReaders - pagesForKeyReader;
		
		// grab memory for the record reader
		ArrayList<MemorySegment> memForRecordReader = new ArrayList<MemorySegment>();
		ArrayList<MemorySegment> memForKeysReader = new ArrayList<MemorySegment>();
		
		for (int i = 0; i < pagesForRecordReader; i++) {
			memForRecordReader.add(memory.remove(memory.size() - 1));
		}
		for (int i = 0; i < pagesForKeyReader; i++) {
			memForKeysReader.add(memory.remove(memory.size() - 1));
		}
		
		keysReader = new FileChannelInputView(ioManager.createBlockChannelReader(keysChannel),
				memManager, memForKeysReader, lastBlockBytesKeys);
		InputViewIterator<Tuple> keyIterator = new InputViewIterator<Tuple>(keysReader, keySerializer);
		
		keySorter = new UnilateralSortMerger<Tuple>(memManager, memory, ioManager, 
				keyIterator, memoryOwner, keySerializerFactory, keyComparator, 1, maxFilehandles, 1.0f, false,
				this.executionConfig.isObjectReuseEnabled());

		// wait for the sorter to sort the keys
		MutableObjectIterator<Tuple> result;
		try {
			result = keySorter.getIterator();
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
		
		recordsReader = new SeekableFileChannelInputView(ioManager, recordsChannel, memManager, memForRecordReader, lastBlockBytesRecords);
		
		return new FetchingIterator<T>(serializer, result, recordsReader, keySerializer, numKeyFields);
	}
	
	/**
	 * Closes all structures and deletes all temporary files.
	 * Even in the presence of failures, this method will try and continue closing
	 * files and deleting temporary files.
	 * 
	 * @throws IOException Thrown if an error occurred while closing/deleting the files.
	 */
	public void close() throws IOException {
		
		// we go on closing and deleting files in the presence of failures.
		// we remember the first exception to occur and re-throw it later
		Throwable ex = null;
		
		synchronized (this) {
			
			if (closed) {
				return;
			}
			closed = true;
			
			// close the writers
			if (recordsOutFile != null) {
				try {
					recordsOutFile.close();
					recordsOutFile = null;
				} catch (Throwable t) {
					LOG.error("Cannot close the large records spill file.", t);
					ex = ex == null ? t : ex;
				}
			}
			if (keysOutFile != null) {
				try {
					keysOutFile.close();
					keysOutFile = null;
				} catch (Throwable t) {
					LOG.error("Cannot close the large records key spill file.", t);
					ex = ex == null ? t : ex;
				}
			}
			
			// close the readers
			if (recordsReader != null) {
				try {
					recordsReader.close();
					recordsReader = null;
				} catch (Throwable t) {
					LOG.error("Cannot close the large records reader.", t);
					ex = ex == null ? t : ex;
				}
			}
			if (keysReader != null) {
				try {
					keysReader.close();
					keysReader = null;
				} catch (Throwable t) {
					LOG.error("Cannot close the large records key reader.", t);
					ex = ex == null ? t : ex;
				}
			}
			
			// delete the spill files
			if (recordsChannel != null) {
				try {
					ioManager.deleteChannel(recordsChannel);
					recordsChannel = null;
				} catch (Throwable t) {
					LOG.error("Cannot delete the large records spill file.", t);
					ex = ex == null ? t : ex;
				}
			}
			if (keysChannel != null) {
				try {
					ioManager.deleteChannel(keysChannel);
					keysChannel = null;
				} catch (Throwable t) {
					LOG.error("Cannot delete the large records key spill file.", t);
					ex = ex == null ? t : ex;
				}
			}
			
			// close the key sorter
			if (keySorter != null) {
				try {
					keySorter.close();
					keySorter = null;
				} catch (Throwable t) {
					LOG.error("Cannot properly dispose the key sorter and clean up its temporary files.", t);
					ex = ex == null ? t : ex;
				}
			}
			
			memManager.release(memory);
			
			recordCounter = 0;
		}
		
		// re-throw the exception, if necessary
		if (ex != null) { 
			throw new IOException("An error occurred cleaning up spill files in the large record handler.", ex);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	public boolean hasData() {
		return recordCounter > 0;
	}
	
	// --------------------------------------------------------------------------------------------
	
	private TypeSerializer<Object> createSerializer(Object key, int pos) {
		if (key == null) {
			throw new NullKeyFieldException(pos);
		}
		try {
			TypeInformation<Object> info = TypeExtractor.getForObject(key);
			return info.createSerializer(executionConfig);
		}
		catch (Throwable t) {
			throw new RuntimeException("Could not create key serializer for type " + key);
		}
	}
	
	private static final class FetchingIterator<T> implements MutableObjectIterator<T> {
		
		private final TypeSerializer<T> serializer;
		
		private final MutableObjectIterator<Tuple> tupleInput;
		
		private final SeekableFileChannelInputView recordsInputs;
		
		private Tuple value;
		
		private final int pointerPos;
		
		
		public FetchingIterator(TypeSerializer<T> serializer, MutableObjectIterator<Tuple> tupleInput,
				SeekableFileChannelInputView recordsInputs, TypeSerializer<Tuple> tupleSerializer, int pointerPos) {
			this.serializer = serializer;
			this.tupleInput = tupleInput;
			this.recordsInputs = recordsInputs;
			this.pointerPos = pointerPos;
			
			this.value = tupleSerializer.createInstance();
		}

		@Override
		public T next(T reuse) throws IOException {
			return next();
		}

		@Override
		public T next() throws IOException {
			Tuple value = tupleInput.next(this.value);
			if (value != null) {
				this.value = value;
				long pointer = value.<Long>getField(pointerPos);

				recordsInputs.seek(pointer);
				return serializer.deserialize(recordsInputs);
			} else {
				return null;
			}
		}
	}
}
