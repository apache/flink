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


package org.apache.flink.runtime.operators;

import java.io.IOException;
import java.util.List;

import org.apache.flink.api.common.ExecutionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.sort.FixedLengthRecordSorter;
import org.apache.flink.runtime.operators.sort.InMemorySorter;
import org.apache.flink.runtime.operators.sort.NormalizedKeySorter;
import org.apache.flink.runtime.operators.sort.QuickSort;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

/**
 * Combine operator for Reduce functions, standalone (not chained).
 * Sorts and groups and reduces data, but never spills the sort. May produce multiple
 * partially aggregated groups.
 * 
 * @param <T> The data type consumed and produced by the combiner.
 */
public class ReduceCombineDriver<T> implements Driver<ReduceFunction<T>, T> {
	
	private static final Logger LOG = LoggerFactory.getLogger(ReduceCombineDriver.class);

	/** Fix length records with a length below this threshold will be in-place sorted, if possible. */
	private static final int THRESHOLD_FOR_IN_PLACE_SORTING = 32;
	
	
	private TaskContext<ReduceFunction<T>, T> taskContext;

	private TypeSerializer<T> serializer;

	private TypeComparator<T> comparator;
	
	private ReduceFunction<T> reducer;
	
	private Collector<T> output;
	
	private InMemorySorter<T> sorter;
	
	private QuickSort sortAlgo = new QuickSort();

	private List<MemorySegment> memory;

	private boolean running;

	private boolean objectReuseEnabled = false;


	// ------------------------------------------------------------------------

	@Override
	public void setup(TaskContext<ReduceFunction<T>, T> context) {
		this.taskContext = context;
		this.running = true;
	}
	
	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public Class<ReduceFunction<T>> getStubType() {
		@SuppressWarnings("unchecked")
		final Class<ReduceFunction<T>> clazz = (Class<ReduceFunction<T>>) (Class<?>) ReduceFunction.class;
		return clazz;
	}

	@Override
	public int getNumberOfDriverComparators() {
		return 1;
	}

	@Override
	public void prepare() throws Exception {
		if (this.taskContext.getTaskConfig().getDriverStrategy() != DriverStrategy.SORTED_PARTIAL_REDUCE) {
			throw new Exception("Invalid strategy " + this.taskContext.getTaskConfig().getDriverStrategy() + " for reduce combiner.");
		}
		
		// instantiate the serializer / comparator
		final TypeSerializerFactory<T> serializerFactory = this.taskContext.getInputSerializer(0);
		this.comparator = this.taskContext.getDriverComparator(0);
		this.serializer = serializerFactory.getSerializer();
		this.reducer = this.taskContext.getStub();
		this.output = this.taskContext.getOutputCollector();

		MemoryManager memManager = this.taskContext.getMemoryManager();
		final int numMemoryPages = memManager.computeNumberOfPages(
				this.taskContext.getTaskConfig().getRelativeMemoryDriver());
		this.memory = memManager.allocatePages(this.taskContext.getOwningNepheleTask(), numMemoryPages);

		// instantiate a fix-length in-place sorter, if possible, otherwise the out-of-place sorter
		if (this.comparator.supportsSerializationWithKeyNormalization() &&
			this.serializer.getLength() > 0 && this.serializer.getLength() <= THRESHOLD_FOR_IN_PLACE_SORTING)
		{
			this.sorter = new FixedLengthRecordSorter<T>(this.serializer, this.comparator, memory);
		} else {
			this.sorter = new NormalizedKeySorter<T>(this.serializer, this.comparator.duplicate(), memory);
		}

		ExecutionConfig executionConfig = taskContext.getExecutionConfig();
		this.objectReuseEnabled = executionConfig.isObjectReuseEnabled();

		if (LOG.isDebugEnabled()) {
			LOG.debug("ReduceCombineDriver object reuse: " + (this.objectReuseEnabled ? "ENABLED" : "DISABLED") + ".");
		}
	}

	@Override
	public void run() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Combiner starting.");
		}
		
		final MutableObjectIterator<T> in = this.taskContext.getInput(0);
		final TypeSerializer<T> serializer = this.serializer;
		
		if (objectReuseEnabled) {
			T value = serializer.createInstance();
		
			while (running && (value = in.next(value)) != null) {
				
				// try writing to the sorter first
				if (this.sorter.write(value)) {
					continue;
				}
		
				// do the actual sorting, combining, and data writing
				sortAndCombine();
				this.sorter.reset();
				
				// write the value again
				if (!this.sorter.write(value)) {
					throw new IOException("Cannot write record to fresh sort buffer. Record too large.");
				}
			}
		}
		else {
			T value;
			while (running && (value = in.next()) != null) {

				// try writing to the sorter first
				if (this.sorter.write(value)) {
					continue;
				}

				// do the actual sorting, combining, and data writing
				sortAndCombine();
				this.sorter.reset();

				// write the value again
				if (!this.sorter.write(value)) {
					throw new IOException("Cannot write record to fresh sort buffer. Record too large.");
				}
			}
		}
		
		// sort, combine, and send the final batch
		sortAndCombine();
	}
		
	private void sortAndCombine() throws Exception {
		final InMemorySorter<T> sorter = this.sorter;

		if (!sorter.isEmpty()) {
			this.sortAlgo.sort(sorter);
			
			final TypeSerializer<T> serializer = this.serializer;
			final TypeComparator<T> comparator = this.comparator;
			final ReduceFunction<T> function = this.reducer;
			final Collector<T> output = this.output;
			final MutableObjectIterator<T> input = sorter.getIterator();

			if (objectReuseEnabled) {
				// We only need two objects. The user function is expected to return
				// the first input as the result. The output value is also expected
				// to have the same key fields as the input elements.

				T reuse1 = serializer.createInstance();
				T reuse2 = serializer.createInstance();

				T value = input.next(reuse1);

				// iterate over key groups
				while (this.running && value != null) {
					comparator.setReference(value);
					T res = value;

					// iterate within a key group
					while ((value = input.next(reuse2)) != null) {
						if (comparator.equalToReference(value)) {
							// same group, reduce
							res = function.reduce(res, value);
							if (res == reuse2) {
								T tmp = reuse1;
								reuse1 = reuse2;
								reuse2 = tmp;
							}
						} else {
							// new key group
							break;
						}
					}

					output.collect(res);

					if (value != null) {
						value = serializer.copy(value, reuse1);
					}
				}
			} else {
				T value = input.next();

				// iterate over key groups
				while (this.running && value != null) {
					comparator.setReference(value);
					T res = value;

					// iterate within a key group
					while ((value = input.next()) != null) {
						if (comparator.equalToReference(value)) {
							// same group, reduce
							res = function.reduce(res, value);
						} else {
							// new key group
							break;
						}
					}

					output.collect(res);
				}
			}
		}
	}

	@Override
	public void cleanup() {
		this.sorter.dispose();
		this.taskContext.getMemoryManager().release(this.memory);
	}

	@Override
	public void cancel() {
		this.running = false;
		
		try {
			this.sorter.dispose();
		}
		catch (Exception e) {
			// may happen during concurrent modifications
		}

		this.taskContext.getMemoryManager().release(this.memory);
	}
}
