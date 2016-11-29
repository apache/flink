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

import java.io.EOFException;
import java.io.IOException;
import java.util.List;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.operators.util.metrics.CountingCollector;
import org.apache.flink.runtime.operators.hash.InPlaceMutableHashTable;
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

	private DriverStrategy strategy;

	private InMemorySorter<T> sorter;

	private QuickSort sortAlgo = new QuickSort();

	private InPlaceMutableHashTable<T> table;

	private InPlaceMutableHashTable<T>.ReduceFacade reduceFacade;

	private List<MemorySegment> memory;

	private volatile boolean running;

	private boolean objectReuseEnabled = false;


	// ------------------------------------------------------------------------

	@Override
	public void setup(TaskContext<ReduceFunction<T>, T> context) {
		taskContext = context;
		running = true;
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
		final Counter numRecordsOut = taskContext.getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter();

		strategy = taskContext.getTaskConfig().getDriverStrategy();

		// instantiate the serializer / comparator
		final TypeSerializerFactory<T> serializerFactory = taskContext.getInputSerializer(0);
		comparator = taskContext.getDriverComparator(0);
		serializer = serializerFactory.getSerializer();
		reducer = taskContext.getStub();
		output = new CountingCollector<>(this.taskContext.getOutputCollector(), numRecordsOut);

		MemoryManager memManager = taskContext.getMemoryManager();
		final int numMemoryPages = memManager.computeNumberOfPages(
			taskContext.getTaskConfig().getRelativeMemoryDriver());
		memory = memManager.allocatePages(taskContext.getContainingTask(), numMemoryPages);

		ExecutionConfig executionConfig = taskContext.getExecutionConfig();
		objectReuseEnabled = executionConfig.isObjectReuseEnabled();

		if (LOG.isDebugEnabled()) {
			LOG.debug("ReduceCombineDriver object reuse: " + (objectReuseEnabled ? "ENABLED" : "DISABLED") + ".");
		}

		switch (strategy) {
			case SORTED_PARTIAL_REDUCE:
				// instantiate a fix-length in-place sorter, if possible, otherwise the out-of-place sorter
				if (comparator.supportsSerializationWithKeyNormalization() &&
					serializer.getLength() > 0 && serializer.getLength() <= THRESHOLD_FOR_IN_PLACE_SORTING) {
					sorter = new FixedLengthRecordSorter<T>(serializer, comparator.duplicate(), memory);
				} else {
					sorter = new NormalizedKeySorter<T>(serializer, comparator.duplicate(), memory);
				}
				break;
			case HASHED_PARTIAL_REDUCE:
				table = new InPlaceMutableHashTable<T>(serializer, comparator, memory);
				reduceFacade = table.new ReduceFacade(reducer, output, objectReuseEnabled);
				break;
			default:
				throw new Exception("Invalid strategy " + taskContext.getTaskConfig().getDriverStrategy() + " for reduce combiner.");
		}
	}

	@Override
	public void run() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Combiner starting.");
		}

		final Counter numRecordsIn = taskContext.getMetricGroup().getIOMetricGroup().getNumRecordsInCounter();

		final MutableObjectIterator<T> in = taskContext.getInput(0);
		final TypeSerializer<T> serializer = this.serializer;

		switch (strategy) {
			case SORTED_PARTIAL_REDUCE:
				if (objectReuseEnabled) {
					T value = serializer.createInstance();
					while (running && (value = in.next(value)) != null) {
						numRecordsIn.inc();

						// try writing to the sorter first
						if (sorter.write(value)) {
							continue;
						}

						// do the actual sorting, combining, and data writing
						sortAndCombine();
						sorter.reset();

						// write the value again
						if (!sorter.write(value)) {
							throw new IOException("Cannot write record to fresh sort buffer. Record too large.");
						}
					}
				} else {
					T value;
					while (running && (value = in.next()) != null) {
						numRecordsIn.inc();

						// try writing to the sorter first
						if (sorter.write(value)) {
							continue;
						}

						// do the actual sorting, combining, and data writing
						sortAndCombine();
						sorter.reset();

						// write the value again
						if (!sorter.write(value)) {
							throw new IOException("Cannot write record to fresh sort buffer. Record too large.");
						}
					}
				}

				// sort, combine, and send the final batch
				sortAndCombine();

				break;
			case HASHED_PARTIAL_REDUCE:
				table.open();
				if (objectReuseEnabled) {
					T value = serializer.createInstance();
					while (running && (value = in.next(value)) != null) {
						numRecordsIn.inc();
						try {
							reduceFacade.updateTableEntryWithReduce(value);
						} catch (EOFException ex) {
							// the table has run out of memory
							reduceFacade.emitAndReset();
							// try again
							reduceFacade.updateTableEntryWithReduce(value);
						}
					}
				} else {
					T value;
					while (running && (value = in.next()) != null) {
						numRecordsIn.inc();
						try {
							reduceFacade.updateTableEntryWithReduce(value);
						} catch (EOFException ex) {
							// the table has run out of memory
							reduceFacade.emitAndReset();
							// try again
							reduceFacade.updateTableEntryWithReduce(value);
						}
					}
				}

				// send the final batch
				reduceFacade.emit();

				table.close();
				break;
			default:
				throw new Exception("Invalid strategy " + taskContext.getTaskConfig().getDriverStrategy() + " for reduce combiner.");
		}
	}

	private void sortAndCombine() throws Exception {
		final InMemorySorter<T> sorter = this.sorter;

		if (!sorter.isEmpty()) {
			sortAlgo.sort(sorter);

			final TypeSerializer<T> serializer = this.serializer;
			final TypeComparator<T> comparator = this.comparator;
			final ReduceFunction<T> function = this.reducer;
			final Collector<T> output = this.output;
			final MutableObjectIterator<T> input = sorter.getIterator();

			if (objectReuseEnabled) {
				// We only need two objects. The first reference stores results and is
				// eventually collected. New values are read into the second.
				//
				// The output value must have the same key fields as the input values.

				T reuse1 = input.next();
				T reuse2 = serializer.createInstance();

				T value = reuse1;

				// iterate over key groups
				while (running && value != null) {
					comparator.setReference(value);

					// iterate within a key group
					while ((reuse2 = input.next(reuse2)) != null) {
						if (comparator.equalToReference(reuse2)) {
							// same group, reduce
							value = function.reduce(value, reuse2);

							// we must never read into the object returned
							// by the user, so swap the reuse objects
							if (value == reuse2) {
								T tmp = reuse1;
								reuse1 = reuse2;
								reuse2 = tmp;
							}
						} else {
							// new key group
							break;
						}
					}

					output.collect(value);

					// swap the value from the new key group into the first object
					T tmp = reuse1;
					reuse1 = reuse2;
					reuse2 = tmp;

					value = reuse1;
				}
			} else {
				T value = input.next();

				// iterate over key groups
				while (running && value != null) {
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
		try {
			if (sorter != null) {
				sorter.dispose();
			}
			if (table != null) {
				table.close();
			}
		} catch (Exception e) {
			// may happen during concurrent modification
		}
		taskContext.getMemoryManager().release(memory);
	}

	@Override
	public void cancel() {
		running = false;

		cleanup();
	}
}
