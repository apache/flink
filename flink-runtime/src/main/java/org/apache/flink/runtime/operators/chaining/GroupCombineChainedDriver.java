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

package org.apache.flink.runtime.operators.chaining;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeComparatorFactory;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.BatchTask;
import org.apache.flink.runtime.operators.sort.FixedLengthRecordSorter;
import org.apache.flink.runtime.operators.sort.InMemorySorter;
import org.apache.flink.runtime.operators.sort.NormalizedKeySorter;
import org.apache.flink.runtime.operators.sort.QuickSort;
import org.apache.flink.runtime.util.NonReusingKeyGroupedIterator;
import org.apache.flink.runtime.util.ReusingKeyGroupedIterator;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Chained variant of the GroupReduceCombineDriver
 * 
 * Acts like a combiner with a custom output type OUT.
 *
 * Sorting and reducing of the elements is performed individually for each partition without data exchange. This may
 * lead to a partial group reduce.
 *  
 * @param <IN> The data type consumed
 * @param <OUT> The data type produced
 */
public class GroupCombineChainedDriver<IN, OUT> extends ChainedDriver<IN, OUT> {

	private static final Logger LOG = LoggerFactory.getLogger(GroupCombineChainedDriver.class);

	/** Fix length records with a length below this threshold will be in-place sorted, if possible. */
	private static final int THRESHOLD_FOR_IN_PLACE_SORTING = 32;

	// --------------------------------------------------------------------------------------------

	private InMemorySorter<IN> sorter;

	private GroupReduceFunction<IN, OUT> reducer;

	private TypeSerializer<IN> serializer;

	private TypeComparator<IN> groupingComparator;

	private AbstractInvokable parent;

	private QuickSort sortAlgo = new QuickSort();

	private List<MemorySegment> memory;

	private volatile boolean running = true;

	// --------------------------------------------------------------------------------------------

	@Override
	public void setup(AbstractInvokable parent) {
		this.parent = parent;

		@SuppressWarnings("unchecked")
		final GroupReduceFunction<IN, OUT> combiner =
			BatchTask.instantiateUserCode(this.config, userCodeClassLoader, GroupReduceFunction.class);
		this.reducer = combiner;
		FunctionUtils.setFunctionRuntimeContext(combiner, getUdfRuntimeContext());
	}

	@Override
	public void openTask() throws Exception {
		// open the stub first
		final Configuration stubConfig = this.config.getStubParameters();
		BatchTask.openUserCode(this.reducer, stubConfig);

		// ----------------- Set up the sorter -------------------------

		// instantiate the serializer / comparator
		final TypeSerializerFactory<IN> serializerFactory = this.config.getInputSerializer(0, this.userCodeClassLoader);
		final TypeComparatorFactory<IN> sortingComparatorFactory = this.config.getDriverComparator(0, this.userCodeClassLoader);
		final TypeComparatorFactory<IN> groupingComparatorFactory = this.config.getDriverComparator(1, this.userCodeClassLoader);
		this.serializer = serializerFactory.getSerializer();
		
		TypeComparator<IN> sortingComparator = sortingComparatorFactory.createComparator();
		this.groupingComparator = groupingComparatorFactory.createComparator();

		MemoryManager memManager = this.parent.getEnvironment().getMemoryManager();
		final int numMemoryPages = memManager.computeNumberOfPages(this.config.getRelativeMemoryDriver());
		this.memory = memManager.allocatePages(this.parent, numMemoryPages);

		// instantiate a fix-length in-place sorter, if possible, otherwise the out-of-place sorter
		if (sortingComparator.supportsSerializationWithKeyNormalization() &&
			this.serializer.getLength() > 0 && this.serializer.getLength() <= THRESHOLD_FOR_IN_PLACE_SORTING)
		{
			this.sorter = new FixedLengthRecordSorter<IN>(this.serializer, sortingComparator.duplicate(), memory);
		} else {
			this.sorter = new NormalizedKeySorter<IN>(this.serializer, sortingComparator.duplicate(), memory);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("SynchronousChainedCombineDriver object reuse: " + (this.objectReuseEnabled ? "ENABLED" : "DISABLED") + ".");
		}
	}

	@Override
	public void closeTask() throws Exception {
		if (this.sorter != null) {
			this.sorter.dispose();
		}
		this.parent.getEnvironment().getMemoryManager().release(this.memory);

		if (this.running) {
			BatchTask.closeUserCode(this.reducer);
		}
	}

	@Override
	public void cancelTask() {
		this.running = false;

		if (this.sorter != null) {
			try {
				this.sorter.dispose();
			}
			catch (Exception e) {
				// may happen during concurrent modification
			}
		}
		
		this.parent.getEnvironment().getMemoryManager().release(this.memory);
	}

	// --------------------------------------------------------------------------------------------

	public Function getStub() {
		return this.reducer;
	}

	public String getTaskName() {
		return this.taskName;
	}

	@Override
	public void collect(IN record) {
		numRecordsIn.inc();
		// try writing to the sorter first
		try {
			if (this.sorter.write(record)) {
				return;
			}

			// do the actual sorting
			sortAndReduce();
			this.sorter.reset();
			
			if (!this.sorter.write(record)) {
				throw new IOException("Cannot write record to fresh sort buffer. Record too large.");
			}
		}
		catch (Exception e) {
			throw new ExceptionInChainedStubException(this.taskName, e);
		}
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public void close() {
		try {
			sortAndReduce();
		} catch (Exception e) {
			throw new ExceptionInChainedStubException(this.taskName, e);
		}

		this.outputCollector.close();
	}

	private void sortAndReduce() throws Exception {
		final InMemorySorter<IN> sorter = this.sorter;

		if (objectReuseEnabled) {
			if (!sorter.isEmpty()) {
				this.sortAlgo.sort(sorter);
				// run the reducer
				final ReusingKeyGroupedIterator<IN> keyIter = new ReusingKeyGroupedIterator<IN>(
						sorter.getIterator(), this.serializer, this.groupingComparator);
				
				// cache references on the stack
				final GroupReduceFunction<IN, OUT> stub = this.reducer;
				final Collector<OUT> output = this.outputCollector;

				// run stub implementation
				while (this.running && keyIter.nextKey()) {
					stub.reduce(keyIter.getValues(), output);
				}
			}
		} else {
			if (!sorter.isEmpty()) {
				this.sortAlgo.sort(sorter);
				// run the reducer
				final NonReusingKeyGroupedIterator<IN> keyIter = new NonReusingKeyGroupedIterator<IN>(
						sorter.getIterator(), this.groupingComparator);


				// cache references on the stack
				final GroupReduceFunction<IN, OUT> stub = this.reducer;
				final Collector<OUT> output = this.outputCollector;

				// run stub implementation
				while (this.running && keyIter.nextKey()) {
					stub.reduce(keyIter.getValues(), output);
				}
			}
		}
	}
}
