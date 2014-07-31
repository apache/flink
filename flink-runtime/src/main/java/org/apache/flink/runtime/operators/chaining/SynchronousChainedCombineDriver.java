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


package org.apache.flink.runtime.operators.chaining;

import java.io.IOException;
import java.util.List;

import org.apache.flink.api.common.functions.FlatCombineFunction;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeComparatorFactory;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memorymanager.MemoryManager;
import org.apache.flink.runtime.operators.RegularPactTask;
import org.apache.flink.runtime.operators.sort.FixedLengthRecordSorter;
import org.apache.flink.runtime.operators.sort.InMemorySorter;
import org.apache.flink.runtime.operators.sort.NormalizedKeySorter;
import org.apache.flink.runtime.operators.sort.QuickSort;
import org.apache.flink.runtime.util.KeyGroupedIterator;
import org.apache.flink.util.Collector;

public class SynchronousChainedCombineDriver<T> extends ChainedDriver<T, T> {

	/**
	 * Fix length records with a length below this threshold will be in-place sorted, if possible.
	 */
	private static final int THRESHOLD_FOR_IN_PLACE_SORTING = 32;

	// --------------------------------------------------------------------------------------------

	private InMemorySorter<T> sorter;

	private FlatCombineFunction<T> combiner;

	private TypeSerializer<T> serializer;

	private TypeComparator<T> comparator;

	private AbstractInvokable parent;

	private QuickSort sortAlgo = new QuickSort();

	private MemoryManager memManager;

	private volatile boolean running = true;

	// --------------------------------------------------------------------------------------------

	@Override
	public void setup(AbstractInvokable parent) {
		this.parent = parent;

		@SuppressWarnings("unchecked")
		final FlatCombineFunction<T> combiner =
			RegularPactTask.instantiateUserCode(this.config, userCodeClassLoader, FlatCombineFunction.class);
		this.combiner = combiner;
		FunctionUtils.setFunctionRuntimeContext(combiner, getUdfRuntimeContext());
	}

	@Override
	public void openTask() throws Exception {
		// open the stub first
		final Configuration stubConfig = this.config.getStubParameters();
		RegularPactTask.openUserCode(this.combiner, stubConfig);

		// ----------------- Set up the asynchronous sorter -------------------------

		this.memManager = this.parent.getEnvironment().getMemoryManager();
		final int numMemoryPages = memManager.computeNumberOfPages(this.config.getRelativeMemoryDriver());

		// instantiate the serializer / comparator
		final TypeSerializerFactory<T> serializerFactory = this.config.getInputSerializer(0, this.userCodeClassLoader);
		final TypeComparatorFactory<T> comparatorFactory = this.config.getDriverComparator(0, this.userCodeClassLoader);
		this.serializer = serializerFactory.getSerializer();
		this.comparator = comparatorFactory.createComparator();

		final List<MemorySegment> memory = this.memManager.allocatePages(this.parent, numMemoryPages);

		// instantiate a fix-length in-place sorter, if possible, otherwise the out-of-place sorter
		if (this.comparator.supportsSerializationWithKeyNormalization() &&
			this.serializer.getLength() > 0 && this.serializer.getLength() <= THRESHOLD_FOR_IN_PLACE_SORTING)
		{
			this.sorter = new FixedLengthRecordSorter<T>(this.serializer, this.comparator, memory);
		} else {
			this.sorter = new NormalizedKeySorter<T>(this.serializer, this.comparator.duplicate(), memory);
		}
	}

	@Override
	public void closeTask() throws Exception {
		this.memManager.release(this.sorter.dispose());

		if (!this.running) {
			return;
		}

		RegularPactTask.closeUserCode(this.combiner);
	}

	@Override
	public void cancelTask() {
		this.running = false;
		this.memManager.release(this.sorter.dispose());
	}

	// --------------------------------------------------------------------------------------------

	public Function getStub() {
		return this.combiner;
	}

	public String getTaskName() {
		return this.taskName;
	}

	@Override
	public void collect(T record) {
		// try writing to the sorter first
		try {
			if (this.sorter.write(record)) {
				return;
			}
		} catch (IOException e) {
			throw new ExceptionInChainedStubException(this.taskName, e);
		}

		// do the actual sorting
		try {
			sortAndCombine();
		} catch (Exception e) {
			throw new ExceptionInChainedStubException(this.taskName, e);
		}
		this.sorter.reset();

		try {
			if (!this.sorter.write(record)) {
				throw new IOException("Cannot write record to fresh sort buffer. Record too large.");
			}
		} catch (IOException e) {
			throw new ExceptionInChainedStubException(this.taskName, e);
		}
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public void close() {
		try {
			sortAndCombine();
		} catch (Exception e) {
			throw new ExceptionInChainedStubException(this.taskName, e);
		}

		this.outputCollector.close();
	}

	private void sortAndCombine() throws Exception {
		final InMemorySorter<T> sorter = this.sorter;

		if (!sorter.isEmpty()) {
			this.sortAlgo.sort(sorter);
			// run the combiner
			final KeyGroupedIterator<T> keyIter = new KeyGroupedIterator<T>(sorter.getIterator(), this.serializer,
				this.comparator);

			// cache references on the stack
			final FlatCombineFunction<T> stub = this.combiner;
			final Collector<T> output = this.outputCollector;

			// run stub implementation
			while (this.running && keyIter.nextKey()) {
				stub.combine(keyIter.getValues(), output);
			}
		}
	}
}
