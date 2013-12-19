/***********************************************************************************************************************
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
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.task.chaining;

import eu.stratosphere.api.common.functions.Function;
import eu.stratosphere.api.common.functions.GenericReducer;
import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypeComparatorFactory;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.common.typeutils.TypeSerializerFactory;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.runtime.sort.FixedLengthRecordSorter;
import eu.stratosphere.pact.runtime.sort.InMemorySorter;
import eu.stratosphere.pact.runtime.sort.NormalizedKeySorter;
import eu.stratosphere.pact.runtime.sort.QuickSort;
import eu.stratosphere.pact.runtime.task.RegularPactTask;
import eu.stratosphere.pact.runtime.util.KeyGroupedIterator;
import eu.stratosphere.util.Collector;

import java.io.IOException;
import java.util.List;

public class SynchronousChainedCombineDriver<T> extends ChainedDriver<T, T> {

	/**
	 * Fix length records with a length below this threshold will be in-place sorted, if possible.
	 */
	private static final int THRESHOLD_FOR_IN_PLACE_SORTING = 32;

	// --------------------------------------------------------------------------------------------

	private InMemorySorter<T> sorter;

	private GenericReducer<T, ?> combiner;

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
		final GenericReducer<T, ?> combiner =
			RegularPactTask.instantiateUserCode(this.config, userCodeClassLoader, GenericReducer.class);
		this.combiner = combiner;
		combiner.setRuntimeContext(getRuntimeContext(parent, this.taskName));
	}

	@Override
	public void openTask() throws Exception {
		// open the stub first
		final Configuration stubConfig = this.config.getStubParameters();
		RegularPactTask.openUserCode(this.combiner, stubConfig);

		// ----------------- Set up the asynchronous sorter -------------------------

		this.memManager = this.parent.getEnvironment().getMemoryManager();
		final long availableMemory = this.config.getMemoryDriver();

		// instantiate the serializer / comparator
		final TypeSerializerFactory<T> serializerFactory = this.config.getInputSerializer(0, this.userCodeClassLoader);
		final TypeComparatorFactory<T> comparatorFactory = this.config.getDriverComparator(0, this.userCodeClassLoader);
		this.serializer = serializerFactory.getSerializer();
		this.comparator = comparatorFactory.createComparator();

		final List<MemorySegment> memory = this.memManager.allocatePages(this.parent, availableMemory);

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

		if (!this.running)
			return;

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
	}

	private void sortAndCombine() throws Exception {
		final InMemorySorter<T> sorter = this.sorter;

		if (!sorter.isEmpty()) {
			this.sortAlgo.sort(sorter);
			// run the combiner
			final KeyGroupedIterator<T> keyIter = new KeyGroupedIterator<T>(sorter.getIterator(), this.serializer,
				this.comparator);

			// cache references on the stack
			final GenericReducer<T, ?> stub = this.combiner;
			final Collector<T> output = this.outputCollector;

			// run stub implementation
			while (this.running && keyIter.nextKey()) {
				stub.combine(keyIter.getValues(), output);
			}
		}
	}
}