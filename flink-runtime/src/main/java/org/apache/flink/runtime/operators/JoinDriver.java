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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.operators.hash.NonReusingBuildFirstHashMatchIterator;
import org.apache.flink.runtime.operators.hash.NonReusingBuildSecondHashMatchIterator;
import org.apache.flink.runtime.operators.sort.NonReusingMergeInnerJoinIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparatorFactory;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memorymanager.MemoryManager;
import org.apache.flink.runtime.operators.hash.ReusingBuildFirstHashMatchIterator;
import org.apache.flink.runtime.operators.hash.ReusingBuildSecondHashMatchIterator;
import org.apache.flink.runtime.operators.sort.ReusingMergeInnerJoinIterator;
import org.apache.flink.runtime.operators.util.JoinTaskIterator;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

/**
 * The join driver implements the logic of a join operator at runtime. It instantiates either
 * hash or sort-merge based strategies to find joining pairs of records.
 * 
 * @see org.apache.flink.api.common.functions.FlatJoinFunction
 */
public class JoinDriver<IT1, IT2, OT> implements PactDriver<FlatJoinFunction<IT1, IT2, OT>, OT> {
	
	protected static final Logger LOG = LoggerFactory.getLogger(JoinDriver.class);
	
	protected PactTaskContext<FlatJoinFunction<IT1, IT2, OT>, OT> taskContext;
	
	private volatile JoinTaskIterator<IT1, IT2, OT> joinIterator; // the iterator that does the actual join 
	
	protected volatile boolean running;

	// ------------------------------------------------------------------------

	@Override
	public void setup(PactTaskContext<FlatJoinFunction<IT1, IT2, OT>, OT> context) {
		this.taskContext = context;
		this.running = true;
	}

	@Override
	public int getNumberOfInputs() {
		return 2;
	}

	@Override
	public Class<FlatJoinFunction<IT1, IT2, OT>> getStubType() {
		@SuppressWarnings("unchecked")
		final Class<FlatJoinFunction<IT1, IT2, OT>> clazz = (Class<FlatJoinFunction<IT1, IT2, OT>>) (Class<?>) FlatJoinFunction.class;
		return clazz;
	}
	
	@Override
	public int getNumberOfDriverComparators() {
		return 2;
	}

	@Override
	public void prepare() throws Exception{
		final TaskConfig config = this.taskContext.getTaskConfig();
		
		// obtain task manager's memory manager and I/O manager
		final MemoryManager memoryManager = this.taskContext.getMemoryManager();
		final IOManager ioManager = this.taskContext.getIOManager();
		
		// set up memory and I/O parameters
		final double fractionAvailableMemory = config.getRelativeMemoryDriver();
		final int numPages = memoryManager.computeNumberOfPages(fractionAvailableMemory);
		
		// test minimum memory requirements
		final DriverStrategy ls = config.getDriverStrategy();
		
		final MutableObjectIterator<IT1> in1 = this.taskContext.getInput(0);
		final MutableObjectIterator<IT2> in2 = this.taskContext.getInput(1);

		// get the key positions and types
		final TypeSerializer<IT1> serializer1 = this.taskContext.<IT1>getInputSerializer(0).getSerializer();
		final TypeSerializer<IT2> serializer2 = this.taskContext.<IT2>getInputSerializer(1).getSerializer();
		final TypeComparator<IT1> comparator1 = this.taskContext.getDriverComparator(0);
		final TypeComparator<IT2> comparator2 = this.taskContext.getDriverComparator(1);
		
		final TypePairComparatorFactory<IT1, IT2> pairComparatorFactory = config.getPairComparatorFactory(
				this.taskContext.getUserCodeClassLoader());
		if (pairComparatorFactory == null) {
			throw new Exception("Missing pair comparator factory for join driver");
		}

		ExecutionConfig executionConfig = taskContext.getExecutionConfig();
		boolean objectReuseEnabled = executionConfig.isObjectReuseEnabled();

		if (LOG.isDebugEnabled()) {
			LOG.debug("Join Driver object reuse: " + (objectReuseEnabled ? "ENABLED" : "DISABLED") + ".");
		}

		// create and return joining iterator according to provided local strategy.
		if (objectReuseEnabled) {
			switch (ls) {
				case MERGE:
					this.joinIterator = new ReusingMergeInnerJoinIterator<>(in1, in2, serializer1, comparator1, serializer2, comparator2, pairComparatorFactory.createComparator12(comparator1, comparator2), memoryManager, ioManager, numPages, this.taskContext.getOwningNepheleTask());

					break;
				case HYBRIDHASH_BUILD_FIRST:
					this.joinIterator = new ReusingBuildFirstHashMatchIterator<>(in1, in2, serializer1, comparator1, serializer2, comparator2, pairComparatorFactory.createComparator21(comparator1, comparator2), memoryManager, ioManager, this.taskContext.getOwningNepheleTask(), fractionAvailableMemory);
					break;
				case HYBRIDHASH_BUILD_SECOND:
					this.joinIterator = new ReusingBuildSecondHashMatchIterator<>(in1, in2, serializer1, comparator1, serializer2, comparator2, pairComparatorFactory.createComparator12(comparator1, comparator2), memoryManager, ioManager, this.taskContext.getOwningNepheleTask(), fractionAvailableMemory);
					break;
				default:
					throw new Exception("Unsupported driver strategy for join driver: " + ls.name());
			}
		} else {
			switch (ls) {
				case MERGE:
					this.joinIterator = new NonReusingMergeInnerJoinIterator<>(in1, in2, serializer1, comparator1, serializer2, comparator2, pairComparatorFactory.createComparator12(comparator1, comparator2), memoryManager, ioManager, numPages, this.taskContext.getOwningNepheleTask());

					break;
				case HYBRIDHASH_BUILD_FIRST:
					this.joinIterator = new NonReusingBuildFirstHashMatchIterator<>(in1, in2, serializer1, comparator1, serializer2, comparator2, pairComparatorFactory.createComparator21(comparator1, comparator2), memoryManager, ioManager, this.taskContext.getOwningNepheleTask(), fractionAvailableMemory);
					break;
				case HYBRIDHASH_BUILD_SECOND:
					this.joinIterator = new NonReusingBuildSecondHashMatchIterator<>(in1, in2, serializer1, comparator1, serializer2, comparator2, pairComparatorFactory.createComparator12(comparator1, comparator2), memoryManager, ioManager, this.taskContext.getOwningNepheleTask(), fractionAvailableMemory);
					break;
				default:
					throw new Exception("Unsupported driver strategy for join driver: " + ls.name());
			}
		}
		
		// open the iterator - this triggers the sorting or hash-table building
		// and blocks until the iterator is ready
		this.joinIterator.open();
		
		if (LOG.isDebugEnabled()) {
			LOG.debug(this.taskContext.formatLogString("join task iterator ready."));
		}
	}

	@Override
	public void run() throws Exception {
		final FlatJoinFunction<IT1, IT2, OT> joinStub = this.taskContext.getStub();
		final Collector<OT> collector = this.taskContext.getOutputCollector();
		final JoinTaskIterator<IT1, IT2, OT> joinIterator = this.joinIterator;
		
		while (this.running && joinIterator.callWithNextKey(joinStub, collector));
	}

	@Override
	public void cleanup() throws Exception {
		if (this.joinIterator != null) {
			this.joinIterator.close();
			this.joinIterator = null;
		}
	}
	
	@Override
	public void cancel() {
		this.running = false;
		if (this.joinIterator != null) {
			this.joinIterator.abort();
		}
	}
}
