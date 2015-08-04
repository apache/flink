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
 * Match task which is executed by a Task Manager. The task has two inputs and one or multiple outputs.
 * It is provided with a JoinFunction implementation.
 * <p>
 * The MatchTask matches all pairs of records that share the same key and come from different inputs. Each pair of 
 * matching records is handed to the <code>match()</code> method of the JoinFunction.
 * 
 * @see org.apache.flink.api.common.functions.FlatJoinFunction
 */
public class MatchDriver<IT1, IT2, OT> implements PactDriver<FlatJoinFunction<IT1, IT2, OT>, OT> {
	
	protected static final Logger LOG = LoggerFactory.getLogger(MatchDriver.class);
	
	protected PactTaskContext<FlatJoinFunction<IT1, IT2, OT>, OT> taskContext;
	
	private volatile JoinTaskIterator<IT1, IT2, OT> matchIterator;		// the iterator that does the actual matching
	
	protected volatile boolean running;

	private boolean objectReuseEnabled = false;

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
			throw new Exception("Missing pair comparator factory for Match driver");
		}

		ExecutionConfig executionConfig = taskContext.getExecutionConfig();
		this.objectReuseEnabled = executionConfig.isObjectReuseEnabled();

		if (LOG.isDebugEnabled()) {
			LOG.debug("MatchDriver object reuse: " + (this.objectReuseEnabled ? "ENABLED" : "DISABLED") + ".");
		}

		// create and return MatchTaskIterator according to provided local strategy.
		if (this.objectReuseEnabled) {
			switch (ls) {
				case MERGE:
					this.matchIterator = new ReusingMergeInnerJoinIterator<IT1, IT2, OT>(in1, in2, serializer1, comparator1, serializer2, comparator2, pairComparatorFactory.createComparator12(comparator1, comparator2), memoryManager, ioManager, numPages, this.taskContext.getOwningNepheleTask());

					break;
				case HYBRIDHASH_BUILD_FIRST:
					this.matchIterator = new ReusingBuildFirstHashMatchIterator<IT1, IT2, OT>(in1, in2, serializer1, comparator1, serializer2, comparator2, pairComparatorFactory.createComparator21(comparator1, comparator2), memoryManager, ioManager, this.taskContext.getOwningNepheleTask(), fractionAvailableMemory);
					break;
				case HYBRIDHASH_BUILD_SECOND:
					this.matchIterator = new ReusingBuildSecondHashMatchIterator<IT1, IT2, OT>(in1, in2, serializer1, comparator1, serializer2, comparator2, pairComparatorFactory.createComparator12(comparator1, comparator2), memoryManager, ioManager, this.taskContext.getOwningNepheleTask(), fractionAvailableMemory);
					break;
				default:
					throw new Exception("Unsupported driver strategy for Match driver: " + ls.name());
			}
		} else {
			switch (ls) {
				case MERGE:
					this.matchIterator = new NonReusingMergeInnerJoinIterator<IT1, IT2, OT>(in1, in2, serializer1, comparator1, serializer2, comparator2, pairComparatorFactory.createComparator12(comparator1, comparator2), memoryManager, ioManager, numPages, this.taskContext.getOwningNepheleTask());

					break;
				case HYBRIDHASH_BUILD_FIRST:
					this.matchIterator = new NonReusingBuildFirstHashMatchIterator<IT1, IT2, OT>(in1, in2, serializer1, comparator1, serializer2, comparator2, pairComparatorFactory.createComparator21(comparator1, comparator2), memoryManager, ioManager, this.taskContext.getOwningNepheleTask(), fractionAvailableMemory);
					break;
				case HYBRIDHASH_BUILD_SECOND:
					this.matchIterator = new NonReusingBuildSecondHashMatchIterator<IT1, IT2, OT>(in1, in2, serializer1, comparator1, serializer2, comparator2, pairComparatorFactory.createComparator12(comparator1, comparator2), memoryManager, ioManager, this.taskContext.getOwningNepheleTask(), fractionAvailableMemory);
					break;
				default:
					throw new Exception("Unsupported driver strategy for Match driver: " + ls.name());
			}
		}
		
		// open MatchTaskIterator - this triggers the sorting or hash-table building
		// and blocks until the iterator is ready
		this.matchIterator.open();
		
		if (LOG.isDebugEnabled()) {
			LOG.debug(this.taskContext.formatLogString("Match task iterator ready."));
		}
	}

	@Override
	public void run() throws Exception {
		final FlatJoinFunction<IT1, IT2, OT> matchStub = this.taskContext.getStub();
		final Collector<OT> collector = this.taskContext.getOutputCollector();
		final JoinTaskIterator<IT1, IT2, OT> matchIterator = this.matchIterator;
		
		while (this.running && matchIterator.callWithNextKey(matchStub, collector));
	}

	@Override
	public void cleanup() throws Exception {
		if (this.matchIterator != null) {
			this.matchIterator.close();
			this.matchIterator = null;
		}
	}
	
	@Override
	public void cancel() {
		this.running = false;
		if (this.matchIterator != null) {
			this.matchIterator.abort();
		}
	}
}
