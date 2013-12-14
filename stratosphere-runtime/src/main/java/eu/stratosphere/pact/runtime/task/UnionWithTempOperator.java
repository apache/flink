/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.runtime.task;

import eu.stratosphere.api.functions.Stub;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.MutableObjectIterator;


public class UnionWithTempOperator<T> implements PactDriver<Stub, T> {
	
	private PactTaskContext<Stub, T> taskContext;
	
	private volatile boolean running;
	
	
	@Override
	public void setup(PactTaskContext<Stub, T> context) {
		this.taskContext = context;
		this.running = true;
	}

	@Override
	public int getNumberOfInputs() {
		return 2;
	}

	@Override
	public Class<Stub> getStubType() {
		return Stub.class;
	}

	@Override
	public boolean requiresComparatorOnInput() {
		return false;
	}

	@Override
	public void prepare() {}

	@Override
	public void run() throws Exception {
		
		final int tempedInput = 0;
		final int streamedInput = 1;
		
		final MutableObjectIterator<T> cache = this.taskContext.getInput(tempedInput);
		final MutableObjectIterator<T> input = this.taskContext.getInput(streamedInput);
		
		final Collector<T> output = this.taskContext.getOutputCollector();

		final T record = this.taskContext.<T>getInputSerializer(streamedInput).createInstance();

		while (this.running && input.next(record)) {
			output.collect(record);
		}
		while (this.running && cache.next(record)) {
			output.collect(record);
		}
	}

	@Override
	public void cleanup() {}

	@Override
	public void cancel() {
		this.running = false;
	}
}