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


package org.apache.flink.runtime.operators;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

/**
 * A driver that does nothing but forward data from its input to its output.
 * 
 * @param <T> The data type.
 */
public class NoOpDriver<T> implements PactDriver<AbstractRichFunction, T> {
	
	private PactTaskContext<AbstractRichFunction, T> taskContext;
	
	private volatile boolean running;
	
	
	@Override
	public void setup(PactTaskContext<AbstractRichFunction, T> context) {
		this.taskContext = context;
		this.running = true;
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}
	
	@Override
	public Class<AbstractRichFunction> getStubType() {
		return null;
	}

	@Override
	public boolean requiresComparatorOnInput() {
		return false;
	}

	@Override
	public void prepare() {}

	@Override
	public void run() throws Exception {
		// cache references on the stack
		final MutableObjectIterator<T> input = this.taskContext.getInput(0);
		final Collector<T> output = this.taskContext.getOutputCollector();
		T record = this.taskContext.<T>getInputSerializer(0).getSerializer().createInstance();

		while (this.running && ((record = input.next(record)) != null)) {
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
