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

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.generic.stub.AbstractStub;

/**
 * A driver that does nothing but forward data from its input to its output.
 * 
 * @param <T> The data type.
 */
public class NoOpDriver<T> implements PactDriver<AbstractStub, T>// extends AbstractPactTask<GenericMapper<IT, OT>, OT>
{
	private PactTaskContext<AbstractStub, T> taskContext;
	
	private volatile boolean running;
	
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.PactDriver#setup(eu.stratosphere.pact.runtime.task.PactTaskContext)
	 */
	@Override
	public void setup(PactTaskContext<AbstractStub, T> context) {
		this.taskContext = context;
		this.running = true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#getNumberOfInputs()
	 */
	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#getStubType()
	 */
	@Override
	public Class<AbstractStub> getStubType() {
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#requiresComparatorOnInput()
	 */
	@Override
	public boolean requiresComparatorOnInput() {
		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#prepare()
	 */
	@Override
	public void prepare() throws Exception {
		// nothing, since a mapper does not need any preparation
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#run()
	 */
	@Override
	public void run() throws Exception {
		// cache references on the stack
		final MutableObjectIterator<T> input = this.taskContext.getInput(0);
		final Collector<T> output = this.taskContext.getOutputCollector();
		final T record = this.taskContext.<T>getInputSerializer(0).createInstance();

		while (this.running && input.next(record)) {
			output.collect(record);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#cleanup()
	 */
	@Override
	public void cleanup() throws Exception {
		// no cleanup, since no strategy is used.
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.PactDriver#cancel()
	 */
	@Override
	public void cancel() {
		this.running = false;
	}
}