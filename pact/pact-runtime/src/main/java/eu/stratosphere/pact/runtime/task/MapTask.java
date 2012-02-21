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

import eu.stratosphere.nephele.annotations.ForceCheckpoint;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.task.util.OutputCollector;

/**
 * Map task which is executed by a Nephele task manager. The task has a single
 * input and one or multiple outputs. It is provided with a MapStub
 * implementation.
 * <p>
 * The MapTask creates an iterator over all key-value pairs of its input and hands that 
 * to the <code>map()</code> method of the MapStub.
 * 
 * @see MapStub
 * @author Fabian Hueske
 * @author Stephan Ewen
 */
public class MapTask extends AbstractPactTask<MapStub>
{
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#getNumberOfInputs()
	 */
	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#getStubType()
	 */
	@Override
	public Class<MapStub> getStubType() {
		return MapStub.class;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#prepare()
	 */
	@Override
	public void prepare() throws Exception {
		// nothing, since a mapper does not need any preparation
		
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#run()
	 */
	@Override
	public void run() throws Exception
	{
		// cache references on the stack
		final MutableObjectIterator<PactRecord> input = this.inputs[0];
		final MapStub stub = this.stub;
		final Collector output = this.output;
		
		final PactRecord record = new PactRecord();
		
		// DW: Start to temporary code
		int count = 0;
		long consumedPactRecordsInBytes = 0L;
		final Environment env = getEnvironment();
		final OutputCollector oc = (OutputCollector) output;
		// DW: End of temporary code
		if(this.stub.getClass().isAnnotationPresent(ForceCheckpoint.class)){
			env.isForced(this.stub.getClass().getAnnotation(ForceCheckpoint.class).checkpoint());
		}
		while (this.running && input.next(record)) {
			// DW: Start to temporary code
			consumedPactRecordsInBytes =+ record.getBinaryLength();
			// DW: End of temporary code
			stub.map(record, output);
			
			// DW: Start to temporary code
			if(++count == 10) {
				env.reportPACTDataStatistics(consumedPactRecordsInBytes, 
					oc.getCollectedPactRecordsInBytes());
				consumedPactRecordsInBytes = 0L;
				count = 0;
			}
			// DW: End of temporary code
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#cleanup()
	 */
	@Override
	public void cleanup() throws Exception {
		// mappers need no cleanup, since no strategies are used.
	}
}
