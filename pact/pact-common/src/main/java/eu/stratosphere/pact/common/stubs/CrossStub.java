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

package eu.stratosphere.pact.common.stubs;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.generic.stub.AbstractStub;
import eu.stratosphere.pact.generic.stub.GenericCrosser;

/**
 * The CrossStub must be extended to provide a cross implementation which is called by a Cross PACT.
 * By definition, a Cross PACT has two input sets of records. It calls the cross implementation for each
 * element of the Cartesian product of both input sets. For details on the Cross PACT read the
 * documentation of the PACT programming model.
 * <p>
 * For a cross implementation, the <code>cross()</code> method must be implemented.
 * 
 * @author Fabian Hueske
 */
public abstract class CrossStub extends AbstractStub implements GenericCrosser<PactRecord, PactRecord, PactRecord>
{
	/**
	 * This method must be implemented to provide a user implementation of a cross.
	 * It is called for each element of the Cartesian product of both input sets.

	 * @param record1 The record from the second input.
	 * @param record2 The record from the second input.
	 * @param out A collector that collects all output records.
	 * 
	 * @throws Exception Implementations may forward exceptions, which are caught by the runtime. When the
	 *                   runtime catches an exception, it aborts the task and lets the fail-over logic
	 *                   decide whether to retry the task execution.
	 */
	@Override
	public abstract void cross(PactRecord record1, PactRecord record2, Collector<PactRecord> out);
}
