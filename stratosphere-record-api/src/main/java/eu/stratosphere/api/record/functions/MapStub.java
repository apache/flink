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

package eu.stratosphere.api.record.functions;

import eu.stratosphere.api.functions.AbstractStub;
import eu.stratosphere.api.functions.GenericMapper;
import eu.stratosphere.types.PactRecord;
import eu.stratosphere.util.Collector;

/**
 * The MapStub must be extended to provide a mapper implementation which is called by a Map PACT.
 * By definition, the Map PACT calls the mapper implementation for each individual input record.
 * For details on the Map PACT read the documentation of the PACT programming model.
 * <p>
 * For a mapper implementation, the <code>map()</code> method must be implemented.
 */
public abstract class MapStub extends AbstractStub implements GenericMapper<PactRecord, PactRecord> {
	
	/**
	 * This method must be implemented to provide a user implementation of a mapper.
	 * It is called for each individual record.
	 * 
	 * @param record The record to be mapped.
	 * @param out A collector that collects all output records.
	 * 
	 * @throws Exception Implementations may forward exceptions, which are caught by the runtime. When the
	 *                   runtime catches an exception, it aborts the map task and lets the fail-over logic
	 *                   decide whether to retry the mapper execution.
	 */
	@Override
	public abstract void map(PactRecord record, Collector<PactRecord> out) throws Exception;
}
