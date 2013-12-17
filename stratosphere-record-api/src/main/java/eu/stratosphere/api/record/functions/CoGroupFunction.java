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

import java.util.Iterator;

import eu.stratosphere.api.functions.AbstractFunction;
import eu.stratosphere.api.functions.GenericCoGrouper;
import eu.stratosphere.types.PactRecord;
import eu.stratosphere.util.Collector;

/**
 * The CoGroupFunction must be extended to provide a co-grouper implementation which is called by a CoGroup PACT.
 * By definition, a CoGroup PACT has two input sets of records. It calls the co-grouper implementation once for each
 * distinct key in its inputs. Together with the key, two iterators over the records containing that key from both
 * inputs are handed to the <code>coGroup()</code> method.
 * For details on the CoGroup PACT read the documentation of the PACT programming model.
 * <p>
 * The CoGroupFunction extension must be parameterized with the type of the key of its input.
 * <p>
 * For a coGroup implementation, the <code>coGroup()</code> method must be implemented.
 */
public abstract class CoGroupFunction extends AbstractFunction implements GenericCoGrouper<PactRecord, PactRecord, PactRecord> {
	
	/**
	 * This method must be implemented to provide a user implementation of a
	 * matcher. It is called for each two key-value pairs that share the same
	 * key and come from different inputs.
	 * 
	 * @param records1 The records from the first input which were paired with the key.
	 * @param records2 The records from the second input which were paired with the key.
	 * @param out A collector that collects all output pairs.
	 * 
	 * @throws Exception Implementations may forward exceptions, which are caught by the runtime. When the
	 *                   runtime catches an exception, it aborts the task and lets the fail-over logic
	 *                   decide whether to retry the task execution.
	 */
	@Override
	public abstract void coGroup(Iterator<PactRecord> records1, Iterator<PactRecord> records2, Collector<PactRecord> out) throws Exception;
	
	/**
	 * This method must be overridden by CoGoup UDFs that want to make use of the combining feature
	 * on their first input. In addition, the extending class must be annotated as CombinableFirst.
	 * <p>
	 * The use of the combiner is typically a pre-reduction of the data.
	 * 
	 * @param records The records to be combined.
	 * @param out The collector to write the result to.
	 * 
	 * @throws Exception Implementations may forward exceptions, which are caught by the runtime. When the
	 *                   runtime catches an exception, it aborts the combine task and lets the fail-over logic
	 *                   decide whether to retry the combiner execution.
	 */
	@Override
	public void combineFirst(Iterator<PactRecord> records, Collector<PactRecord> out) throws Exception {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * This method must be overridden by CoGoup UDFs that want to make use of the combining feature
	 * on their second input. In addition, the extending class must be annotated as CombinableSecond.
	 * <p>
	 * The use of the combiner is typically a pre-reduction of the data.
	 * 
	 * @param records The records to be combined.
	 * @param out The collector to write the result to.
	 * 
	 * @throws Exception Implementations may forward exceptions, which are caught by the runtime. When the
	 *                   runtime catches an exception, it aborts the combine task and lets the fail-over logic
	 *                   decide whether to retry the combiner execution.
	 */
	@Override
	public void combineSecond(Iterator<PactRecord> records, Collector<PactRecord> out) throws Exception {
		throw new UnsupportedOperationException();
	}
}
