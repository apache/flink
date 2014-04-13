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

package eu.stratosphere.api.java.record.functions;

import java.util.Iterator;

import eu.stratosphere.api.common.functions.AbstractFunction;
import eu.stratosphere.api.common.functions.GenericCoGrouper;
import eu.stratosphere.api.java.record.operators.CoGroupOperator;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

/**
 * The CoGroupFunction is the base class for functions that are invoked by a {@link CoGroupOperator}.
 */
public abstract class CoGroupFunction extends AbstractFunction implements GenericCoGrouper<Record, Record, Record> {

	private static final long serialVersionUID = 1L;

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
	public abstract void coGroup(Iterator<Record> records1, Iterator<Record> records2, Collector<Record> out) throws Exception;
	
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
	public Record combineFirst(Iterator<Record> records) throws Exception {
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
	public Record combineSecond(Iterator<Record> records) throws Exception {
		throw new UnsupportedOperationException();
	}
}
