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

package org.apache.flink.api.java.record.functions;

import java.util.Iterator;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;

/**
 * 
 * <b>NOTE: The Record API is marked as deprecated. It is not being developed anymore and will be removed from
 * the code at some point.
 * See <a href="https://issues.apache.org/jira/browse/FLINK-1106">FLINK-1106</a> for more details.</b>
 * 
 * 
 * The ReduceFunction must be extended to provide a reducer implementation, as invoked by a
 * {@link org.apache.flink.api.java.record.operators.ReduceOperator}.
 */
@Deprecated
public abstract class ReduceFunction extends AbstractRichFunction {
	
	private static final long serialVersionUID = 1L;
	
	/**
	 * The central function to be implemented for a reducer. The function receives per call one
	 * key and all the values that belong to that key. Each key is guaranteed to be processed by exactly
	 * one function call across all involved instances across all computing nodes.
	 * 
	 * @param records All records that belong to the given input key.
	 * @param out The collector to hand results to.
	 * 
	 * @throws Exception Implementations may forward exceptions, which are caught by the runtime. When the
	 *                   runtime catches an exception, it aborts the reduce task and lets the fail-over logic
	 *                   decide whether to retry the reduce execution.
	 */
	public abstract void reduce(Iterator<Record> records, Collector<Record> out) throws Exception;

	/**
	 * No default implementation provided.
	 * This method must be overridden by reduce stubs that want to make use of the combining feature.
	 * In addition, the ReduceFunction extending class must be annotated as Combinable.
	 * Note that this function must be implemented, if the reducer is annotated as combinable.
	 * <p>
	 * The use of the combiner is typically a pre-reduction of the data. It works similar as the reducer, only that is
	 * is not guaranteed to see all values with the same key in one call to the combine function. Since it is called
	 * prior to the <code>reduce()</code> method, input and output types of the combine method are the input types of
	 * the <code>reduce()</code> method.
	 * 
	 * @see org.apache.flink.api.java.record.operators.ReduceOperator.Combinable
	 * @param records
	 *        The records to be combined. Unlike in the reduce method, these are not necessarily all records
	 *        belonging to the given key.
	 * @param out The collector to write the result to.
	 * 
	 * @throws Exception Implementations may forward exceptions, which are caught by the runtime. When the
	 *                   runtime catches an exception, it aborts the combine task and lets the fail-over logic
	 *                   decide whether to retry the combiner execution.
	 */
	public void combine(Iterator<Record> records, Collector<Record> out) throws Exception {
		// to be implemented, if the reducer should use a combiner. Note that the combining method
		// is only used, if the stub class is further annotated with the annotation
		// @ReduceOperator.Combinable
		reduce(records, out);
	}
}
