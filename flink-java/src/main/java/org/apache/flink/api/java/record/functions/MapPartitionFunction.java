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

package org.apache.flink.api.java.record.functions;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;

/**
 * The MapPartitionFunction must be extended to provide a map partition implementation
 * By definition, the map partition is called for a full input set.
 */
public abstract class MapPartitionFunction extends AbstractRichFunction implements org.apache.flink.api.common.functions.MapPartitionFunction<Record, Record> {
	
	private static final long serialVersionUID = 1L;
	
	/**
	 * This method must be implemented to provide a user implementation of a MapPartitionFunction.
	 * It is called for a full input set.
	 *
	 * @param records all input records
	 * @param out A collector that collects all output records.
	 *
	 * @throws Exception Implementations may forward exceptions, which are caught by the runtime. When the
	 *                   runtime catches an exception, it aborts the map task and lets the fail-over logic
	 *                   decide whether to retry the mapper execution.
	 */
	@Override
	public abstract void mapPartition(Iterable<Record> records, Collector<Record> out) throws Exception;
}