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

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.types.Record;

/**
 * 
 * <b>NOTE: The Record API is marked as deprecated. It is not being developed anymore and will be removed from
 * the code at some point.
 * See <a href="https://issues.apache.org/jira/browse/FLINK-1106">FLINK-1106</a> for more details.</b>
 * 
 * The CrossFunction is the base class for functions that are invoked by a {@link org.apache.flink.api.java.record.operators.CrossOperator}.
 */
@Deprecated
public abstract class CrossFunction extends AbstractRichFunction implements org.apache.flink.api.common.functions.CrossFunction<Record, Record, Record> {
	
	private static final long serialVersionUID = 1L;
	
	/**
	 * This method must be implemented to provide a user implementation of a cross.
	 * It is called for each element of the Cartesian product of both input sets.

	 * @param first The record from the second input.
	 * @param second The record from the second input.
	 * @return The result of the cross UDF
	 * 
	 * @throws Exception Implementations may forward exceptions, which are caught by the runtime. When the
	 *                   runtime catches an exception, it aborts the task and lets the fail-over logic
	 *                   decide whether to retry the task execution.
	 */
	@Override
	public abstract Record cross(Record first, Record second) throws Exception;

}
