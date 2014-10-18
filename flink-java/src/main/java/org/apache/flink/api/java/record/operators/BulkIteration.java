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


package org.apache.flink.api.java.record.operators;

import org.apache.flink.api.common.operators.base.BulkIterationBase;
import org.apache.flink.types.Record;

/**
 *  * <b>NOTE: The Record API is marked as deprecated. It is not being developed anymore and will be removed from
 * the code at some point.
 * See <a href="https://issues.apache.org/jira/browse/FLINK-1106">FLINK-1106</a> for more details.</b>
 */
@Deprecated
public class BulkIteration extends BulkIterationBase<Record> {
	public BulkIteration() {
		super(OperatorInfoHelper.unary());
	}

	public BulkIteration(String name) {
		super(OperatorInfoHelper.unary(), name);
	}

	/**
	 * Specialized operator to use as a recognizable place-holder for the input to the
	 * step function when composing the nested data flow.
	 */
	public static class PartialSolutionPlaceHolder extends BulkIterationBase.PartialSolutionPlaceHolder<Record> {
		public PartialSolutionPlaceHolder(BulkIterationBase<Record> container) {
			super(container, OperatorInfoHelper.unary());
		}
	}
}
