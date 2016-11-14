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


package org.apache.flink.runtime.operators.chaining;

import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.operators.base.BulkIterationBase;
import org.apache.flink.api.common.operators.base.BulkIterationBase.TerminationCriterionAggregator;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

public class ChainedTerminationCriterionDriver<IT, OT> extends ChainedDriver<IT, OT> {
	
	private TerminationCriterionAggregator agg;

	// --------------------------------------------------------------------------------------------

	@Override
	public void setup(AbstractInvokable parent) {
		agg = ((IterationRuntimeContext) getUdfRuntimeContext()).getIterationAggregator(BulkIterationBase.TERMINATION_CRITERION_AGGREGATOR_NAME);
	}

	@Override
	public void openTask() {}

	@Override
	public void closeTask() {}

	@Override
	public void cancelTask() {}

	// --------------------------------------------------------------------------------------------

	public RichFunction getStub() {
		return null;
	}

	public String getTaskName() {
		return "";
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public void collect(IT record) {
		numRecordsIn.inc();
		agg.aggregate(1);
	}

	@Override
	public void close() {}
}
