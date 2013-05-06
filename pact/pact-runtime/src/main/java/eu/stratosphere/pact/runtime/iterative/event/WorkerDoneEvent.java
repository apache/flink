/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.runtime.iterative.event;

import eu.stratosphere.pact.common.stubs.aggregators.Aggregator;
import eu.stratosphere.pact.common.type.Value;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class WorkerDoneEvent extends IterationEventWithAggregators {
	
	private int workerIndex;
	
	public WorkerDoneEvent() {
		super();
	}

	public WorkerDoneEvent(int workerIndex, String aggregatorName, Value aggregate) {
		super(aggregatorName, aggregate);
		this.workerIndex = workerIndex;
	}
	
	public WorkerDoneEvent(int workerIndex, Map<String, Aggregator<?>> aggregators) {
		super(aggregators);
		this.workerIndex = workerIndex;
	}
	
	public int getWorkerIndex() {
		return workerIndex;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.workerIndex);
		super.write(out);
	}
	
	@Override
	public void read(DataInput in) throws IOException {
		this.workerIndex = in.readInt();
		super.read(in);
	}
}
