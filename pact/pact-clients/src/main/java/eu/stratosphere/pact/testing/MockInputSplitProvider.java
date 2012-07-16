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
package eu.stratosphere.pact.testing;

import java.util.concurrent.atomic.AtomicInteger;

import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.jobmanager.splitassigner.InputSplitManager;
import eu.stratosphere.nephele.template.InputSplit;
import eu.stratosphere.nephele.template.InputSplitProvider;

final class MockInputSplitProvider implements InputSplitProvider {
	private ExecutionVertex vertex;

	final AtomicInteger sequenceNumber = new AtomicInteger(0);

	private InputSplitManager inputSplitManager;

	public MockInputSplitProvider(InputSplitManager inputSplitManager, ExecutionVertex vertex) {
		this.vertex = vertex;
		this.inputSplitManager = inputSplitManager;
	}

	@Override
	public InputSplit getNextInputSplit() {
		return this.inputSplitManager.getNextInputSplit(this.vertex, this.sequenceNumber.getAndIncrement());
	}
}