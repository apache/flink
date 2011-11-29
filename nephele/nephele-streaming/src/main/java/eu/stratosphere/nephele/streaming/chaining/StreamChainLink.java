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

package eu.stratosphere.nephele.streaming.chaining;

import eu.stratosphere.nephele.execution.Mapper;
import eu.stratosphere.nephele.streaming.wrappers.StreamingInputGate;
import eu.stratosphere.nephele.streaming.wrappers.StreamingOutputGate;
import eu.stratosphere.nephele.types.Record;

public final class StreamChainLink<I extends Record, O extends Record> {

	private final Mapper<I, O> mapper;

	private final StreamingInputGate<I> inputGate;

	private final StreamingOutputGate<O> outputGate;

	StreamChainLink(final Mapper<I, O> mapper,
			final StreamingInputGate<I> inputGate,
			final StreamingOutputGate<O> outputGate) {

		this.mapper = mapper;
		this.inputGate = inputGate;
		this.outputGate = outputGate;
	}

	Mapper<I, O> getMapper() {

		return this.mapper;
	}

	StreamingInputGate<I> getInputGate() {

		return this.inputGate;
	}

	StreamingOutputGate<O> getOutputGate() {

		return this.outputGate;
	}
}
