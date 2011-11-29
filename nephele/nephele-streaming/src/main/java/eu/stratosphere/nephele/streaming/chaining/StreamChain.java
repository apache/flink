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

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

import eu.stratosphere.nephele.streaming.wrappers.StreamingOutputGate;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.nephele.util.StringUtils;

public final class StreamChain {

	@SuppressWarnings("rawtypes")
	private final List<StreamChainLink> chainLinks;

	@SuppressWarnings("rawtypes")
	StreamChain(final List<StreamChainLink> chainLinks) {

		if (chainLinks.isEmpty()) {
			throw new IllegalArgumentException("List chainLinks must not be empty");
		}

		this.chainLinks = chainLinks;
	}

	@SuppressWarnings("unchecked")
	public StreamingOutputGate<? extends Record> getFirstOutputGate() {

		return this.chainLinks.get(0).getOutputGate();
	}

	public void writeRecord(final Record record) throws IOException, InterruptedException {

		try {
			executeMapper(record, 1);
		} catch (Exception e) {
			throw new IOException(StringUtils.stringifyException(e));
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	void executeMapper(final Record record, final int chainIndex) throws Exception {

		final StreamChainLink chainLink = this.chainLinks.get(chainIndex);
		final Queue output = new ArrayDeque();

		chainLink.getMapper().map(record, output);

		if (chainIndex == this.chainLinks.size() - 1) {

			final StreamingOutputGate outputGate = chainLink.getOutputGate();
			
			while (!output.isEmpty()) {
				outputGate.writeRecord((Record)output.poll());
			}
			
		} else {

			while (!output.isEmpty()) {
				executeMapper((Record)output.poll(), chainIndex+1);
			}
		}
	}
}
