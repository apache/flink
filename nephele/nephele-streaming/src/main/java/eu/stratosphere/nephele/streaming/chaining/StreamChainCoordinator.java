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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.execution.Mapper;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.streaming.wrappers.StreamingInputGate;
import eu.stratosphere.nephele.streaming.wrappers.StreamingOutputGate;
import eu.stratosphere.nephele.types.Record;

public final class StreamChainCoordinator {

	/**
	 * The log object.
	 */
	private static final Log LOG = LogFactory.getLog(StreamChainCoordinator.class);

	private final ConcurrentMap<ExecutionVertexID, StreamChainLink> chainLinks = new ConcurrentHashMap<ExecutionVertexID, StreamChainLink>();

	public void registerMapper(final ExecutionVertexID vertexID,
			final Mapper<? extends Record, ? extends Record> mapper,
			final StreamingInputGate<? extends Record> inputGate, final StreamingOutputGate<? extends Record> outputGate) {

		final StreamChainLink chainLink = new StreamChainLink(mapper, inputGate, outputGate);

		if (this.chainLinks.putIfAbsent(vertexID, chainLink) == null) {
			LOG.info("Registering stream chain link for vertex ID " + vertexID);
		}
	}

	public StreamChain constructStreamChain(final List<ExecutionVertexID> vertexIDs) {

		final Iterator<ExecutionVertexID> it = vertexIDs.iterator();
		final List<StreamChainLink> chainLinkList = new ArrayList<StreamChainLink>();
		while (it.hasNext()) {

			final ExecutionVertexID vertexID = it.next();
			final StreamChainLink chainLink = this.chainLinks.get(vertexID);
			if (chainLink == null) {
				LOG.error("Cannot construct stream chain from " + vertexIDs.get(0) + " to "
					+ vertexIDs.get(vertexIDs.size() - 1) + ": No chain link for vertex ID " + vertexID);
				return null;
			}

			chainLinkList.add(chainLink);
		}

		return new StreamChain(Collections.unmodifiableList(chainLinkList));
	}
}
