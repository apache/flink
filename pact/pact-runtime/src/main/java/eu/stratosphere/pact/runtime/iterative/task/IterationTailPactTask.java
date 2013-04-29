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

package eu.stratosphere.pact.runtime.iterative.task;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.generic.types.TypeSerializer;
import eu.stratosphere.pact.generic.types.TypeSerializerFactory;
import eu.stratosphere.pact.runtime.iterative.concurrent.BlockingBackChannel;
import eu.stratosphere.pact.runtime.iterative.concurrent.BlockingBackChannelBroker;
import eu.stratosphere.pact.runtime.iterative.concurrent.Broker;
import eu.stratosphere.pact.runtime.iterative.io.DataOutputCollector;
import eu.stratosphere.pact.runtime.iterative.monitoring.IterationMonitoring;
import eu.stratosphere.pact.runtime.task.PactTaskContext;

//TODO could this be an output???
/**
 * The tail of an iteration, which is able to run a {@link eu.stratosphere.pact.runtime.task.PactDriver} inside. It will
 * send back its output to
 * the iteration's head via a {@link BlockingBackChannel}. Therefore this task must be scheduled on the same instance as
 * the head.
 */
public class IterationTailPactTask<S extends Stub, OT> extends AbstractIterativePactTask<S, OT>
		implements PactTaskContext<S, OT>
{
	private static final Log log = LogFactory.getLog(IterationTailPactTask.class);

	
	private BlockingBackChannel retrieveBackChannel() throws Exception {
		// blocking call to retrieve the backchannel from the iteration head
		Broker<BlockingBackChannel> broker = BlockingBackChannelBroker.instance();
		return broker.get(brokerKey());
	}

	@Override
	public void run() throws Exception {

		// Initially retrieve the backchannel from the iteration head
		final BlockingBackChannel backChannel = retrieveBackChannel();
		
		// instantiate the collector that writes to the back channel
		final TypeSerializerFactory<OT> outSerializerFact = this.config.getOutputSerializer(this.userCodeClassLoader);
		if (outSerializerFact == null) {
			throw new Exception("Error: Missing serializer for tail result!");
		}
		
		final TypeSerializer<OT> serializer = outSerializerFact.getSerializer();
		final DataOutputCollector<OT> outputCollector = new DataOutputCollector<OT>(
				backChannel.getWriteEnd(), serializer);
		this.output = outputCollector;

		while (this.running && !terminationRequested()) {

			notifyMonitor(IterationMonitoring.Event.TAIL_STARTING);
			if (log.isInfoEnabled()) {
				log.info(formatLogString("starting iteration [" + currentIteration() + "]"));
			}

			notifyMonitor(IterationMonitoring.Event.TAIL_PACT_STARTING);

			super.run();
			notifyMonitor(IterationMonitoring.Event.TAIL_PACT_FINISHED);

			long elementsCollected = outputCollector.getElementsCollectedAndReset();
			if (log.isInfoEnabled()) {
				log.info("IterationTail [" + getEnvironment().getIndexInSubtaskGroup() + "] inserted [" +
					elementsCollected + "] elements into backchannel in iteration [" + currentIteration() + "]");
			}

			if (log.isInfoEnabled()) {
				log.info(formatLogString("finishing iteration [" + currentIteration() + "]"));
			}

			if (!terminationRequested()) {
				backChannel.notifyOfEndOfSuperstep();
				incrementIterationCounter();
			}
			notifyMonitor(IterationMonitoring.Event.TAIL_FINISHED);
		}
	}
}
