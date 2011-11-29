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

package eu.stratosphere.nephele.streaming.wrappers;

import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.execution.Mapper;
import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.RecordDeserializer;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.plugins.wrapper.AbstractEnvironmentWrapper;
import eu.stratosphere.nephele.streaming.listeners.StreamListener;
import eu.stratosphere.nephele.types.Record;

/**
 * A streaming environment wraps the created input and output gates in special {@link StreamingInputGate} and
 * {@link StreamingOutputGate} objects to intercept particular methods calls necessary for the statistics collection.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class StreamingEnvironment extends AbstractEnvironmentWrapper {

	private final StreamListener streamListener;

	/**
	 * Constructs a new streaming environment
	 * 
	 * @param wrappedEnvironment
	 *        the environment to be encapsulated by this streaming environment
	 * @param streamListener
	 *        the stream listener
	 */
	StreamingEnvironment(final Environment wrappedEnvironment, final StreamListener streamListener) {
		super(wrappedEnvironment);

		this.streamListener = streamListener;
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public OutputGate<? extends Record> createOutputGate(final GateID gateID,
			final Class<? extends Record> outputClass, final ChannelSelector<? extends Record> selector,
			final boolean isBroadcast) {

		final OutputGate<? extends Record> outputGate = getWrappedEnvironment().createOutputGate(gateID, outputClass,
			selector, isBroadcast);

		return new StreamingOutputGate(outputGate, this.streamListener);
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public InputGate<? extends Record> createInputGate(final GateID gateID,
			final RecordDeserializer<? extends Record> deserializer, final DistributionPattern distributionPattern) {

		final InputGate<? extends Record> inputGate = getWrappedEnvironment().createInputGate(gateID, deserializer,
			distributionPattern);

		return new StreamingInputGate(inputGate, this.streamListener);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerMapper(final Mapper<? extends Record, ? extends Record> mapper,
			final RecordReader<? extends Record> reader, final RecordWriter<? extends Record> writer) {

		this.streamListener.registerMapper(mapper, reader, writer);
	}
}
