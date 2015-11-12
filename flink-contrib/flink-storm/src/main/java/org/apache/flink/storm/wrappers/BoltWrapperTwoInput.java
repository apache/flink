/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.storm.wrappers;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IRichBolt;
import backtype.storm.tuple.Fields;
import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple25;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Collection;

/**
 * A {@link BoltWrapperTwoInput} wraps an {@link IRichBolt} in order to execute the Storm bolt within a Flink Streaming
 * program. In contrast to {@link BoltWrapper}, this wrapper takes two input stream as input.
 */
public class BoltWrapperTwoInput<IN1, IN2, OUT> extends BoltWrapper<IN1, OUT> implements TwoInputStreamOperator<IN1, IN2, OUT> {

	/** The schema (ie, ordered field names) of the second input stream. */
	private final Fields inputSchema2;

	/** The component id of the second input stream of the bolt */
	private final String componentId2;
	/** The stream id of the second input stream of the bolt */
	private final String streamId2;

	/**
	 * Instantiates a new {@link BoltWrapperTwoInput} that wraps the given Storm {@link IRichBolt bolt} such that it can be
	 * used within a Flink streaming program. The given input schema enable attribute-by-name access for input types
	 * {@link Tuple0} to {@link Tuple25}. The output type can be any type if parameter {@code rawOutput} is {@code true}
	 * and the bolt's number of declared output tuples is 1. If {@code rawOutput} is {@code false} the output type will
	 * be one of {@link Tuple0} to {@link Tuple25} depending on the bolt's declared number of attributes.
	 *  @param bolt The Storm {@link IRichBolt bolt} to be used.
	 * @param boltId The name of the bolt.
	 * @param streamId1 The stream id of the second input stream for this bolt
	 * @param componentId2 The component id of the second input stream for this bolt
	 * @param inputSchema1
	 *            The schema (ie, ordered field names) of the input stream.
	 * @throws IllegalArgumentException
	 *             If {@code rawOuput} is {@code true} and the number of declared output attributes is not 1 or if
	 *             {@code rawOuput} is {@code false} and the number of declared output attributes is not with range
	 * */
	public BoltWrapperTwoInput(final IRichBolt bolt, final String boltId,
							final String streamId1, final String streamId2,
							final String componentId1, final String componentId2,
							final Fields inputSchema1, final Fields inputSchema2) throws IllegalArgumentException {
		this(bolt, boltId, streamId1, streamId2, componentId1, componentId2, inputSchema1, inputSchema2, null);
	}

	/**
	 * Instantiates a new {@link BoltWrapperTwoInput} that wraps the given Storm {@link IRichBolt bolt} such that it can be
	 * used within a Flink streaming program. The given input schema enable attribute-by-name access for input types
	 * {@link Tuple0} to {@link Tuple25}. The output type can be any type if parameter {@code rawOutput} is {@code true}
	 * and the bolt's number of declared output tuples is 1. If {@code rawOutput} is {@code false} the output type will
	 * be one of {@link Tuple0} to {@link Tuple25} depending on the bolt's declared number of attributes.
	 * @param bolt The Storm {@link IRichBolt bolt} to be used.
	 * @param boltId The name of the bolt.
	 * @param streamId1 The stream id of the first input stream for this bolt
	 * @param streamId2 The stream id of the first input stream for this bolt
	 * @param componentId1 The component id of the first input stream for this bolt
	 * @param componentId2 The component id of the second input stream for this bolt
	 * @param inputSchema1
	 *             The schema (ie, ordered field names) of the first input stream.
	 * @param inputSchema2
	 *             The schema (ie, ordered field names) of the second input stream.
	 * @param rawOutputs
	 *            Contains stream names if a single attribute output stream, should not be of type {@link Tuple1} but be
	 *            of a raw type.
	 * @throws IllegalArgumentException
	 *          If {@code rawOuput} is {@code true} and the number of declared output attributes is not 1 or if
	 *            {@code rawOuput} is {@code false} and the number of declared output attributes is not with range
	 */
	public BoltWrapperTwoInput(final IRichBolt bolt, final String boltId,
							final String streamId1, final String streamId2,
							final String componentId1, final String componentId2,
							final Fields inputSchema1, final Fields inputSchema2,
							final Collection<String> rawOutputs) throws IllegalArgumentException {
		super(bolt, boltId, streamId1, componentId1, inputSchema1, rawOutputs);
		this.componentId2 = componentId2;
		this.streamId2 = streamId2;
		this.inputSchema2 = inputSchema2;
	}

	/**
	 * Sets the original Storm topology.
	 *
	 * @param stormTopology
	 *            The original Storm topology.
	 */
	public void setStormTopology(StormTopology stormTopology) {
		this.stormTopology = stormTopology;
	}


	@Override
	public void processElement1(final StreamRecord<IN1> element) throws Exception {
		super.processElement(element);
	}

	@Override
	public void processElement2(StreamRecord<IN2> element) throws Exception {
		this.flinkCollector.setTimestamp(element.getTimestamp());
		IN2 value = element.getValue();
		this.bolt.execute(new StormTuple<>(value, inputSchema2, topologyContext.getThisTaskId(), streamId2, componentId2));
	}

	@Override
	public void processWatermark1(Watermark mark) throws Exception {
		super.processWatermark(mark);
	}

	@Override
	public void processWatermark2(Watermark mark) throws Exception {
		this.output.emitWatermark(mark);
	}
}
