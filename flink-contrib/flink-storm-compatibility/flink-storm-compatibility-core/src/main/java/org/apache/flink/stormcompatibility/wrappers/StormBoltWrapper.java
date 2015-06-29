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
package org.apache.flink.stormcompatibility.wrappers;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.tuple.Fields;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple25;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;





/**
 * A {@link StormBoltWrapper} wraps an {@link IRichBolt} in order to execute the Storm bolt within a Flink Streaming
 * program. It takes the Flink input tuples of type {@code IN} and transforms them into {@link StormTuple}s that the
 * bolt can process. Furthermore, it takes the bolt's output tuples and transforms them into Flink tuples of type
 * {@code OUT} (see {@link AbstractStormCollector} for supported types).<br />
 * <br />
 * <strong>CAUTION: currently, only simple bolts are supported! (ie, bolts that do not use the Storm configuration
 * <code>Map</code> or <code>TopologyContext</code> that is provided by the bolt's <code>open(..)</code> method.
 * Furthermore, acking and failing of tuples as well as accessing tuple attributes by field names is not supported so
 * far.</strong>
 */
public class StormBoltWrapper<IN, OUT> extends AbstractStreamOperator<OUT> implements OneInputStreamOperator<IN, OUT> {
	private static final long serialVersionUID = -4788589118464155835L;

	/** The wrapped Storm {@link IRichBolt bolt} */
	private final IRichBolt bolt;
	/** Number of attributes of the bolt's output tuples */
	private final int numberOfAttributes;
	/** The schema (ie, ordered field names) of the input stream. */
	private final Fields inputSchema;

	/**
	 *  We have to use this because Operators must output
	 *  {@link org.apache.flink.streaming.runtime.streamrecord.StreamRecord}.
	 */
	private TimestampedCollector<OUT> flinkCollector;

	/**
	 * Instantiates a new {@link StormBoltWrapper} that wraps the given Storm {@link IRichBolt bolt} such that it can be
	 * used within a Flink streaming program. As no input schema is defined, attribute-by-name access in only possible
	 * for POJO input types. The output type will be one of {@link Tuple1} to {@link Tuple25} depending on the bolt's
	 * declared number of attributes.
	 * 
	 * @param bolt
	 * 		The Storm {@link IRichBolt bolt} to be used.
	 * @throws IllegalArgumentException
	 * 		If the number of declared output attributes is not with range [1;25].
	 */
	public StormBoltWrapper(final IRichBolt bolt) throws IllegalArgumentException {
		this(bolt, null, false);
	}

	/**
	 * Instantiates a new {@link StormBoltWrapper} that wraps the given Storm {@link IRichBolt bolt} such that it can be
	 * used within a Flink streaming program. The given input schema enable attribute-by-name access for input types
	 * {@link Tuple1} to {@link Tuple25}. The output type will be one of {@link Tuple1} to {@link Tuple25} depending on
	 * the bolt's declared number of attributes.
	 * 
	 * @param bolt
	 * 		The Storm {@link IRichBolt bolt} to be used.
	 * @param inputSchema
	 * 		The schema (ie, ordered field names) of the input stream.
	 * @throws IllegalArgumentException
	 * 		If the number of declared output attributes is not with range [1;25].
	 */
	public StormBoltWrapper(final IRichBolt bolt, final Fields inputSchema)
			throws IllegalArgumentException {
		this(bolt, inputSchema, false);
	}

	/**
	 * Instantiates a new {@link StormBoltWrapper} that wraps the given Storm {@link IRichBolt bolt} such that it can be
	 * used within a Flink streaming program. As no input schema is defined, attribute-by-name access in only possible
	 * for POJO input types. The output type can be any type if parameter {@code rawOutput} is {@code true} and the
	 * bolt's number of declared output tuples is 1. If {@code rawOutput} is {@code false} the output type will be one
	 * of {@link Tuple1} to {@link Tuple25} depending on the bolt's declared number of attributes.
	 * 
	 * @param bolt
	 * 		The Storm {@link IRichBolt bolt} to be used.
	 * @param rawOutput
	 * 		Set to {@code true} if a single attribute output stream, should not be of type
	 * 		{@link Tuple1} but be of a raw type.
	 * @throws IllegalArgumentException
	 * 		If {@code rawOuput} is {@code true} and the number of declared output attributes is
	 * 		not 1 or if {@code rawOuput} is {@code false} and the number of declared output
	 * 		attributes is not with range [1;25].
	 */
	public StormBoltWrapper(final IRichBolt bolt, final boolean rawOutput)
			throws IllegalArgumentException {
		this(bolt, null, rawOutput);
	}

	/**
	 * Instantiates a new {@link StormBoltWrapper} that wraps the given Storm {@link IRichBolt bolt} such that it can be
	 * used within a Flink streaming program. The given input schema enable attribute-by-name access for input types
	 * {@link Tuple1} to {@link Tuple25}. The output type can be any type if parameter {@code rawOutput} is {@code true}
	 * and the bolt's number of declared output tuples is 1. If {@code rawOutput} is {@code false} the output type will
	 * be one of {@link Tuple1} to {@link Tuple25} depending on the bolt's declared number of attributes.
	 * 
	 * @param bolt
	 * 		The Storm {@link IRichBolt bolt} to be used.
	 * @param inputSchema
	 * 		The schema (ie, ordered field names) of the input stream.
	 * @param rawOutput
	 * 		Set to {@code true} if a single attribute output stream, should not be of type
	 * 		{@link Tuple1} but be of a raw type.
	 * @throws IllegalArgumentException
	 * 		If {@code rawOuput} is {@code true} and the number of declared output attributes is
	 * 		not 1 or if {@code rawOuput} is {@code false} and the number of declared output
	 * 		attributes is not with range [1;25].
	 */
	public StormBoltWrapper(final IRichBolt bolt, final Fields inputSchema, final boolean rawOutput)
			throws IllegalArgumentException {
		this.bolt = bolt;
		this.inputSchema = inputSchema;
		this.numberOfAttributes = StormWrapperSetupHelper.getNumberOfAttributes(bolt, rawOutput);
	}

	@Override
	public void open(final Configuration parameters) throws Exception {
		super.open(parameters);

		final TopologyContext topologyContext = StormWrapperSetupHelper.convertToTopologyContext(
				super.runtimeContext, false);
		flinkCollector = new TimestampedCollector<OUT>(output);
		OutputCollector stormCollector = null;

		if (this.numberOfAttributes != -1) {
			stormCollector = new OutputCollector(new StormBoltCollector<OUT>(
					this.numberOfAttributes, flinkCollector));
		}

		this.bolt.prepare(null, topologyContext, stormCollector);
	}

	@Override
	public void close() throws Exception {
		super.close();
		this.bolt.cleanup();
	}

	@Override
	public void processElement(final StreamRecord<IN> element) throws Exception {
		flinkCollector.setTimestamp(element.getTimestamp());
		this.bolt.execute(new StormTuple<IN>(element.getValue(), inputSchema));
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		output.emitWatermark(mark);
	}

}
