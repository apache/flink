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

import java.util.Collection;
import java.util.HashMap;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.tuple.Fields;

import org.apache.flink.api.common.ExecutionConfig.GlobalJobParameters;
import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple25;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.stormcompatibility.util.SplitStreamType;
import org.apache.flink.stormcompatibility.util.StormConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import com.google.common.collect.Sets;

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

	/** The wrapped Storm {@link IRichBolt bolt}. */
	private final IRichBolt bolt;
	/** Number of attributes of the bolt's output tuples per stream. */
	private final HashMap<String, Integer> numberOfAttributes;
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
	 * for POJO input types. The output type will be one of {@link Tuple0} to {@link Tuple25} depending on the bolt's
	 * declared number of attributes.
	 * 
	 * @param bolt
	 *            The Storm {@link IRichBolt bolt} to be used.
	 * @throws IllegalArgumentException
	 *             If the number of declared output attributes is not with range [0;25].
	 */
	public StormBoltWrapper(final IRichBolt bolt) throws IllegalArgumentException {
		this(bolt, null, (Collection<String>) null);
	}

	/**
	 * Instantiates a new {@link StormBoltWrapper} that wraps the given Storm {@link IRichBolt bolt} such that it can be
	 * used within a Flink streaming program. The given input schema enable attribute-by-name access for input types
	 * {@link Tuple0} to {@link Tuple25}. The output type will be one of {@link Tuple0} to {@link Tuple25} depending on
	 * the bolt's declared number of attributes.
	 * 
	 * @param bolt
	 *            The Storm {@link IRichBolt bolt} to be used.
	 * @param inputSchema
	 *            The schema (ie, ordered field names) of the input stream.
	 * @throws IllegalArgumentException
	 *             If the number of declared output attributes is not with range [0;25].
	 */
	public StormBoltWrapper(final IRichBolt bolt, final Fields inputSchema)
			throws IllegalArgumentException {
		this(bolt, inputSchema, (Collection<String>) null);
	}

	/**
	 * Instantiates a new {@link StormBoltWrapper} that wraps the given Storm {@link IRichBolt bolt} such that it can be
	 * used within a Flink streaming program. As no input schema is defined, attribute-by-name access in only possible
	 * for POJO input types. The output type can be any type if parameter {@code rawOutput} is {@code true} and the
	 * bolt's number of declared output tuples is 1. If {@code rawOutput} is {@code false} the output type will be one
	 * of {@link Tuple0} to {@link Tuple25} depending on the bolt's declared number of attributes.
	 * 
	 * @param bolt
	 *            The Storm {@link IRichBolt bolt} to be used.
	 * @param rawOutputs
	 *            Contains stream names if a single attribute output stream, should not be of type {@link Tuple1} but be
	 *            of a raw type.
	 * @throws IllegalArgumentException
	 *             If {@code rawOuput} is {@code true} and the number of declared output attributes is not 1 or if
	 *             {@code rawOuput} is {@code false} and the number of declared output attributes is not with range
	 *             [1;25].
	 */
	public StormBoltWrapper(final IRichBolt bolt, final String[] rawOutputs)
			throws IllegalArgumentException {
		this(bolt, null, Sets.newHashSet(rawOutputs));
	}

	/**
	 * Instantiates a new {@link StormBoltWrapper} that wraps the given Storm {@link IRichBolt bolt} such that it can be
	 * used within a Flink streaming program. As no input schema is defined, attribute-by-name access in only possible
	 * for POJO input types. The output type can be any type if parameter {@code rawOutput} is {@code true} and the
	 * bolt's number of declared output tuples is 1. If {@code rawOutput} is {@code false} the output type will be one
	 * of {@link Tuple0} to {@link Tuple25} depending on the bolt's declared number of attributes.
	 * 
	 * @param bolt
	 *            The Storm {@link IRichBolt bolt} to be used.
	 * @param rawOutputs
	 *            Contains stream names if a single attribute output stream, should not be of type {@link Tuple1} but be
	 *            of a raw type.
	 * @throws IllegalArgumentException
	 *             If {@code rawOuput} is {@code true} and the number of declared output attributes is not 1 or if
	 *             {@code rawOuput} is {@code false} and the number of declared output attributes is not with range
	 *             [1;25].
	 */
	public StormBoltWrapper(final IRichBolt bolt, final Collection<String> rawOutputs)
			throws IllegalArgumentException {
		this(bolt, null, rawOutputs);
	}

	/**
	 * Instantiates a new {@link StormBoltWrapper} that wraps the given Storm {@link IRichBolt bolt} such that it can be
	 * used within a Flink streaming program. The given input schema enable attribute-by-name access for input types
	 * {@link Tuple0} to {@link Tuple25}. The output type can be any type if parameter {@code rawOutput} is {@code true}
	 * and the bolt's number of declared output tuples is 1. If {@code rawOutput} is {@code false} the output type will
	 * be one of {@link Tuple0} to {@link Tuple25} depending on the bolt's declared number of attributes.
	 * 
	 * @param bolt
	 *            The Storm {@link IRichBolt bolt} to be used.
	 * @param inputSchema
	 *            The schema (ie, ordered field names) of the input stream.
	 * @param rawOutputs
	 *            Contains stream names if a single attribute output stream, should not be of type {@link Tuple1} but be
	 *            of a raw type.
	 * @throws IllegalArgumentException
	 *             If {@code rawOuput} is {@code true} and the number of declared output attributes is not 1 or if
	 *             {@code rawOuput} is {@code false} and the number of declared output attributes is not with range
	 *             [0;25].
	 */
	public StormBoltWrapper(final IRichBolt bolt, final Fields inputSchema,
			final String[] rawOutputs) throws IllegalArgumentException {
		this(bolt, inputSchema, Sets.newHashSet(rawOutputs));
	}

	/**
	 * Instantiates a new {@link StormBoltWrapper} that wraps the given Storm {@link IRichBolt bolt} such that it can be
	 * used within a Flink streaming program. The given input schema enable attribute-by-name access for input types
	 * {@link Tuple0} to {@link Tuple25}. The output type can be any type if parameter {@code rawOutput} is {@code true}
	 * and the bolt's number of declared output tuples is 1. If {@code rawOutput} is {@code false} the output type will
	 * be one of {@link Tuple0} to {@link Tuple25} depending on the bolt's declared number of attributes.
	 * 
	 * @param bolt
	 *            The Storm {@link IRichBolt bolt} to be used.
	 * @param inputSchema
	 *            The schema (ie, ordered field names) of the input stream.
	 * @param rawOutputs
	 *            Contains stream names if a single attribute output stream, should not be of type {@link Tuple1} but be
	 *            of a raw type.
	 * @throws IllegalArgumentException
	 *             If {@code rawOuput} is {@code true} and the number of declared output attributes is not 1 or if
	 *             {@code rawOuput} is {@code false} and the number of declared output attributes is not with range
	 *             [0;25].
	 */
	public StormBoltWrapper(final IRichBolt bolt, final Fields inputSchema,
			final Collection<String> rawOutputs) throws IllegalArgumentException {
		this.bolt = bolt;
		this.inputSchema = inputSchema;
		this.numberOfAttributes = StormWrapperSetupHelper.getNumberOfAttributes(bolt, rawOutputs);
	}

	@Override
	public void open(final Configuration parameters) throws Exception {
		super.open(parameters);

		final TopologyContext topologyContext = StormWrapperSetupHelper.convertToTopologyContext(
				super.runtimeContext, false);
		flinkCollector = new TimestampedCollector<OUT>(output);
		OutputCollector stormCollector = null;

		if (this.numberOfAttributes.size() > 0) {
			stormCollector = new OutputCollector(new StormBoltCollector<OUT>(
					this.numberOfAttributes, flinkCollector));
		}

		GlobalJobParameters config = super.executionConfig.getGlobalJobParameters();
		StormConfig stormConfig = new StormConfig();

		if (config != null) {
			if (config instanceof StormConfig) {
				stormConfig = (StormConfig) config;
			} else {
				stormConfig.putAll(config.toMap());
			}
		}

		this.bolt.prepare(stormConfig, topologyContext, stormCollector);
	}

	@Override
	public void dispose() {
		this.bolt.cleanup();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void processElement(final StreamRecord<IN> element) throws Exception {
		flinkCollector.setTimestamp(element.getTimestamp());
		IN value = element.getValue();
		if (value instanceof SplitStreamType) {
			this.bolt.execute(new StormTuple<IN>(((SplitStreamType<IN>) value).value,
					inputSchema));
		} else {
			this.bolt.execute(new StormTuple<IN>(value, inputSchema));
		}
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		output.emitWatermark(mark);
	}

}
