/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.streamoperator.dataproc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.common.utils.RowTypeDataStream;
import org.apache.flink.ml.params.shared.HasRandomSeed;
import org.apache.flink.ml.params.validators.RangeValidator;
import org.apache.flink.ml.streamoperator.StreamOperator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * Sample operator for the streaming data.
 */
public class SampleStreamOp extends StreamOperator <SampleStreamOp> implements HasRandomSeed <SampleStreamOp> {

	public static final ParamInfo <Double> RATIO = ParamInfoFactory
		.createParamInfo("ratio", Double.class)
		.setDescription("sampling ratio, it should be in range of [0, 1]")
		.setRequired()
		.setValidator(new RangeValidator <>(0.0, 1.0))
		.setAlias(new String[] {"sampleRate"})
		.build();

	public static final ParamInfo <Long> MAX_SAMPLES = ParamInfoFactory
		.createParamInfo("maxSamples", Long.class)
		.setDescription("the max records to sampled, default +inf")
		.setHasDefaultValue(Long.MAX_VALUE)
		.setValidator(new RangeValidator <>(0L, Long.MAX_VALUE))
		.build();

	public SampleStreamOp(double ratio) {
		super(new Params().set(RATIO, ratio));
	}

	public SampleStreamOp(double ratio, long maxSamples) {
		super(new Params().set(RATIO, ratio).set(MAX_SAMPLES, maxSamples));
	}

	public SampleStreamOp(Params params) {
		super(params);
	}

	public Double getRatio() {
		return getParams().get(RATIO);
	}

	public SampleStreamOp setRatio(Double value) {
		return set(RATIO, value);
	}

	public Long getMaxSamples() {
		return getParams().get(MAX_SAMPLES);
	}

	public SampleStreamOp setMaxSamples(Long value) {
		return set(MAX_SAMPLES, value);
	}

	@Override
	public SampleStreamOp linkFrom(StreamOperator in) {
		if (getRatio() >= 1) {
			this.table = in.getTable();
		} else {
			DataStream <Row> rst = RowTypeDataStream.fromTable(in.getTable())
				.flatMap(new Sampler(getRatio(), getMaxSamples()));

			this.table = RowTypeDataStream.toTable(rst, in.getSchema());
		}
		return this;
	}

	private static class Sampler implements FlatMapFunction <Row, Row> {

		private final double ratio;
		private final long maxSamples;
		private final Random rand;
		private long cnt = 0;

		public Sampler(double ratio, long maxSamples) {
			this.ratio = ratio;
			this.maxSamples = maxSamples;
			this.rand = new Random();
		}

		@Override
		public void flatMap(Row t, Collector <Row> clctr) throws Exception {
			if (cnt++ < this.maxSamples) {
				if (rand.nextDouble() <= this.ratio) {
					clctr.collect(t);
				}
			}
		}
	}

}
