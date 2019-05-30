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

package org.apache.flink.ml.batchoperator.dataproc;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.functions.SampleWithFraction;
import org.apache.flink.ml.batchoperator.BatchOperator;
import org.apache.flink.ml.common.utils.RowTypeDataSet;
import org.apache.flink.ml.params.ParamInfo;
import org.apache.flink.ml.params.Params;
import org.apache.flink.ml.params.shared.HasRandomSeed;
import org.apache.flink.ml.params.validators.RangeValidator;
import org.apache.flink.types.Row;

/**
 * Sample(by ratio) operation for the Batch data.
 */
public final class SampleBatchOp extends BatchOperator <SampleBatchOp> implements HasRandomSeed <SampleBatchOp> {

	public static final ParamInfo <Double> RATIO = new ParamInfo <>(
		"ratio",
		"sampling ratio, it should be in range of [0, 1]",
		false,
		Double.class,
		new RangeValidator<>(0.0, 1.0)
	);

	public static final ParamInfo <Boolean> WITH_REPLACEMENT = new ParamInfo <>(
		"withReplacement",
		"Indicates whether to enable sampling with replacement, default is without replcement",
		true,
		false,
		Boolean.class
	);

	public SampleBatchOp() {
		super(null);
	}

	public SampleBatchOp(Params params) {
		super(params);
	}

	public SampleBatchOp(double ratio) {
		super(new Params()
			.set(RATIO, ratio)
			.set(WITH_REPLACEMENT, false));
	}

	public SampleBatchOp(double ratio, boolean withReplacement) {
		super(new Params()
			.set(RATIO, ratio)
			.set(WITH_REPLACEMENT, withReplacement)
		);
	}

	public SampleBatchOp(double ratio, boolean withReplacement, long seed) {
		super(new Params()
			.set(RATIO, ratio)
			.set(WITH_REPLACEMENT, withReplacement)
			.set(HasRandomSeed.RANDOM_SEED, seed)
		);
	}

	public SampleBatchOp setRatio(Double value) {
		return set(RATIO, value);
	}

	public Double getRatio() {
		return getParams().get(RATIO);
	}

	public SampleBatchOp setWithReplacement(Boolean value) {
		return set(WITH_REPLACEMENT, value);
	}

	public Boolean getWithReplacement() {
		return getParams().get(WITH_REPLACEMENT);
	}

	@Override
	public SampleBatchOp linkFrom(BatchOperator in) {
		long randseed = getParams().contains(HasRandomSeed.RANDOM_SEED) ? getRandomSeed() : Utils.RNG.nextLong();

		DataSet <Row> rst = RowTypeDataSet.fromTable(in.getTable())
			.mapPartition(new SampleWithFraction(getWithReplacement(), getRatio(), randseed));

		this.table = RowTypeDataSet.toTable(rst, in.getSchema());
		return this;
	}

}
