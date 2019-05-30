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
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.ml.batchoperator.BatchOperator;
import org.apache.flink.ml.common.utils.RowTypeDataSet;
import org.apache.flink.types.Row;

/**
 * Sample(by size) operation for the Batch data.
 */
public class SampleWithSizeBatchOp extends BatchOperator {

	private final int numSample;
	private final boolean withReplacement;
	private final Long seed;

	public SampleWithSizeBatchOp(int numSamples) {
		super(null);
		this.numSample = numSamples;
		this.withReplacement = false;
		this.seed = null;
	}

	public SampleWithSizeBatchOp(int numSamples, boolean withReplacement) {
		super(null);
		this.numSample = numSamples;
		this.withReplacement = withReplacement;
		this.seed = null;
	}

	public SampleWithSizeBatchOp(int numSamples, boolean withReplacement, long seed) {
		super(null);
		this.numSample = numSamples;
		this.withReplacement = withReplacement;
		this.seed = seed;
	}

	@Override
	public BatchOperator linkFrom(BatchOperator in) {
		DataSet <Row> src = RowTypeDataSet.fromTable(in.getTable());
		DataSet <Row> rst;
		if (null == this.seed) {
			rst = DataSetUtils.sampleWithSize(src, withReplacement, numSample);
		} else {
			rst = DataSetUtils.sampleWithSize(src, withReplacement, numSample, seed);
		}

		this.table = RowTypeDataSet.toTable(rst, in.getSchema());
		return this;
	}

}
