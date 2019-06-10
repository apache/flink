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

package org.apache.flink.ml.common.statistics.basicstatistic;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.ml.common.matrix.Vector;
import org.apache.flink.util.Collector;

/**
 * MapPartitionFunction of VectorSummarizer .
 */
public class VectorSummarizerPartition implements MapPartitionFunction <Vector, BaseVectorSummarizer> {
	private boolean bCov;

	public VectorSummarizerPartition(boolean bCov) {
		this.bCov = bCov;
	}

	@Override
	public void mapPartition(Iterable <Vector> iterable, Collector <BaseVectorSummarizer> collector) throws Exception {
		BaseVectorSummarizer vsrt = new DenseVectorSummarizer(bCov);
		for (Vector sv : iterable) {
			vsrt = vsrt.visit(sv);
		}

		collector.collect(vsrt);
	}

}
