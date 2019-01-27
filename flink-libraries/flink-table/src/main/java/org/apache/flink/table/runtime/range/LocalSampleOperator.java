/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.range;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.sampling.IntermediateSampleData;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.codegen.CodeGenUtils;
import org.apache.flink.table.codegen.GeneratedProjection;
import org.apache.flink.table.codegen.Projection;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.AbstractStreamOperatorWithMetrics;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * LocalSampleOperator wraps the sample logic on the partition side (the first phase of distributed
 * sample algorithm). It executes the partition side sample logic.
 */
@Internal
public class LocalSampleOperator extends AbstractStreamOperatorWithMetrics<IntermediateSampleData<BaseRow>>
		implements OneInputStreamOperator<BaseRow, IntermediateSampleData<BaseRow>> {

	private ReservoirSamplerWithoutReplacement sampler;
	private transient Collector<IntermediateSampleData<BaseRow>> collector;
	private GeneratedProjection localSampleProjection;
	private int numSample;

	public LocalSampleOperator(GeneratedProjection localSampleProjection, int numSample) {
		this.numSample = numSample;
		this.localSampleProjection = localSampleProjection;
	}

	@Override
	public void open() throws Exception {
		super.open();
		this.collector = new StreamRecordCollector<>(output);
		sampler = new ReservoirSamplerWithoutReplacement(numSample, System.nanoTime());
		Class<Projection> buildProjectionClass = CodeGenUtils.compile(
				getUserCodeClassloader(),
				localSampleProjection.name(),
				localSampleProjection.code());
		localSampleProjection = null;
		sampler.setProjection(buildProjectionClass.newInstance());
	}

	@Override
	public void processElement(StreamRecord<BaseRow> streamRecord) throws Exception {
		sampler.collectPartitionData(streamRecord.getValue());
	}

	@Override
	public void endInput() throws Exception {
		Iterator<IntermediateSampleData<BaseRow>> sampled = sampler.sample();
		while (sampled.hasNext()) {
			collector.collect(sampled.next());
		}
	}
}
