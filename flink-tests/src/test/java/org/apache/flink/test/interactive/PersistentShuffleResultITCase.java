/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.interactive;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.interactive.DefaultIntermediateResultSummary;
import org.apache.flink.api.common.interactive.IntermediateResultDescriptors;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.BlockingShuffleOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * Test Persistent Shuffle Result.
 */
public class PersistentShuffleResultITCase extends TestLogger {

	@Test
	public void testReportingPersistentIntermediateResultSummaryToClient() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(1);

		DataSet<Tuple2<Long, Long>> input = env.fromElements(new Tuple2<>(1L, 2L));

		DataSet ds = input.map((MapFunction<Tuple2<Long, Long>, Object>) value -> new Tuple2<>(value.f0 + 1, value.f1));

		// specify IntermediateDataSetID
		AbstractID intermediateDataSetId = new AbstractID();

		// this output branch will be excluded.
		ds.output(BlockingShuffleOutputFormat.createOutputFormat(intermediateDataSetId))
			.setParallelism(1);

		ds.collect();

		DefaultIntermediateResultSummary summary = env.getIntermediateResultSummary();
		IntermediateResultDescriptors descriptors = summary.getIntermediateResultDescriptors();

		// only one cached IntermediateDataSet
		Assert.assertEquals(1, descriptors.getIntermediateResultDescriptors().size());

		AbstractID intermediateDataSetID = descriptors.getIntermediateResultDescriptors().keySet().iterator().next();

		// IntermediateDataSetID should be the same
		Assert.assertEquals(intermediateDataSetID, intermediateDataSetID);

		Map<AbstractID, SerializedValue<Object>> serializedValueMap = descriptors.getIntermediateResultDescriptors().get(intermediateDataSetID);

		// only one shuffle descriptor
		Assert.assertEquals(1, serializedValueMap.size());

		SerializedValue<Object> serializedDescriptor = serializedValueMap.values().iterator().next();
		Object serializedDescriptorObj = serializedDescriptor.deserializeValue(Thread.currentThread().getContextClassLoader());
		Assert.assertThat(serializedDescriptorObj, Matchers.instanceOf(ShuffleDescriptor.class));
	}
}
