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

package org.apache.flink.test.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.accumulators.ContinuousHistogram;
import org.apache.flink.api.common.accumulators.DiscreteHistogram;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class DataSetUtilsITCase extends MultipleProgramsTestBase {

	public DataSetUtilsITCase(TestExecutionMode mode) {
		super(mode);
	}

	@Test
	public void testZipWithIndex() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		long expectedSize = 100L;
		DataSet<Long> numbers = env.generateSequence(0, expectedSize - 1);

		List<Tuple2<Long, Long>> result = Lists.newArrayList(DataSetUtils.zipWithIndex(numbers).collect());

		Assert.assertEquals(expectedSize, result.size());
		// sort result by created index
		Collections.sort(result, new Comparator<Tuple2<Long, Long>>() {
			@Override
			public int compare(Tuple2<Long, Long> o1, Tuple2<Long, Long> o2) {
				return o1.f0.compareTo(o2.f0);
			}
		});
		// test if index is consecutive
		for (int i = 0; i < expectedSize; i++) {
			Assert.assertEquals(i, result.get(i).f0.longValue());
		}
	}

	@Test
	public void testZipWithUniqueId() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		long expectedSize = 100L;
		DataSet<Long> numbers = env.generateSequence(1L, expectedSize);

		DataSet<Long> ids = DataSetUtils.zipWithUniqueId(numbers).map(new MapFunction<Tuple2<Long,Long>, Long>() {
			@Override
			public Long map(Tuple2<Long, Long> value) throws Exception {
				return value.f0;
			}
		});

		Set<Long> result = Sets.newHashSet(ids.collect());

		Assert.assertEquals(expectedSize, result.size());
	}

	@Test
	public void testHistogram() throws Exception {
		DataSet<Double> data = ExecutionEnvironment.getExecutionEnvironment().fromElements(1.0, 2.0,
				3.0, 5.0, 1.0, 7.0, 9.0, 1.0, 0.0, 1.0, 4.0, 6.0, 7.0, 9.0, 4.0, 3.0, 1.0, 4.0, 6.0,
				8.0, 4.0, 3.0, 6.0, 8.0, 4.0, 3.0, 6.0, 8.0, 9.0, 7.0, 8.0, 2.0, 3.0, 6.0, 0.0)
				.setParallelism(2);

		DiscreteHistogram histogram1 = DataSetUtils.createDiscreteHistogram(data).collect().get(0);
		assertEquals(35, histogram1.getTotal());
		assertEquals(10, histogram1.getSize());
		assertEquals(2, histogram1.count(0.0));
		assertEquals(5, histogram1.count(1.0));
		assertEquals(2, histogram1.count(2.0));
		assertEquals(5, histogram1.count(3.0));
		assertEquals(5, histogram1.count(4.0));
		assertEquals(1, histogram1.count(5.0));
		assertEquals(5, histogram1.count(6.0));
		assertEquals(3, histogram1.count(7.0));
		assertEquals(4, histogram1.count(8.0));
		assertEquals(3, histogram1.count(9.0));

		ContinuousHistogram histogram2 = DataSetUtils.createContinuousHistogram(data, 5).collect().get(0);
		assertEquals(35, histogram2.getTotal());
		assertEquals(5, histogram2.getSize());
		assertEquals(4, histogram2.count(histogram2.quantile(0.1)));
		assertEquals(7, histogram2.count(histogram2.quantile(0.2)));
		assertEquals(18, histogram2.count(histogram2.quantile(0.5)));
		assertEquals(0.0, histogram2.min(), 0.0);
		assertEquals(9.0, histogram2.max(), 0.0);
		assertEquals(4.543, histogram2.mean(), 1e-3);
		assertEquals(7.619, histogram2.variance(), 1e-3);
	}
}
