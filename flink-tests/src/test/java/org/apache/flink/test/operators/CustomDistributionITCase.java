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

package org.apache.flink.test.operators;

import org.apache.flink.api.common.distributions.DataDistribution;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.test.operators.util.CollectionDataSets;
import org.apache.flink.test.util.MiniClusterResource;
import org.apache.flink.test.util.MiniClusterResourceConfiguration;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.fail;

/**
 * Integration tests for custom {@link DataDistribution}.
 */
@SuppressWarnings("serial")
public class CustomDistributionITCase extends TestLogger {

	@ClassRule
	public static final MiniClusterResource MINI_CLUSTER_RESOURCE = new MiniClusterResource(
		new MiniClusterResourceConfiguration.Builder()
			.setNumberTaskManagers(1)
			.setNumberSlotsPerTaskManager(8)
			.build());

	// ------------------------------------------------------------------------

	/**
	 * Test the record partitioned rightly with one field according to the customized data distribution.
	 */
	@Test
	public void testPartitionWithDistribution1() throws Exception {
		final TestDataDist1 dist = new TestDataDist1();

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(dist.getParallelism());

		DataSet<Tuple3<Integer, Long, String>> input = CollectionDataSets.get3TupleDataSet(env);

		DataSet<Boolean> result = DataSetUtils
			.partitionByRange(input, dist, 0)
			.mapPartition(new RichMapPartitionFunction<Tuple3<Integer, Long, String>, Boolean>() {

				@Override
				public void mapPartition(Iterable<Tuple3<Integer, Long, String>> values, Collector<Boolean> out) throws Exception {
					int pIdx = getRuntimeContext().getIndexOfThisSubtask();

					for (Tuple3<Integer, Long, String> s : values) {
						boolean correctlyPartitioned = true;
						if (pIdx == 0) {
							Integer[] upper = dist.boundaries[0];
							if (s.f0.compareTo(upper[0]) > 0) {
								correctlyPartitioned = false;
							}
						}
						else if (pIdx > 0 && pIdx < dist.getParallelism() - 1) {
							Integer[] lower = dist.boundaries[pIdx - 1];
							Integer[] upper = dist.boundaries[pIdx];
							if (s.f0.compareTo(upper[0]) > 0 || (s.f0.compareTo(lower[0]) <= 0)) {
								correctlyPartitioned = false;
							}
						}
						else {
							Integer[] lower = dist.boundaries[pIdx - 1];
							if ((s.f0.compareTo(lower[0]) <= 0)) {
								correctlyPartitioned = false;
							}
						}

						if (!correctlyPartitioned) {
							fail("Record was not correctly partitioned: " + s.toString());
						}
					}
				}
			}
			);

		result.output(new DiscardingOutputFormat<Boolean>());
		env.execute();
	}

	/**
	 * Test the record partitioned rightly with two fields according to the customized data distribution.
	 */
	@Test
	public void testRangeWithDistribution2() throws Exception {
		final TestDataDist2 dist = new TestDataDist2();

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(dist.getParallelism());

		DataSet<Tuple3<Integer, Integer, String>> input = env.fromElements(
						new Tuple3<>(1, 5, "Hi"),
						new Tuple3<>(1, 6, "Hi"),
						new Tuple3<>(1, 7, "Hi"),
						new Tuple3<>(1, 11, "Hello"),
						new Tuple3<>(2, 3, "World"),
						new Tuple3<>(2, 4, "World"),
						new Tuple3<>(2, 5, "World"),
						new Tuple3<>(2, 13, "Hello World"),
						new Tuple3<>(3, 8, "Say"),
						new Tuple3<>(4, 0, "Why"),
						new Tuple3<>(4, 2, "Java"),
						new Tuple3<>(4, 11, "Say Hello"),
						new Tuple3<>(5, 1, "Hi Java!"),
						new Tuple3<>(5, 2, "Hi Java?"),
						new Tuple3<>(5, 3, "Hi Java again")
			);

		DataSet<Boolean> result = DataSetUtils
			.partitionByRange(input, dist, 0, 1)
			.mapPartition(new RichMapPartitionFunction<Tuple3<Integer, Integer, String>, Boolean>() {

				@Override
				public void mapPartition(Iterable<Tuple3<Integer, Integer, String>> values, Collector<Boolean> out) throws Exception {
					int pIdx = getRuntimeContext().getIndexOfThisSubtask();
					boolean correctlyPartitioned = true;

					for (Tuple3<Integer, Integer, String> s : values) {

						if (pIdx == 0) {
							Integer[] upper = dist.boundaries[0];
							if (s.f0.compareTo(upper[0]) > 0 ||
								(s.f0.compareTo(upper[0]) == 0 && s.f1.compareTo(upper[1]) > 0)) {
								correctlyPartitioned = false;
							}
						}
						else if (pIdx > 0 && pIdx < dist.getParallelism() - 1) {
							Integer[] lower = dist.boundaries[pIdx - 1];
							Integer[] upper = dist.boundaries[pIdx];

							if (s.f0.compareTo(upper[0]) > 0 ||
								(s.f0.compareTo(upper[0]) == 0 && s.f1.compareTo(upper[1]) > 0) ||
								(s.f0.compareTo(lower[0]) < 0) ||
								(s.f0.compareTo(lower[0]) == 0 && s.f1.compareTo(lower[1]) <= 0)) {
								correctlyPartitioned = false;
							}
						}
						else {
							Integer[] lower = dist.boundaries[pIdx - 1];
							if ((s.f0.compareTo(lower[0]) < 0) ||
								(s.f0.compareTo(lower[0]) == 0 && s.f1.compareTo(lower[1]) <= 0)) {
								correctlyPartitioned = false;
							}
						}

						if (!correctlyPartitioned) {
							fail("Record was not correctly partitioned: " + s.toString());
						}
					}
				}
			}
			);

		result.output(new DiscardingOutputFormat<Boolean>());
		env.execute();
	}

	/*
	 * Test the number of partition keys less than the number of distribution fields
	 */
	@Test
	public void testPartitionKeyLessDistribution() throws Exception {
		final TestDataDist2 dist = new TestDataDist2();

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(dist.getParallelism());

		DataSet<Tuple3<Integer, Long, String>> input = CollectionDataSets.get3TupleDataSet(env);

		DataSet<Boolean> result = DataSetUtils
			.partitionByRange(input, dist, 0)
			.mapPartition(new RichMapPartitionFunction<Tuple3<Integer, Long, String>, Boolean>() {

				@Override
				public void mapPartition(Iterable<Tuple3<Integer, Long, String>> values, Collector<Boolean> out) throws Exception {
					int pIdx = getRuntimeContext().getIndexOfThisSubtask();

					for (Tuple3<Integer, Long, String> s : values) {
						boolean correctlyPartitioned = true;
						if (pIdx == 0) {
							Integer[] upper = dist.boundaries[0];
							if (s.f0.compareTo(upper[0]) > 0) {
								correctlyPartitioned = false;
							}
						}
						else if (pIdx > 0 && pIdx < dist.getParallelism() - 1) {
							Integer[] lower = dist.boundaries[pIdx - 1];
							Integer[] upper = dist.boundaries[pIdx];
							if (s.f0.compareTo(upper[0]) > 0 || (s.f0.compareTo(lower[0]) <= 0)) {
								correctlyPartitioned = false;
							}
						}
						else {
							Integer[] lower = dist.boundaries[pIdx - 1];
							if ((s.f0.compareTo(lower[0]) <= 0)) {
								correctlyPartitioned = false;
							}
						}

						if (!correctlyPartitioned) {
							fail("Record was not correctly partitioned: " + s.toString());
						}
					}
				}
			}
			);

		result.output(new DiscardingOutputFormat<Boolean>());
		env.execute();
	}

	/*
	 * Test the number of partition keys larger than the number of distribution fields
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testPartitionMoreThanDistribution() throws Exception {
		final TestDataDist2 dist = new TestDataDist2();

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> input = CollectionDataSets.get3TupleDataSet(env);
		DataSetUtils.partitionByRange(input, dist, 0, 1, 2);
	}

	/**
	 * The class is used to do the tests of range partition with one key.
	 */
	public static class TestDataDist1 implements DataDistribution {

		public Integer[][] boundaries = new Integer[][]{
			new Integer[]{4},
			new Integer[]{9},
			new Integer[]{13},
			new Integer[]{18}
		};

		public TestDataDist1() {}

		public int getParallelism() {
			return boundaries.length;
		}

		@Override
		public Object[] getBucketBoundary(int bucketNum, int totalNumBuckets) {
			return boundaries[bucketNum];
		}

		@Override
		public int getNumberOfFields() {
			return 1;
		}

		@Override
		public TypeInformation[] getKeyTypes() {
			return new TypeInformation[]{BasicTypeInfo.INT_TYPE_INFO};
		}

		@Override
		public void write(DataOutputView out) throws IOException {}

		@Override
		public void read(DataInputView in) throws IOException {}
	}

	/**
	 * The class is used to do the tests of range partition with two keys.
	 */
	public static class TestDataDist2 implements DataDistribution {

		public Integer[][] boundaries = new Integer[][]{
			new Integer[]{1, 6},
			new Integer[]{2, 4},
			new Integer[]{3, 9},
			new Integer[]{4, 1},
			new Integer[]{5, 2}
		};

		public TestDataDist2() {}

		public int getParallelism() {
			return boundaries.length;
		}

		@Override
		public Object[] getBucketBoundary(int bucketNum, int totalNumBuckets) {
			return boundaries[bucketNum];
		}

		@Override
		public int getNumberOfFields() {
			return 2;
		}

		@Override
		public TypeInformation[] getKeyTypes() {
			return new TypeInformation[]{BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO};
		}

		@Override
		public void write(DataOutputView out) throws IOException {}

		@Override
		public void read(DataInputView in) throws IOException {}
	}
}
