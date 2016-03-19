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

package org.apache.flink.test.javaApiOperators;

import org.apache.flink.api.common.distributions.DataDistribution;
import org.apache.flink.api.common.functions.MapFunction;
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
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets;
import org.apache.flink.util.Collector;
import org.junit.Test;


import java.io.IOException;

import static org.junit.Assert.fail;


public class CustomDistributionITCase {
	
	@Test
	public void testPartitionWithDistribution1() throws Exception{
		/*
		 * Test the record partitioned rightly with one field according to the customized data distribution
		 */

		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

		DataSet<Tuple3<Integer, Long, String>> input1 = CollectionDataSets.get3TupleDataSet(env);
		final TestDataDist dist = new TestDataDist(1);

		env.setParallelism(dist.getParallelism());

		DataSet<Boolean> result = DataSetUtils.partitionByRange(input1, dist, 0).mapPartition(new RichMapPartitionFunction<Tuple3<Integer, Long, String>, Boolean>() {
			@Override
			public void mapPartition(Iterable<Tuple3<Integer, Long, String>> values, Collector<Boolean> out) throws Exception {
				int partitionIndex = getRuntimeContext().getIndexOfThisSubtask();

				for (Tuple3<Integer, Long, String> s : values) {
					if ((s.f0 - 1) / 7 != partitionIndex) {
						fail("Record was not correctly partitioned: " + s.toString());
					}
				}
			}
		});

		result.output(new DiscardingOutputFormat()); 
		env.execute();
	}

	@Test
	public void testRangeWithDistribution2() throws Exception{
		/*
		 * Test the record partitioned rightly with two fields according to the customized data distribution
		 */

		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

		DataSet<Tuple3<Integer, Long, String>> input1 = CollectionDataSets.get3TupleDataSet(env);
		final TestDataDist dist = new TestDataDist(2);

		env.setParallelism(dist.getParallelism());

		DataSet<Boolean> result = DataSetUtils.partitionByRange(input1.map(new MapFunction<Tuple3<Integer, Long, String>, Tuple3<Integer, Integer, String>>() {
			@Override
			public Tuple3<Integer, Integer, String> map(Tuple3<Integer, Long, String> value) throws Exception {
				return new Tuple3<>(value.f0, value.f1.intValue(), value.f2);
			}
		}), dist, 0, 1).mapPartition(new RichMapPartitionFunction<Tuple3<Integer, Integer, String>, Boolean>() {
			@Override
			public void mapPartition(Iterable<Tuple3<Integer, Integer, String>> values, Collector<Boolean> out) throws Exception {
				int partitionIndex = getRuntimeContext().getIndexOfThisSubtask();

				for (Tuple3<Integer, Integer, String> s : values) {
					if (s.f0 <= partitionIndex * (partitionIndex + 1) / 2 ||
							s.f0 > (partitionIndex + 1) * (partitionIndex + 2) / 2 ||
							s.f1 - 1 != partitionIndex) {
						fail("Record was not correctly partitioned: " + s.toString());
					}
				}
			}
		});

		result.output(new DiscardingOutputFormat());
		env.execute();
	}

	/**
	 * The class is used to do the tests of range partition with customed data distribution.
	 */
	public static class TestDataDist implements DataDistribution {

		private int dim;

		public TestDataDist() {}

		/**
		 * Constructor of the customized distribution for range partition.
		 * @param dim the number of the fields.
		 */
		public TestDataDist(int dim) {
			this.dim = dim;
		}

		public int getParallelism() {
			if (dim == 1) {
				return 3;
			}
			return 6;
		}

		@Override
		public Object[] getBucketBoundary(int bucketNum, int totalNumBuckets) {
			if (dim == 1) {
				/*
				for the first test, the boundary is just like : 
				(0, 7]
				(7, 14]
				(14, 21]
				 */

				return new Integer[]{(bucketNum + 1) * 7};
			}
			/*
			for the second test, the boundary is just like : 
			(0, 1], (0, 1]
			(1, 3], (1, 2]
			(3, 6], (2, 3]
			(6, 10], (3, 4]
			(10, 15], (4, 5]
			(15, 21], (5, 6]
			 */

			return new Integer[]{(bucketNum + 1) * (bucketNum + 2) / 2, bucketNum + 1};
		}

		@Override
		public int getNumberOfFields() {
			return this.dim;
		}

		@Override
		public TypeInformation[] getKeyTypes() {
			if (dim == 1) {
				return new TypeInformation[]{BasicTypeInfo.INT_TYPE_INFO};
			}
			return new TypeInformation[]{BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO};
		}

		@Override
		public void write(DataOutputView out) throws IOException {
			out.writeInt(this.dim);
		}

		@Override
		public void read(DataInputView in) throws IOException {
			this.dim = in.readInt();
		}
	}
}
