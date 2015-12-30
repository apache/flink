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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.util.Collector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TypeHintITCase extends JavaProgramTestBase {

	private static int NUM_PROGRAMS = 9;

	private int curProgId = config.getInteger("ProgramId", -1);

	public TypeHintITCase(Configuration config) {
		super(config);
	}

	@Override
	protected void testProgram() throws Exception {
		TypeHintProgs.runProgram(curProgId);
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() throws FileNotFoundException, IOException {

		LinkedList<Configuration> tConfigs = new LinkedList<Configuration>();

		for(int i=1; i <= NUM_PROGRAMS; i++) {
			Configuration config = new Configuration();
			config.setInteger("ProgramId", i);
			tConfigs.add(config);
		}

		return toParameterList(tConfigs);
	}

	private static class TypeHintProgs {

		public static void runProgram(int progId) throws Exception {
			switch(progId) {
			// Test identity map with missing types and string type hint
			case 1: {
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.getSmall3TupleDataSet(env);
				DataSet<Tuple3<Integer, Long, String>> identityMapDs = ds
						.map(new Mapper<Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>>())
						.returns("Tuple3<Integer, Long, String>");
				List<Tuple3<Integer, Long, String>> result = identityMapDs.collect();

				String expectedResult = "(2,2,Hello)\n" +
						"(3,2,Hello world)\n" +
						"(1,1,Hi)\n";

				compareResultAsText(result, expectedResult);
				break;
			}
			// Test identity map with missing types and type information type hint
			case 2: {
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.getSmall3TupleDataSet(env);
				DataSet<Tuple3<Integer, Long, String>> identityMapDs = ds
						// all following generics get erased during compilation
						.map(new Mapper<Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>>())
						.returns(new TupleTypeInfo<Tuple3<Integer, Long, String>>(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO));
				List<Tuple3<Integer, Long, String>> result = identityMapDs
						.collect();

				String expectedResult = "(2,2,Hello)\n" +
						"(3,2,Hello world)\n" +
						"(1,1,Hi)\n";

				compareResultAsText(result, expectedResult);
				break;
			}
			// Test flat map with class type hint
			case 3: {
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.getSmall3TupleDataSet(env);
				DataSet<Integer> identityMapDs = ds
						.flatMap(new FlatMapper<Tuple3<Integer, Long, String>, Integer>())
						.returns(Integer.class);
				List<Integer> result = identityMapDs.collect();

				String expectedResult = "2\n" +
						"3\n" +
						"1\n";

				compareResultAsText(result, expectedResult);
				break;
			}
			// Test join with type information type hint
			case 4: {
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

				DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
				DataSet<Tuple3<Integer, Long, String>> ds2 = CollectionDataSets.getSmall3TupleDataSet(env);
				DataSet<Integer> resultDs = ds1
						.join(ds2)
						.where(0)
						.equalTo(0)
						.with(new Joiner<Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>, Integer>())
						.returns(BasicTypeInfo.INT_TYPE_INFO);
				List<Integer> result = resultDs.collect();

				String expectedResult = "2\n" +
						"3\n" +
						"1\n";

				compareResultAsText(result, expectedResult);
				break;
			}
			// Test flat join with type information type hint
			case 5: {
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

				DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
				DataSet<Tuple3<Integer, Long, String>> ds2 = CollectionDataSets.getSmall3TupleDataSet(env);
				DataSet<Integer> resultDs = ds1
						.join(ds2)
						.where(0)
						.equalTo(0)
						.with(new FlatJoiner<Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>, Integer>())
						.returns(BasicTypeInfo.INT_TYPE_INFO);
				List<Integer> result = resultDs.collect();

				String expectedResult = "2\n" +
						"3\n" +
						"1\n";

				compareResultAsText(result, expectedResult);
				break;
			}
			// Test unsorted group reduce with type information type hint
			case 6: {
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.getSmall3TupleDataSet(env);
				DataSet<Integer> resultDs = ds
						.groupBy(0)
						.reduceGroup(new GroupReducer<Tuple3<Integer, Long, String>, Integer>())
						.returns(BasicTypeInfo.INT_TYPE_INFO);
				List<Integer> result = resultDs.collect();

				String expectedResult = "2\n" +
						"3\n" +
						"1\n";

				compareResultAsText(result, expectedResult);
				break;
			}
			// Test sorted group reduce with type information type hint
			case 7: {
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.getSmall3TupleDataSet(env);
				DataSet<Integer> resultDs = ds
						.groupBy(0)
						.sortGroup(0, Order.ASCENDING)
						.reduceGroup(new GroupReducer<Tuple3<Integer, Long, String>, Integer>())
						.returns(BasicTypeInfo.INT_TYPE_INFO);
				List<Integer> result = resultDs.collect();

				String expectedResult = "2\n" +
						"3\n" +
						"1\n";

				compareResultAsText(result, expectedResult);
				break;
			}
			// Test combine group with type information type hint
			case 8: {
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.getSmall3TupleDataSet(env);
				DataSet<Integer> resultDs = ds
						.groupBy(0)
						.combineGroup(new GroupCombiner<Tuple3<Integer, Long, String>, Integer>())
						.returns(BasicTypeInfo.INT_TYPE_INFO);
				List<Integer> result = resultDs.collect();

				String expectedResult = "2\n" +
						"3\n" +
						"1\n";

				compareResultAsText(result, expectedResult);
				break;
			}
			// Test cogroup with type information type hint
			case 9: {
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

				DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
				DataSet<Tuple3<Integer, Long, String>> ds2 = CollectionDataSets.getSmall3TupleDataSet(env);
				DataSet<Integer> resultDs = ds1
						.coGroup(ds2)
						.where(0)
						.equalTo(0)
						.with(new CoGrouper<Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>, Integer>())
						.returns(BasicTypeInfo.INT_TYPE_INFO);
				List<Integer> result = resultDs.collect();

				String expectedResult = "2\n" +
						"3\n" +
						"1\n";

				compareResultAsText(result, expectedResult);
				break;
			}
			default:
				throw new IllegalArgumentException("Invalid program id");
			}
		}
	}

	// --------------------------------------------------------------------------------------------

	public static class Mapper<T, V> implements MapFunction<T, V> {
		private static final long serialVersionUID = 1L;

		@SuppressWarnings("unchecked")
		@Override
		public V map(T value) throws Exception {
			return (V) value;
		}
	}

	public static class FlatMapper<T, V> implements FlatMapFunction<T, V> {
		private static final long serialVersionUID = 1L;

		@SuppressWarnings({ "unchecked", "rawtypes" })
		@Override
		public void flatMap(T value, Collector<V> out) throws Exception {
			out.collect((V) ((Tuple3)value).f0);
		}
	}

	public static class Joiner<IN1, IN2, OUT> implements JoinFunction<IN1, IN2, OUT> {
		private static final long serialVersionUID = 1L;

		@Override
		public OUT join(IN1 first, IN2 second) throws Exception {
			return (OUT) ((Tuple3) first).f0;
		}
	}

	public static class FlatJoiner<IN1, IN2, OUT> implements FlatJoinFunction<IN1, IN2, OUT> {
		private static final long serialVersionUID = 1L;

		@Override
		public void join(IN1 first, IN2 second, Collector<OUT> out) throws Exception {
			out.collect((OUT) ((Tuple3) first).f0);
		}
	}

	public static class GroupReducer<IN, OUT> implements GroupReduceFunction<IN, OUT> {
		private static final long serialVersionUID = 1L;

		@Override
		public void reduce(Iterable<IN> values, Collector<OUT> out) throws Exception {
			out.collect((OUT) ((Tuple3) values.iterator().next()).f0);
		}
	}

	public static class GroupCombiner<IN, OUT> implements GroupCombineFunction<IN, OUT> {
		private static final long serialVersionUID = 1L;

		@Override
		public void combine(Iterable<IN> values, Collector<OUT> out) throws Exception {
			out.collect((OUT) ((Tuple3) values.iterator().next()).f0);
		}
	}

	public static class CoGrouper<IN1, IN2, OUT> implements CoGroupFunction<IN1, IN2, OUT> {
		private static final long serialVersionUID = 1L;

		@Override
		public void coGroup(Iterable<IN1> first, Iterable<IN2> second, Collector<OUT> out) throws Exception {
			out.collect((OUT) ((Tuple3) first.iterator().next()).f0);
		}
	}

}
