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

package org.apache.flink.test.hadoopcompatibility.mapred;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.hadoopcompatibility.mapred.HadoopReduceFunction;
import org.apache.flink.test.util.MultipleProgramsTestBase;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Iterator;

/**
 * IT cases for the {@link HadoopReduceFunction}.
 */
@RunWith(Parameterized.class)
public class HadoopReduceFunctionITCase extends MultipleProgramsTestBase {

	public HadoopReduceFunctionITCase(TestExecutionMode mode){
		super(mode);
	}

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void testStandardGrouping() throws Exception{
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<IntWritable, Text>> ds = HadoopTestData.getKVPairDataSet(env).
				map(new Mapper1());

		DataSet<Tuple2<IntWritable, IntWritable>> commentCnts = ds.
				groupBy(0).
				reduceGroup(new HadoopReduceFunction<IntWritable, Text, IntWritable, IntWritable>(new CommentCntReducer()));

		String resultPath = tempFolder.newFile().toURI().toString();

		commentCnts.writeAsText(resultPath);
		env.execute();

		String expected = "(0,0)\n" +
				"(1,3)\n" +
				"(2,5)\n" +
				"(3,5)\n" +
				"(4,2)\n";

		compareResultsByLinesInMemory(expected, resultPath);
	}

	@Test
	public void testUngroupedHadoopReducer() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<IntWritable, Text>> ds = HadoopTestData.getKVPairDataSet(env);

		DataSet<Tuple2<IntWritable, IntWritable>> commentCnts = ds.
				reduceGroup(new HadoopReduceFunction<IntWritable, Text, IntWritable, IntWritable>(new AllCommentCntReducer()));

		String resultPath = tempFolder.newFile().toURI().toString();

		commentCnts.writeAsText(resultPath);
		env.execute();

		String expected = "(42,15)\n";

		compareResultsByLinesInMemory(expected, resultPath);
	}

	@Test
	public void testConfigurationViaJobConf() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		JobConf conf = new JobConf();
		conf.set("my.cntPrefix", "Hello");

		DataSet<Tuple2<IntWritable, Text>> ds = HadoopTestData.getKVPairDataSet(env).
				map(new Mapper2());

		DataSet<Tuple2<IntWritable, IntWritable>> helloCnts = ds.
				groupBy(0).
				reduceGroup(new HadoopReduceFunction<IntWritable, Text, IntWritable, IntWritable>(
						new ConfigurableCntReducer(), conf));

		String resultPath = tempFolder.newFile().toURI().toString();

		helloCnts.writeAsText(resultPath);
		env.execute();

		String expected = "(0,0)\n" +
				"(1,0)\n" +
				"(2,1)\n" +
				"(3,1)\n" +
				"(4,1)\n";

		compareResultsByLinesInMemory(expected, resultPath);
	}

	/**
	 * A {@link Reducer} to sum counts.
	 */
	public static class CommentCntReducer implements Reducer<IntWritable, Text, IntWritable, IntWritable> {

		@Override
		public void reduce(IntWritable k, Iterator<Text> vs, OutputCollector<IntWritable, IntWritable> out, Reporter r)
				throws IOException {
			int commentCnt = 0;
			while (vs.hasNext()) {
				String v = vs.next().toString();
				if (v.startsWith("Comment")) {
					commentCnt++;
				}
			}
			out.collect(k, new IntWritable(commentCnt));
		}

		@Override
		public void configure(final JobConf arg0) { }

		@Override
		public void close() throws IOException { }
	}

	/**
	 * A {@link Reducer} to sum counts.
	 */
	public static class AllCommentCntReducer implements Reducer<IntWritable, Text, IntWritable, IntWritable> {

		@Override
		public void reduce(IntWritable k, Iterator<Text> vs, OutputCollector<IntWritable, IntWritable> out, Reporter r)
				throws IOException {
			int commentCnt = 0;
			while (vs.hasNext()) {
				String v = vs.next().toString();
				if (v.startsWith("Comment")) {
					commentCnt++;
				}
			}
			out.collect(new IntWritable(42), new IntWritable(commentCnt));
		}

		@Override
		public void configure(final JobConf arg0) { }

		@Override
		public void close() throws IOException { }
	}

	/**
	 * A {@link Reducer} to sum counts for a specific prefix.
	 */
	public static class ConfigurableCntReducer implements Reducer<IntWritable, Text, IntWritable, IntWritable> {
		private String countPrefix;

		@Override
		public void reduce(IntWritable k, Iterator<Text> vs, OutputCollector<IntWritable, IntWritable> out, Reporter r)
				throws IOException {
			int commentCnt = 0;
			while (vs.hasNext()) {
				String v = vs.next().toString();
				if (v.startsWith(this.countPrefix)) {
					commentCnt++;
				}
			}
			out.collect(k, new IntWritable(commentCnt));
		}

		@Override
		public void configure(final JobConf c) {
			this.countPrefix = c.get("my.cntPrefix");
		}

		@Override
		public void close() throws IOException { }
	}

	/**
	 * Test mapper.
	 */
	public static class Mapper1 implements MapFunction<Tuple2<IntWritable, Text>, Tuple2<IntWritable, Text>> {
		private static final long serialVersionUID = 1L;
		@Override
		public Tuple2<IntWritable, Text> map(Tuple2<IntWritable, Text> v)
		throws Exception {
			v.f0 = new IntWritable(v.f0.get() / 5);
			return v;
		}
	}

	/**
	 * Test mapper.
	 */
	public static class Mapper2 implements MapFunction<Tuple2<IntWritable, Text>, Tuple2<IntWritable, Text>> {
		private static final long serialVersionUID = 1L;
		@Override
		public Tuple2<IntWritable, Text> map(Tuple2<IntWritable, Text> v)
		throws Exception {
			v.f0 = new IntWritable(v.f0.get() % 5);
			return v;
		}
	}
}
