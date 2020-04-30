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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.hadoopcompatibility.mapred.HadoopMapFunction;
import org.apache.flink.test.util.MultipleProgramsTestBase;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;

/**
 * IT cases for the {@link HadoopMapFunction}.
 */
@RunWith(Parameterized.class)
public class HadoopMapFunctionITCase extends MultipleProgramsTestBase {

	public HadoopMapFunctionITCase(TestExecutionMode mode){
		super(mode);
	}

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void testNonPassingMapper() throws Exception{
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<IntWritable, Text>> ds = HadoopTestData.getKVPairDataSet(env);
		DataSet<Tuple2<IntWritable, Text>> nonPassingFlatMapDs = ds.
				flatMap(new HadoopMapFunction<IntWritable, Text, IntWritable, Text>(new NonPassingMapper()));

		String resultPath = tempFolder.newFile().toURI().toString();

		nonPassingFlatMapDs.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();

		compareResultsByLinesInMemory("\n", resultPath);
	}

	@Test
	public void testDataDuplicatingMapper() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<IntWritable, Text>> ds = HadoopTestData.getKVPairDataSet(env);
		DataSet<Tuple2<IntWritable, Text>> duplicatingFlatMapDs = ds.
				flatMap(new HadoopMapFunction<IntWritable, Text, IntWritable, Text>(new DuplicatingMapper()));

		String resultPath = tempFolder.newFile().toURI().toString();

		duplicatingFlatMapDs.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();

		String expected = "(1,Hi)\n" + "(1,HI)\n" +
				"(2,Hello)\n" + "(2,HELLO)\n" +
				"(3,Hello world)\n" + "(3,HELLO WORLD)\n" +
				"(4,Hello world, how are you?)\n" + "(4,HELLO WORLD, HOW ARE YOU?)\n" +
				"(5,I am fine.)\n" + "(5,I AM FINE.)\n" +
				"(6,Luke Skywalker)\n" + "(6,LUKE SKYWALKER)\n" +
				"(7,Comment#1)\n" + "(7,COMMENT#1)\n" +
				"(8,Comment#2)\n" + "(8,COMMENT#2)\n" +
				"(9,Comment#3)\n" + "(9,COMMENT#3)\n" +
				"(10,Comment#4)\n" + "(10,COMMENT#4)\n" +
				"(11,Comment#5)\n" + "(11,COMMENT#5)\n" +
				"(12,Comment#6)\n" + "(12,COMMENT#6)\n" +
				"(13,Comment#7)\n" + "(13,COMMENT#7)\n" +
				"(14,Comment#8)\n" + "(14,COMMENT#8)\n" +
				"(15,Comment#9)\n" + "(15,COMMENT#9)\n" +
				"(16,Comment#10)\n" + "(16,COMMENT#10)\n" +
				"(17,Comment#11)\n" + "(17,COMMENT#11)\n" +
				"(18,Comment#12)\n" + "(18,COMMENT#12)\n" +
				"(19,Comment#13)\n" + "(19,COMMENT#13)\n" +
				"(20,Comment#14)\n" + "(20,COMMENT#14)\n" +
				"(21,Comment#15)\n" + "(21,COMMENT#15)\n";

		compareResultsByLinesInMemory(expected, resultPath);
	}

	@Test
	public void testConfigurableMapper() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		JobConf conf = new JobConf();
		conf.set("my.filterPrefix", "Hello");

		DataSet<Tuple2<IntWritable, Text>> ds = HadoopTestData.getKVPairDataSet(env);
		DataSet<Tuple2<IntWritable, Text>> hellos = ds.
				flatMap(new HadoopMapFunction<IntWritable, Text, IntWritable, Text>(new ConfigurableMapper(), conf));

		String resultPath = tempFolder.newFile().toURI().toString();

		hellos.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();

		String expected = "(2,Hello)\n" +
				"(3,Hello world)\n" +
				"(4,Hello world, how are you?)\n";

		compareResultsByLinesInMemory(expected, resultPath);
	}

	/**
	 * {@link Mapper} that only forwards records containing "bananas".
	 */
	public static class NonPassingMapper implements Mapper<IntWritable, Text, IntWritable, Text> {

		@Override
		public void map(final IntWritable k, final Text v,
				final OutputCollector<IntWritable, Text> out, final Reporter r) throws IOException {
			if (v.toString().contains("bananas")) {
				out.collect(k, v);
			}
		}

		@Override
		public void configure(final JobConf arg0) { }

		@Override
		public void close() throws IOException { }
	}

	/**
	 * {@link Mapper} that duplicates records.
	 */
	public static class DuplicatingMapper implements Mapper<IntWritable, Text, IntWritable, Text> {

		@Override
		public void map(final IntWritable k, final Text v,
				final OutputCollector<IntWritable, Text> out, final Reporter r) throws IOException {
			out.collect(k, v);
			out.collect(k, new Text(v.toString().toUpperCase()));
		}

		@Override
		public void configure(final JobConf arg0) { }

		@Override
		public void close() throws IOException { }
	}

	/**
	 * {@link Mapper} that filters records based on a prefix.
	 */
	public static class ConfigurableMapper implements Mapper<IntWritable, Text, IntWritable, Text> {
		private String filterPrefix;

		@Override
		public void map(IntWritable k, Text v, OutputCollector<IntWritable, Text> out, Reporter r)
				throws IOException {
			if (v.toString().startsWith(filterPrefix)) {
				out.collect(k, v);
			}
		}

		@Override
		public void configure(JobConf c) {
			filterPrefix = c.get("my.filterPrefix");
		}

		@Override
		public void close() throws IOException { }
	}
}
