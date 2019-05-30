/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.hadoopcompatibility.mapreduce;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.hadoopcompatibility.mapreduce.HadoopMapFunction;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link HadoopMapFunction} and
 * {@link org.apache.flink.hadoopcompatibility.mapreduce.wrapper.HadoopProxyMapper}.
 */
public class HadoopMapperTest {

	@Test
	public void testProxyMapper() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().enableObjectReuse();

		Job job = Job.getInstance();
		job.getConfiguration().set("my.suffix", "123");
		job.getConfiguration().set("hello", "world");

		TestHadoopMapFunction hadoopMapFunction = new TestHadoopMapFunction(new TestMapper(), job);

		DataSet<Tuple2<IntWritable, Text>> ds = env.fromCollection(Collections.singleton(new Tuple2<>(new IntWritable(1), new Text("Hi"))));
		DataSet<Tuple2<IntWritable, Text>> resultDS = ds.flatMap(hadoopMapFunction);
		List<Tuple2<IntWritable, Text>> result = resultDS.collect();

		//verify hadoop mapper
		assertEquals(1, result.size());
		assertEquals(1, result.get(0).f0.get());
		assertEquals("Hi123", result.get(0).f1.toString());
	}

	/**
	 * A test hadoop map function.
	 */
	public static class TestHadoopMapFunction extends HadoopMapFunction<IntWritable, Text, IntWritable, Text> {

		public TestHadoopMapFunction(Mapper hadoopMapper) throws IOException {
			super(hadoopMapper);
		}

		public TestHadoopMapFunction(Mapper hadoopMapper, Job conf) {
			super(hadoopMapper, conf);
		}

		@Override
		public void close() throws Exception {
			super.close();

			//verify proxy mapper
			assertNotNull(getHadoopProxyMapper().getSetupMethod());
			assertNotNull(getHadoopProxyMapper().getMapMethod());
			assertNotNull(getHadoopProxyMapper().getCleanupMethod());

			//verify proxy mapper context
			assertTrue(getMapperContext().isObjectReuseEnabled());
			assertEquals(new IntWritable(1), getMapperContext().getCurrentKey());
			assertEquals(new Text("Hi"), getMapperContext().getCurrentValue());
			assertEquals(IntWritable.class, getMapperContext().getMapOutputKeyClass());
			assertEquals(Text.class, getMapperContext().getMapOutputValueClass());
			assertEquals("world", getMapperContext().getConfiguration().get("hello"));
			assertEquals(TestMapper.class, getMapperContext().getMapperClass());
		}
	}

	/**
	 * A hadoop mapper for testing.
	 */
	public static class TestMapper extends Mapper<IntWritable, Text, IntWritable, Text> {

		private String suffix;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			suffix = context.getConfiguration().get("my.suffix");
		}

		@Override
		protected void map(IntWritable k, Text v, Context context) throws IOException, InterruptedException {
			context.write(k, new Text(v.toString() + suffix));
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {

		}
	}
}
