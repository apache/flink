/**
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
package org.apache.flink.hadoopcompatibility.mapred.example;

import org.apache.flink.hadoopcompatibility.mapred.FlinkHadoopJobClient;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.mapred.lib.TokenCountMapper;

import java.io.IOException;

/**
 * A Hadoop job run on Flink.
 */
public class FullWordCount {

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: FullWordCount <input path> <result path>");
			return;
		}
		final String inputPath = args[0];
		final String outputPath = args[1];

		final JobConf conf = new JobConf();

		conf.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class);
		org.apache.hadoop.mapred.TextInputFormat.addInputPath(conf, new Path(inputPath));

		conf.setOutputFormat(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setMapperClass(TestTokenizeMap.class);
		conf.setReducerClass(LongSumReducer.class);
		conf.setCombinerClass((LongSumReducer.class));

		conf.set("mapred.textoutputformat.separator", " ");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(LongWritable.class);

		FlinkHadoopJobClient.runJob(conf);
	}

	public static class TestTokenizeMap<K> extends TokenCountMapper<K> {
		@Override
		public void map(K key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {
			final Text strippedValue = new Text(value.toString().toLowerCase().replaceAll("\\W+", " "));
			super.map(key, strippedValue, output, reporter);
		}
	}
}
