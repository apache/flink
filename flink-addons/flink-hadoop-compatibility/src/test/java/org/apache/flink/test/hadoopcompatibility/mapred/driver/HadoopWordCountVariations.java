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

package org.apache.flink.test.hadoopcompatibility.mapred.driver;


import org.apache.flink.hadoopcompatibility.mapred.FlinkHadoopJobClient;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapred.lib.TokenCountMapper;

import java.io.IOException;
import java.util.Iterator;

public class HadoopWordCountVariations {

	public static class TestTokenizeMap<K> extends TokenCountMapper<K> {
		@Override
		public void map(K key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {
			final Text strippedValue = new Text(value.toString().toLowerCase().replaceAll("\\W+", " "));
			super.map(key, strippedValue, output, reporter);
		}
	}

	public static class NonGenericInputFormat {

		public static void main(String[] args) throws Exception {
			final String inputPath = args[0];
			final String outputPath = args[1];

			final JobConf conf = new JobConf();

			conf.setInputFormat(CustomTextInputFormat.class);
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

		public static class CustomTextInputFormat extends org.apache.hadoop.mapred.TextInputFormat {
		}
	}

	public static class StringTokenizer {

		public static void main(String[] args) throws Exception {
			final String inputPath = args[0];
			final String outputPath = args[1];

			final JobConf conf = new JobConf();

			conf.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class);
			org.apache.hadoop.mapred.TextInputFormat.addInputPath(conf, new Path(inputPath));

			conf.setOutputFormat(TextOutputFormat.class);
			TextOutputFormat.setOutputPath(conf, new Path(outputPath));

			conf.setMapperClass(TestTokenizeMap.class);


			conf.set("mapred.textoutputformat.separator", " ");
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(LongWritable.class);

			FlinkHadoopJobClient.runJob(conf);
		}
	}

	public static class WordCountDifferentCombiner {

		public static void main(String[] args) throws Exception {
			final String inputPath = args[0];
			final String outputPath = args[1];

			final JobConf conf = new JobConf();

			conf.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class);
			org.apache.hadoop.mapred.TextInputFormat.addInputPath(conf, new Path(inputPath));

			conf.setOutputFormat(TextOutputFormat.class);
			TextOutputFormat.setOutputPath(conf, new Path(outputPath));

			conf.setMapperClass(TestTokenizeMap.class);
			conf.setCombinerClass((LongSumReducer.class));
			conf.setReducerClass(TestReducer.class);


			conf.set("mapred.textoutputformat.separator", " ");
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(LongWritable.class);

			FlinkHadoopJobClient.runJob(conf);
		}

		public static class TestReducer<K> extends LongSumReducer<K> {

			@Override
			public void reduce(K key, Iterator<LongWritable> values, OutputCollector<K,LongWritable> output, Reporter reporter) throws IOException{
				output.collect(key, values.next());
			}

		}
	}

	public static class WordCountNoCombiner {

		public static void main(String[] args) throws Exception {
			final String inputPath = args[0];
			final String outputPath = args[1];

			final JobConf conf = new JobConf();

			conf.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class);
			org.apache.hadoop.mapred.TextInputFormat.addInputPath(conf, new Path(inputPath));

			conf.setOutputFormat(TextOutputFormat.class);
			TextOutputFormat.setOutputPath(conf, new Path(outputPath));

			conf.setMapperClass(TestTokenizeMap.class);
			conf.setReducerClass(LongSumReducer.class);

			conf.set("mapred.textoutputformat.separator", " ");
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(LongWritable.class);

			FlinkHadoopJobClient.runJob(conf);
		}
	}

	public static class WordCountSameCombiner {

		public static void main(String[] args) throws Exception {
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
	}

	public static class WordCountValueViaConf {
		public static void main(String[] args) throws Exception {
			final String inputPath = args[0];
			final String outputPath = args[1];

			final JobConf conf = new JobConf();

			conf.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class);
			org.apache.hadoop.mapred.TextInputFormat.addInputPath(conf, new Path(inputPath));

			conf.setOutputFormat(TextOutputFormat.class);
			TextOutputFormat.setOutputPath(conf, new Path(outputPath));

			conf.setMapperClass(ValueConfMapper.class);
			conf.set("mapred.textoutputformat.separator", " ");
			conf.setInt("test.value", 100);

			conf.setMapOutputKeyClass(Text.class);
			conf.setMapOutputValueClass(LongWritable.class);

			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(LongWritable.class);

			FlinkHadoopJobClient.runJob(conf);
		}

		public static class ValueConfMapper extends MapReduceBase implements Mapper {

			private int value;

			@Override
			public void configure(final JobConf conf) {
				this.value = conf.getInt("test.value", 0);
			}

			@Override
			@SuppressWarnings("unchecked")
			public void map(final Object o, final Object o2, final OutputCollector outputCollector, final Reporter reporter) throws IOException {
				outputCollector.collect(o2, new LongWritable(this.value));
			}
		}
	}

	public static class WordCountDifferentReducerTypes {

		public static void main(String[] args) throws Exception {
			final String inputPath = args[0];
			final String outputPath = args[1];

			final JobConf conf = new JobConf();

			conf.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class);
			org.apache.hadoop.mapred.TextInputFormat.addInputPath(conf, new Path(inputPath));

			conf.setOutputFormat(TextOutputFormat.class);
			TextOutputFormat.setOutputPath(conf, new Path(outputPath));

			conf.setMapperClass(TestTokenizeMap.class);
			conf.setReducerClass(HashCodeReducer.class);
			conf.setCombinerClass((LongSumReducer.class));

			conf.set("mapred.textoutputformat.separator", " ");

			conf.setMapOutputKeyClass(Text.class);
			conf.setMapOutputValueClass(LongWritable.class);

			conf.setOutputKeyClass(IntWritable.class);
			conf.setOutputValueClass(LongWritable.class);

			FlinkHadoopJobClient.runJob(conf);
		}


		public static class HashCodeReducer extends MapReduceBase implements Reducer<Text, LongWritable, IntWritable, LongWritable> {

			@Override
			public void reduce(final Text text, final Iterator<LongWritable> iterator,
			                   final OutputCollector<IntWritable, LongWritable> collector,
			                   final Reporter reporter) throws IOException {
				long sum = 0;
				while (iterator.hasNext()) {
					sum += iterator.next().get();
				}
				collector.collect(new IntWritable(text.toString().hashCode()),new LongWritable(sum));
			}
		}
	}

	public static class MultipleInputsWordCount {
		public static void main(String[] args) throws Exception{
			final String inputPath1 = args[0];
			final String inputPath2 = args[1];
			final String outputPath = args[2];

			final JobConf conf = new JobConf();

			MultipleInputs.addInputPath(conf, new Path(inputPath1), TextInputFormat.class);
			MultipleInputs.addInputPath(conf, new Path(inputPath2), TextInputFormat.class);

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
	}

	public static class WordCountMapperOnly {
		public static void main(String[] args) throws Exception {
			final String inputPath = args[0];
			final String outputPath = args[1];

			final JobConf conf = new JobConf();

			conf.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class);
			org.apache.hadoop.mapred.TextInputFormat.addInputPath(conf, new Path(inputPath));

			conf.setOutputFormat(TextOutputFormat.class);
			TextOutputFormat.setOutputPath(conf, new Path(outputPath));

			conf.setMapperClass(TestTokenizeMap.class);
			conf.setNumReduceTasks(0);

			conf.set("mapred.textoutputformat.separator", " ");
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(LongWritable.class);

			FlinkHadoopJobClient.runJob(conf);
		}
	}

}
