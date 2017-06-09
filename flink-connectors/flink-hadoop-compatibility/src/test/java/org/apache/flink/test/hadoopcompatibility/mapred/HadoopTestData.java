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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Test data.
 */
public class HadoopTestData {

	public static DataSet<Tuple2<IntWritable, Text>> getKVPairDataSet(ExecutionEnvironment env) {

		List<Tuple2<IntWritable, Text>> data = new ArrayList<Tuple2<IntWritable, Text>>();
		data.add(new Tuple2<IntWritable, Text>(new IntWritable(1), new Text("Hi")));
		data.add(new Tuple2<IntWritable, Text>(new IntWritable(2), new Text("Hello")));
		data.add(new Tuple2<IntWritable, Text>(new IntWritable(3), new Text("Hello world")));
		data.add(new Tuple2<IntWritable, Text>(new IntWritable(4), new Text("Hello world, how are you?")));
		data.add(new Tuple2<IntWritable, Text>(new IntWritable(5), new Text("I am fine.")));
		data.add(new Tuple2<IntWritable, Text>(new IntWritable(6), new Text("Luke Skywalker")));
		data.add(new Tuple2<IntWritable, Text>(new IntWritable(7), new Text("Comment#1")));
		data.add(new Tuple2<IntWritable, Text>(new IntWritable(8), new Text("Comment#2")));
		data.add(new Tuple2<IntWritable, Text>(new IntWritable(9), new Text("Comment#3")));
		data.add(new Tuple2<IntWritable, Text>(new IntWritable(10), new Text("Comment#4")));
		data.add(new Tuple2<IntWritable, Text>(new IntWritable(11), new Text("Comment#5")));
		data.add(new Tuple2<IntWritable, Text>(new IntWritable(12), new Text("Comment#6")));
		data.add(new Tuple2<IntWritable, Text>(new IntWritable(13), new Text("Comment#7")));
		data.add(new Tuple2<IntWritable, Text>(new IntWritable(14), new Text("Comment#8")));
		data.add(new Tuple2<IntWritable, Text>(new IntWritable(15), new Text("Comment#9")));
		data.add(new Tuple2<IntWritable, Text>(new IntWritable(16), new Text("Comment#10")));
		data.add(new Tuple2<IntWritable, Text>(new IntWritable(17), new Text("Comment#11")));
		data.add(new Tuple2<IntWritable, Text>(new IntWritable(18), new Text("Comment#12")));
		data.add(new Tuple2<IntWritable, Text>(new IntWritable(19), new Text("Comment#13")));
		data.add(new Tuple2<IntWritable, Text>(new IntWritable(20), new Text("Comment#14")));
		data.add(new Tuple2<IntWritable, Text>(new IntWritable(21), new Text("Comment#15")));

		Collections.shuffle(data);

		return env.fromCollection(data);
	}
}
