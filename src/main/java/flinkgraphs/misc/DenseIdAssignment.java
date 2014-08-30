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

package flinkgraphs.misc;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.functions.RichMapFunction;
import org.apache.flink.api.java.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class DenseIdAssignment implements java.io.Serializable {
	
	
	private static final String TEST_DATA_PATH = "/data/demodata/strings/strings.txt";
	
	
	public static void main(String[] args) throws Exception {
		
		LocalEnvironment env = ExecutionEnvironment.createLocalEnvironment(4);
		env.enableLogging();
		
		DataSet<Tuple1<String>> strings = env.readCsvFile(TEST_DATA_PATH).types(String.class);
		
		// make strings unique (optional)
		DataSet<String> uniqueStrings = strings
//									.groupBy(0)
//									.reduce(new ReduceFunction<Tuple1<String>>() {
//										public Tuple1<String> reduce(Tuple1<String> a, Tuple1<String> b) {
//											return a;
//										}
//									})
									.map(new MapFunction<Tuple1<String>, String>() {
										@Override
										public String map(Tuple1<String> value) {
											return value.f0;
										}
									});
		
		// count the elements in each partition
		DataSet<Tuple2<Integer, Long>> partitionCounts = uniqueStrings.mapPartition(new PerPartitionCounter());
		
		DataSet<Tuple2<String, Long>> result = uniqueStrings.map(new IdAssigner()).withBroadcastSet(partitionCounts, "COUNTERS");
				
		result.print();
		
		env.execute();
	}
	
	/**
	 * Counts the number of elements per partition and returns a (partition-num, count) tuple.
	 */
	public static final class PerPartitionCounter extends RichMapPartitionFunction<String, Tuple2<Integer, Long>> {
		
		@Override
		public void mapPartition(Iterable<String> values, Collector<Tuple2<Integer, Long>> out) {
			long count = 0;
			for (@SuppressWarnings("unused") String str : values) {
				count++;
			}
			
			int partition = getRuntimeContext().getIndexOfThisSubtask();
			out.collect(new Tuple2<Integer, Long>(partition, count));
		}
	}
	
	public static final class IdAssigner extends RichMapFunction<String, Tuple2<String, Long>> {

		private long id;
		
		@Override
		public void open(Configuration parameters) {
			@SuppressWarnings("unchecked")
			List<Tuple2<Integer, Long>> counters = (List<Tuple2<Integer, Long>>) (List<?>) getRuntimeContext().getBroadcastVariable("COUNTERS");
			
			Collections.sort(counters, new Comparator<Tuple2<Integer, Long>>() {
				public int compare(Tuple2<Integer, Long> o1, Tuple2<Integer, Long> o2) {
					return o1.f0.compareTo(o2.f0);
				}
			});
			
			int ourPartition = getRuntimeContext().getIndexOfThisSubtask();
			
			// sum up all counts from partitions before us
			for (int i = 0; i < ourPartition; i++) {
				id += counters.get(i).f1;
			}
		}
		
		@Override
		public Tuple2<String, Long> map(String value) {
			return new Tuple2<String, Long>(value, id++);
		}
	}
	
	
	// --------------------------------------------------------------------------------------------
	//  Some test data
	// --------------------------------------------------------------------------------------------
	
//	public static void main(String[] args) throws Exception {
//		writeRandomStrings(TEST_DATA_PATH, 100000000);
//	}
	
	protected static void writeRandomStrings(String path, int num) throws IOException {
		File f = new File(path);
		Random rnd = new Random();
		
		try (FileWriter writer = new FileWriter(f)) {
			for (int i = 0; i < num; i++) {
				writer.write(getRandomLine(rnd));
			}
		}
	}
	
	private static String getRandomLine(Random rnd) {
		StringBuilder bld = new StringBuilder(21);
		for (int i = 0; i < 20; i++) {
			bld.append((char) (rnd.nextInt('Z' - 'A') + 1 + 'A'));
		}
		bld.append('\n');
		return bld.toString();
	}
}
