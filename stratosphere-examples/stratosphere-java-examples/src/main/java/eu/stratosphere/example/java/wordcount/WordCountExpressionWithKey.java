package eu.stratosphere.example.java.wordcount;
/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
//package eu.stratosphere.example.java.wordcount;
//
//import java.util.Iterator;
//
//import eu.stratosphere.api.java.DataSet;
//import eu.stratosphere.api.java.ExecutionEnvironment;
//import eu.stratosphere.api.java.functions.FlatMapFunction;
//import eu.stratosphere.api.java.functions.KeyExtractor;
//import eu.stratosphere.api.java.functions.ReduceFunction;
//import eu.stratosphere.api.java.functions.ReduceWithKeyFunction;
//import eu.stratosphere.api.java.tuple.*;
//import eu.stratosphere.core.fs.Path;
//import eu.stratosphere.example.java.wordcount.WordCount2.Tokenizer;
//import eu.stratosphere.example.java.wordcount.WordCount2.WC;
//import eu.stratosphere.util.Collector;
//
//import static eu.stratosphere.api.java.aggregation.Aggregations.*;
//
//
//public class WordCount4 {
//	
//	public static class WC {
//		
//		public String word;
//		public int count;
//		
//		public WC() {}
//		
//		public WC(String word, int count) {
//			this.word = word;
//			this.count = count;
//		}
//		
//		@Override
//		public String toString() {
//			return "(" + word + ", " + count + ")";
//		}
//	}
//
//	public static final class MyReducer extends ReduceWithKeyFunction<String, Integer, Tuple2<String, Integer>> {
//
//
//		@Override
//		public void reduce(String Key, Iterator<Integer> values, Collector<Tuple2<String, Integer>> out) {
//			// TODO Auto-generated method stub
//			
//		}
//		
//	}
//	
//	@SuppressWarnings("serial")
//	public static void main(String[] args) {
//		if (args.length < 2) {
//			System.out.println("Usage: <input path> <output path>");
//			return;
//		}
//		
//		final String inputPath = args[0];
//		final String outputPath = args[1];
//		
//		final ExecutionEnvironment context = ExecutionEnvironment.getExecutionEnvironment();
//		
//		DataSet<String> text = context.readTextFile(inputPath);
//		
//		DataSet<WC> tokenized = text.flatMap((value, collector) -> {
//			String[] tokens = value.toLowerCase().split("\\W");
//			for (String token : tokens) {
//				out.collect(new WC(token, 1));
//			}
//		});
//		
//		DataSet<WC> result = tokenized.reduceWithKey("word", "count", new MyReducer());
//		
//		result.writeAsText(new Path(outputPath));
//	}
//}
