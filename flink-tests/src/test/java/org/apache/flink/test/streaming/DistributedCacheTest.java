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

package org.apache.flink.test.streaming;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.StreamingProgramTestBase;
import org.apache.flink.util.Collector;

import java.io.*;

import java.util.*;


public class DistributedCacheTest extends StreamingProgramTestBase {
	public static final String data
		= "machen\n"
		+ "zeit\n"
		+ "heerscharen\n"
		+ "keiner\n"
		+ "meine\n";

	protected String textPath;

	@Override
	protected void preSubmit() throws Exception {
		textPath = createTempFile("count.txt", data);
	}

	@Override
	protected void testProgram() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.registerCachedFile(textPath, "cache_test");
		DataStream<Tuple1<String>> dataStream = env
			.readTextFile(textPath)
			.flatMap(new org.apache.flink.test.distributedCache.DistributedCacheTest.WordChecker());
		Iterator<Tuple1<String>> items = DataStreamUtils.collect(dataStream);

		List<String> result = new ArrayList<>();
		while (items.hasNext()){
			result.add(items.next().f0);
		}

		compareResultAsText(result, data);
	}

	public static class WordChecker extends RichFlatMapFunction<String, Tuple1<String>> {
		private static final long serialVersionUID = 1L;

		private final Set<String> wordList = new HashSet<>();

		@Override
		public void open(Configuration conf) throws FileNotFoundException, IOException {
			File file = getRuntimeContext().getDistributedCache().getFile("cache_test");
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String tempString;
			while ((tempString = reader.readLine()) != null) {
				wordList.add(tempString);
			}
			reader.close();
		}

		@Override
		public void flatMap(String word, Collector<Tuple1<String>> out) throws Exception {
			if (wordList.contains(word)) {
				out.collect(new Tuple1<>(word));
			}
		}
	}

}
