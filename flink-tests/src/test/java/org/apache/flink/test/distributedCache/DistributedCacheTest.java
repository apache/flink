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
package org.apache.flink.test.distributedCache;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.util.Collector;

/**
 * Tests the distributed cache by comparing a text file with a distributed copy.
 */
public class DistributedCacheTest extends JavaProgramTestBase {

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
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.registerCachedFile(textPath, "cache_test");

		List<Tuple1<String>> result = env
				.readTextFile(textPath)
				.flatMap(new WordChecker())
				.collect();

		compareResultAsTuples(result, data);
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
