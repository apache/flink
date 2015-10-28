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

package org.apache.flink.test.clients.examples;

import java.io.File;
import java.io.FileWriter;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.LocalExecutor;
import org.apache.flink.test.testdata.WordCountData;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Test;


public class LocalExecutorITCase {

	private static final int parallelism = 4;

	@Test
	public void testLocalExecutorWithWordCount() {
		try {
			// set up the files
			File inFile = File.createTempFile("wctext", ".in");
			File outFile = File.createTempFile("wctext", ".out");
			inFile.deleteOnExit();
			outFile.deleteOnExit();
			
			FileWriter fw = new FileWriter(inFile);
			fw.write(WordCountData.TEXT);
			fw.close();

			LocalExecutor executor = new LocalExecutor();
			executor.setDefaultOverwriteFiles(true);
			executor.setTaskManagerNumSlots(parallelism);
			executor.setPrintStatusDuringExecution(false);
			executor.start();
			Plan wcPlan = getWordCountPlan(inFile, outFile, parallelism);
			wcPlan.setExecutionConfig(new ExecutionConfig());
			executor.executePlan(wcPlan);
			executor.stop();
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	
	private Plan getWordCountPlan(File inFile, File outFile, int parallelism) {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);
		env.readTextFile(inFile.getAbsolutePath())
				.flatMap(new Tokenizer())
				.groupBy(0)
				.sum(1)
				.writeAsCsv(outFile.getAbsolutePath());
		return env.createProgramPlan();
	}

	public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			String[] tokens = value.toLowerCase().split("\\W+");
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<>(token, 1));
				}
			}
		}
	}
}
