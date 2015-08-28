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
import org.apache.flink.client.LocalExecutor;
import org.apache.flink.test.recordJobs.wordcount.WordCount;
import org.apache.flink.test.testdata.WordCountData;
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
			
			// run WordCount
			WordCount wc = new WordCount();

			LocalExecutor executor = new LocalExecutor();
			executor.setDefaultOverwriteFiles(true);
			executor.setTaskManagerNumSlots(parallelism);
			executor.setPrintStatusDuringExecution(false);
			executor.start();
			Plan wcPlan = wc.getPlan(Integer.valueOf(parallelism).toString(), inFile.toURI().toString(),outFile.toURI().toString());
			wcPlan.setExecutionConfig(new ExecutionConfig());
			executor.executePlan(wcPlan);
			executor.stop();
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
}
