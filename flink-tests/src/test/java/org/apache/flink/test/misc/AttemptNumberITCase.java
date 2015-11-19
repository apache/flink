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

package org.apache.flink.test.misc;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.junit.Assert;

public class AttemptNumberITCase extends JavaProgramTestBase {

	private static int[] attemptCount;

	@Override
	protected boolean skipCollectionExecution() {
		return true;
	}

	@Override
	protected void preSubmit() {
		attemptCount = new int[2];
	}

	@Override
	protected void testProgram() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setNumberOfExecutionRetries(1);
		env.getConfig().setExecutionRetryDelay(0);
		env.fromElements(1).map(new RichMapFunction<Integer, Integer>() {
			@Override
			public void open(Configuration parameters) throws Exception {
				int attempt = getRuntimeContext().getAttemptNumber();
				attemptCount[attempt]++;
				if (attempt == 0) {
					throw new Exception();
				}
			}

			@Override
			public Integer map(Integer value) throws Exception {
				return value;
			}
		}).output(new DiscardingOutputFormat<Integer>());
		env.execute();
	}

	@Override
	protected void postSubmit() {
		Assert.assertEquals(1, attemptCount[0]);
		Assert.assertEquals(1, attemptCount[1]);
	}
}
