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

package org.apache.flink.api.java.lambdas;

import junit.framework.Assert;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.junit.Test;

public class LambdaFunctionsTest {
	private static String TEXT = "Aaaa\n" +
			"Baaa\n" +
			"Caaa\n" +
			"Daaa\n" +
			"Eaaa\n" +
			"Faaa\n" +
			"Gaaa\n" +
			"Haaa\n" +
			"Hbbb\n";

	@Test
	public void testFilterLambda () {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<String> text = env.fromElements(TEXT);

		try {
			DataSet<String> result = text
					.filter(s -> s.startsWith("H"));
			result.print();
			Assert.assertNotNull(result);
			DataSet<String> result2 = text
					.filter(s -> s.startsWith("I"));
		}
		catch (Exception e) {
			Assert.fail();
		}

	}

	@Test
	public void testReduceLambda () {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<String> text = env.fromElements(TEXT);

		try {
			DataSet<String> result = text
					.reduce ((s, t) -> s.concat(t).substring(0,t.length()));
			result.print();

		}
		catch (Exception e) {
			Assert.fail();
		}

	}
}
