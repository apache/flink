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

package org.apache.flink.test.javaApiOperators.lambdas;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.UnsupportedLambdaExpressionException;
import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;

@SuppressWarnings("serial")
public class FlatMapITCase implements Serializable {

	@Test
	public void testFlatMapLambda() {
		try {
			final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

			DataSet<String> stringDs = env.fromElements("aa", "ab", "ac", "ad");
			DataSet<String> flatMappedDs = stringDs.flatMap((s, out) -> out.collect(s.replace("a", "b")));
			env.execute();
		} catch (UnsupportedLambdaExpressionException e) {
			// Success
			return;
		} catch (Exception e) {
			Assert.fail();
		}
	}
}
