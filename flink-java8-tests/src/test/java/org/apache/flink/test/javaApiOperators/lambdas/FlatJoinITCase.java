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
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;

@SuppressWarnings("serial")
public class FlatJoinITCase implements Serializable {

	@Test
	public void testFlatJoinLambda() {
		try {
			final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

			DataSet<Tuple2<Integer, String>> left = env.fromElements(
					new Tuple2<Integer, String>(1, "hello"),
					new Tuple2<Integer, String>(2, "what's"),
					new Tuple2<Integer, String>(2, "up")
			);
			DataSet<Tuple2<Integer, String>> right = env.fromElements(
					new Tuple2<Integer, String>(1, "not"),
					new Tuple2<Integer, String>(1, "much"),
					new Tuple2<Integer, String>(2, "really")
			);
			DataSet<Tuple2<Integer,String>> joined = left.join(right).where(0).equalTo(0)
					.with((t,s,out) -> out.collect(new Tuple2<Integer,String>(t.f0, t.f1 + " " + s.f1)));
		} catch (UnsupportedLambdaExpressionException e) {
			// Success
			return;
		} catch (Exception e) {
			Assert.fail();
		}
	}
}


