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

package org.apache.flink.api.java.operator;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.Operator;
import org.apache.flink.api.java.typeutils.ValueTypeInfo;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class OperatorTest {

	@Test
	public void testConfigurationOfParallelism() {
		Operator operator = new MockOperator();

		// verify explicit change in parallelism
		int parallelism = 36;
		operator.setParallelism(parallelism);

		assertEquals(parallelism, operator.getParallelism());

		// verify that parallelism is reset to default flag value
		parallelism = ExecutionConfig.PARALLELISM_DEFAULT;
		operator.setParallelism(parallelism);

		assertEquals(parallelism, operator.getParallelism());
	}

	private class MockOperator extends Operator {
		public MockOperator() {
			super(ExecutionEnvironment.createCollectionsEnvironment(), ValueTypeInfo.NULL_VALUE_TYPE_INFO);
		}
	}
}
