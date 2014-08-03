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

package org.apache.flink.api.java.operators.translation;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.operators.base.GroupReduceOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DistinctOperator;
import org.junit.Assert;
import org.junit.Test;

@SuppressWarnings("serial")
public class DistrinctTranslationTest {

	@Test
	public void testCombinable() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			
			DataSet<String> input = env.fromElements("1", "2", "1", "3");
			
			
			DistinctOperator<String> op = input.distinct(new KeySelector<String, String>() {
				public String getKey(String value) { return value; }
			});
			
			op.print();
			
			Plan p = env.createProgramPlan();
			
			GroupReduceOperatorBase<?, ?, ?> reduceOp = (GroupReduceOperatorBase<?, ?, ?>) p.getDataSinks().iterator().next().getInput();
			Assert.assertTrue(reduceOp.isCombinable());
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
}
