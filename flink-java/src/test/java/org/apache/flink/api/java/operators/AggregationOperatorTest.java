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

package org.apache.flink.api.java.operators;

import static org.apache.flink.util.TestHelper.uniqueInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import org.apache.flink.api.common.operators.base.MapOperatorBase;
import org.apache.flink.api.common.operators.base.ReduceOperatorBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.AggregationFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;


@RunWith(PowerMockRunner.class)
@PrepareForTest({ AggregationOperator.class })
@SuppressWarnings({"rawtypes", "unchecked"})
public class AggregationOperatorTest {

	private AggregationOperator op;
	
	@Before
	public void setup() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Tuple1<Long> data = new Tuple1<Long>(1L);
		DataSet input = env.fromElements(data);
		TypeInformation type = input.getType();
		AggregationFunction[] functions = { mock(AggregationFunction.class) };
		op = new AggregationOperator(input, type, type, new int[0], functions, functions);
	}
	
	@Test
	public void shouldSetParallelism() throws Exception {
		// given
		int dop = uniqueInt();
		org.apache.flink.api.common.operators.Operator input = mock(org.apache.flink.api.common.operators.Operator.class);
		MapOperatorBase map1 = mock(MapOperatorBase.class);
		MapOperatorBase map2 = mock(MapOperatorBase.class);
		ReduceOperatorBase reduce = mock(ReduceOperatorBase.class);
		whenNew(MapOperatorBase.class).withAnyArguments().thenReturn(map1, map2);
		whenNew(ReduceOperatorBase.class).withAnyArguments().thenReturn(reduce);
		op.setParallelism(dop);
		
		// when
		op.translateToDataFlow(input);

		// then
		verify(map1).setDegreeOfParallelism(dop);
		verify(reduce).setDegreeOfParallelism(dop);
		verify(map2).setDegreeOfParallelism(dop);
	}
	
}
