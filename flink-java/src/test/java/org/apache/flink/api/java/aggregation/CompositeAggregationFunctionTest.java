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

package org.apache.flink.api.java.aggregation;

import static org.apache.flink.util.TestHelper.uniqueInt;
import static org.apache.flink.util.TestHelper.uniqueString;

import java.util.List;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple;
import org.junit.Before;
import org.junit.Test;

public class CompositeAggregationFunctionTest {

	@SuppressWarnings("serial")
	private static class MockCompositeAggregationFunction extends CompositeAggregationFunction<Object, Object> {

		public MockCompositeAggregationFunction(String name, int field) {
			super(name, field);
		}
		
		@Override
		public List<AggregationFunction<?, ?>> getIntermediates() {
			return null;
		}

		@Override
		public Object computeComposite(Tuple tuple) {
			return null;
		}

		@Override
		public AggregationFunction.ResultTypeBehavior getResultTypeBehavior() {
			return null;
		}

		@Override
		public BasicTypeInfo<Object> getResultType() {
			return null;
		}

		@Override
		public void setInputType(BasicTypeInfo<Object> inputType) {

		}

	}
	
	private CompositeAggregationFunction<Object, Object> composite;
	
	@Before
	public void setup() {
		int field = uniqueInt();
		String name = uniqueString();
		composite = new MockCompositeAggregationFunction(name, field);
	}
	
	@Test(expected=IllegalStateException.class)
	public void shouldNotCallInitialize() {
		// when
		composite.initializeIntermediate(null);
	}
	
	@Test(expected=IllegalStateException.class)
	public void shouldNotCallReduce() {
		// when
		composite.reduce(null, null);
	}
	
}
