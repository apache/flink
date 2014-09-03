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

import static org.junit.Assert.*;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.java.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;

@SuppressWarnings("serial")
public class LambdaExtractionTest {

	@Test
	public void testIdentifyLambdas() {
		try {
			MapFunction<?, ?> anonymousFromInterface = new MapFunction<String, Integer>() {
				@Override
				public Integer map(String value) { return Integer.parseInt(value); }
			};
			
			MapFunction<?, ?> anonymousFromClass = new RichMapFunction<String, Integer>() {
				@Override
				public Integer map(String value) { return Integer.parseInt(value); }
			};
			
			MapFunction<?, ?> fromProperClass = new StaticMapper();
			
			MapFunction<?, ?> fromDerived = new ToTuple<Integer>() {
				@Override
				public Tuple2<Integer, Long> map(Integer value) {
					return new Tuple2<Integer, Long>(value, 1L);
				}
			};
			
			MapFunction<String, Integer> lambda = (str) -> Integer.parseInt(str);
			
			assertFalse(FunctionUtils.isLambdaFunction(anonymousFromInterface));
			assertFalse(FunctionUtils.isLambdaFunction(anonymousFromClass));
			assertFalse(FunctionUtils.isLambdaFunction(fromProperClass));
			assertFalse(FunctionUtils.isLambdaFunction(fromDerived));
			assertTrue(FunctionUtils.isLambdaFunction(lambda));
			assertTrue(FunctionUtils.isLambdaFunction(STATIC_LAMBDA));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	public static class StaticMapper implements MapFunction<String, Integer> {

		@Override
		public Integer map(String value) { return Integer.parseInt(value); }
	}
	
	public interface ToTuple<T> extends MapFunction<T, Tuple2<T, Long>> {

		@Override
		public Tuple2<T, Long> map(T value) throws Exception;
	}
	
	private static final MapFunction<String, Integer> STATIC_LAMBDA = (str) -> Integer.parseInt(str);
}
