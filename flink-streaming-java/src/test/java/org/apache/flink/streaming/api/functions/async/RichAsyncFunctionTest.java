/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.async;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.async.collector.AsyncCollector;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test case for {@link RichAsyncFunction}
 */
public class RichAsyncFunctionTest {

	private RichAsyncFunction<String, String> initFunction() {
		RichAsyncFunction<String, String> function = new RichAsyncFunction<String, String>() {
			@Override
			public void asyncInvoke(String input, AsyncCollector<String> collector) throws Exception {
				getRuntimeContext().getState(mock(ValueStateDescriptor.class));
			}
		};

		return function;
	}

	@Test
	public void testIterationRuntimeContext() throws Exception {
		// test runtime context is not set
		RichAsyncFunction<String, String> function = new RichAsyncFunction<String, String>() {
			@Override
			public void asyncInvoke(String input, AsyncCollector<String> collector) throws Exception {
				getIterationRuntimeContext().getIterationAggregator("test");
			}
		};

		try {
			function.asyncInvoke("test", mock(AsyncCollector.class));
		}
		catch (Exception e) {
			Assert.assertEquals("The runtime context has not been initialized.", e.getMessage());
		}

		// test get agg from iteration runtime context
		function.setRuntimeContext(mock(IterationRuntimeContext.class));

		try {
			function.asyncInvoke("test", mock(AsyncCollector.class));
		}
		catch (Exception e) {
			Assert.assertEquals("Get iteration aggregator is not supported in rich async function", e.getMessage());
		}

		// get state from iteration runtime context
		function = new RichAsyncFunction<String, String>() {
			@Override
			public void asyncInvoke(String input, AsyncCollector<String> collector) throws Exception {
				getIterationRuntimeContext().getState(mock(ValueStateDescriptor.class));
			}
		};

		function.setRuntimeContext(mock(RuntimeContext.class));

		try {
			function.asyncInvoke("test", mock(AsyncCollector.class));
		}
		catch (Exception e) {
			Assert.assertEquals("State is not supported in rich async function", e.getMessage());
		}

		// test getting a counter from iteration runtime context
		function = new RichAsyncFunction<String, String>() {
			@Override
			public void asyncInvoke(String input, AsyncCollector<String> collector) throws Exception {
				getIterationRuntimeContext().getIntCounter("test").add(6);
			}
		};

		IterationRuntimeContext context = mock(IterationRuntimeContext.class);
		IntCounter counter = new IntCounter(0);
		when(context.getIntCounter(anyString())).thenReturn(counter);

		function.setRuntimeContext(context);

		function.asyncInvoke("test", mock(AsyncCollector.class));

		Assert.assertTrue(6 == counter.getLocalValue());
	}

	@Test
	public void testRuntimeContext() throws Exception {
		// test run time context is not set
		RichAsyncFunction<String, String> function = new RichAsyncFunction<String, String>() {
			@Override
			public void asyncInvoke(String input, AsyncCollector<String> collector) throws Exception {
				getRuntimeContext().getState(mock(ValueStateDescriptor.class));
			}
		};

		try {
			function.asyncInvoke("test", mock(AsyncCollector.class));
		}
		catch (Exception e) {
			Assert.assertEquals("The runtime context has not been initialized.", e.getMessage());
		}

		// test get state
		function = new RichAsyncFunction<String, String>() {
			@Override
			public void asyncInvoke(String input, AsyncCollector<String> collector) throws Exception {
				getRuntimeContext().getState(mock(ValueStateDescriptor.class));
			}
		};

		function.setRuntimeContext(mock(RuntimeContext.class));

		try {
			function.asyncInvoke("test", mock(AsyncCollector.class));
		}
		catch (Exception e) {
			Assert.assertEquals("State is not supported in rich async function", e.getMessage());
		}

		// test getting a counter from runtime context
		function = new RichAsyncFunction<String, String>() {
			@Override
			public void asyncInvoke(String input, AsyncCollector<String> collector) throws Exception {
				getIterationRuntimeContext().getIntCounter("test").add(6);
			}
		};

		IterationRuntimeContext context = mock(IterationRuntimeContext.class);
		IntCounter counter = new IntCounter(0);
		when(context.getIntCounter(anyString())).thenReturn(counter);

		function.setRuntimeContext(context);

		function.asyncInvoke("test", mock(AsyncCollector.class));

		Assert.assertTrue(6 == counter.getLocalValue());
	}
}
