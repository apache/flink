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

package org.apache.flink.streaming.api.operators.windowing.functions;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.operators.OutputTypeConfigurable;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableAllWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalSingleValueAllWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalSingleValueWindowFunction;
import org.apache.flink.util.Collector;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class InternalWindowFunctionTest {

	@SuppressWarnings("unchecked")
	@Test
	public void testInternalIterableAllWindowFunction() throws Exception {

		AllWindowFunctionMock mock = mock(AllWindowFunctionMock.class);
		InternalIterableAllWindowFunction<Long, String, TimeWindow> windowFunction =
			new InternalIterableAllWindowFunction<>(mock);

		// check setOutputType
		TypeInformation<String> stringType = BasicTypeInfo.STRING_TYPE_INFO;
		ExecutionConfig execConf = new ExecutionConfig();
		execConf.setParallelism(42);

		windowFunction.setOutputType(stringType, execConf);
		verify(mock).setOutputType(stringType, execConf);

		// check open
		Configuration config = new Configuration();

		windowFunction.open(config);
		verify(mock).open(config);

		// check setRuntimeContext
		RuntimeContext rCtx = mock(RuntimeContext.class);

		windowFunction.setRuntimeContext(rCtx);
		verify(mock).setRuntimeContext(rCtx);

		// check apply
		TimeWindow w = mock(TimeWindow.class);
		Iterable<Long> i = (Iterable<Long>)mock(Iterable.class);
		Collector<String> c = (Collector<String>) mock(Collector.class);

		windowFunction.apply(((byte)0), w, i, c);
		verify(mock).apply(w, i, c);

		// check close
		windowFunction.close();
		verify(mock).close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testInternalIterableWindowFunction() throws Exception {

		WindowFunctionMock mock = mock(WindowFunctionMock.class);
		InternalIterableWindowFunction<Long, String, Long, TimeWindow> windowFunction =
			new InternalIterableWindowFunction<>(mock);

		// check setOutputType
		TypeInformation<String> stringType = BasicTypeInfo.STRING_TYPE_INFO;
		ExecutionConfig execConf = new ExecutionConfig();
		execConf.setParallelism(42);

		windowFunction.setOutputType(stringType, execConf);
		verify(mock).setOutputType(stringType, execConf);

		// check open
		Configuration config = new Configuration();

		windowFunction.open(config);
		verify(mock).open(config);

		// check setRuntimeContext
		RuntimeContext rCtx = mock(RuntimeContext.class);

		windowFunction.setRuntimeContext(rCtx);
		verify(mock).setRuntimeContext(rCtx);

		// check apply
		TimeWindow w = mock(TimeWindow.class);
		Iterable<Long> i = (Iterable<Long>)mock(Iterable.class);
		Collector<String> c = (Collector<String>) mock(Collector.class);

		windowFunction.apply(42L, w, i, c);
		verify(mock).apply(42L, w, i, c);

		// check close
		windowFunction.close();
		verify(mock).close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testInternalSingleValueWindowFunction() throws Exception {

		WindowFunctionMock mock = mock(WindowFunctionMock.class);
		InternalSingleValueWindowFunction<Long, String, Long, TimeWindow> windowFunction =
			new InternalSingleValueWindowFunction<>(mock);

		// check setOutputType
		TypeInformation<String> stringType = BasicTypeInfo.STRING_TYPE_INFO;
		ExecutionConfig execConf = new ExecutionConfig();
		execConf.setParallelism(42);

		windowFunction.setOutputType(stringType, execConf);
		verify(mock).setOutputType(stringType, execConf);

		// check open
		Configuration config = new Configuration();

		windowFunction.open(config);
		verify(mock).open(config);

		// check setRuntimeContext
		RuntimeContext rCtx = mock(RuntimeContext.class);

		windowFunction.setRuntimeContext(rCtx);
		verify(mock).setRuntimeContext(rCtx);

		// check apply
		TimeWindow w = mock(TimeWindow.class);
		Collector<String> c = (Collector<String>) mock(Collector.class);

		windowFunction.apply(42L, w, 23L, c);
		verify(mock).apply(eq(42L), eq(w), (Iterable<Long>)argThat(IsIterableContainingInOrder.contains(23L)), eq(c));

		// check close
		windowFunction.close();
		verify(mock).close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testInternalSingleValueAllWindowFunction() throws Exception {

		AllWindowFunctionMock mock = mock(AllWindowFunctionMock.class);
		InternalSingleValueAllWindowFunction<Long, String, TimeWindow> windowFunction =
			new InternalSingleValueAllWindowFunction<>(mock);

		// check setOutputType
		TypeInformation<String> stringType = BasicTypeInfo.STRING_TYPE_INFO;
		ExecutionConfig execConf = new ExecutionConfig();
		execConf.setParallelism(42);

		windowFunction.setOutputType(stringType, execConf);
		verify(mock).setOutputType(stringType, execConf);

		// check open
		Configuration config = new Configuration();

		windowFunction.open(config);
		verify(mock).open(config);

		// check setRuntimeContext
		RuntimeContext rCtx = mock(RuntimeContext.class);

		windowFunction.setRuntimeContext(rCtx);
		verify(mock).setRuntimeContext(rCtx);

		// check apply
		TimeWindow w = mock(TimeWindow.class);
		Collector<String> c = (Collector<String>) mock(Collector.class);

		windowFunction.apply(((byte)0), w, 23L, c);
		verify(mock).apply(eq(w), (Iterable<Long>)argThat(IsIterableContainingInOrder.contains(23L)), eq(c));

		// check close
		windowFunction.close();
		verify(mock).close();
	}

	public static class WindowFunctionMock
		extends RichWindowFunction<Long, String, Long, TimeWindow>
		implements OutputTypeConfigurable<String> {

		@Override
		public void setOutputType(TypeInformation<String> outTypeInfo, ExecutionConfig executionConfig) { }

		@Override
		public void apply(Long aLong, TimeWindow window, Iterable<Long> input, Collector<String> out) throws Exception { }
	}

	public static class AllWindowFunctionMock
		extends RichAllWindowFunction<Long, String, TimeWindow>
		implements OutputTypeConfigurable<String> {

		@Override
		public void setOutputType(TypeInformation<String> outTypeInfo, ExecutionConfig executionConfig) { }

		@Override
		public void apply(TimeWindow window, Iterable<Long> values, Collector<String> out) throws Exception { }
	}
}
