/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.stormcompatibility.wrappers;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.streaming.runtime.tasks.StreamingRuntimeContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(StormWrapperSetupHelper.class)
public class FiniteStormSpoutWrapperTest {

	@SuppressWarnings("unchecked")
	@Test
	public void runAndExecuteTest1() throws Exception {
		final FiniteStormSpout stormSpout = mock(FiniteStormSpout.class);
		when(stormSpout.reachedEnd()).thenReturn(false, false, false, true, false, false, true);

		final StreamingRuntimeContext taskContext = mock(StreamingRuntimeContext.class);
		when(taskContext.getExecutionConfig()).thenReturn(new ExecutionConfig());

		final FiniteStormSpoutWrapper<?> wrapper = new FiniteStormSpoutWrapper<Object>(stormSpout);
		wrapper.setRuntimeContext(taskContext);

		wrapper.run(mock(SourceContext.class));
		verify(stormSpout, times(3)).nextTuple();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void runAndExecuteTest2() throws Exception {
		final FiniteStormSpout stormSpout = mock(FiniteStormSpout.class);
		when(stormSpout.reachedEnd()).thenReturn(true, false, true, false, true, false, true);

		final StreamingRuntimeContext taskContext = mock(StreamingRuntimeContext.class);
		when(taskContext.getExecutionConfig()).thenReturn(new ExecutionConfig());

		final FiniteStormSpoutWrapper<?> wrapper = new FiniteStormSpoutWrapper<Object>(stormSpout);
		wrapper.setRuntimeContext(taskContext);

		wrapper.run(mock(SourceContext.class));
		verify(stormSpout, never()).nextTuple();
	}

}
