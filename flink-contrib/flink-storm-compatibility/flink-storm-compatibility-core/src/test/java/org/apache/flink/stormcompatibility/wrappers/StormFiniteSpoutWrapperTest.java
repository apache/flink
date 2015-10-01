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

import backtype.storm.topology.IRichSpout;
import backtype.storm.tuple.Fields;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.stormcompatibility.util.AbstractTest;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.streaming.runtime.tasks.StreamingRuntimeContext;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.LinkedList;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(StormWrapperSetupHelper.class)
public class StormFiniteSpoutWrapperTest extends AbstractTest {

	@SuppressWarnings("unchecked")
	@Test
	public void testRunExecuteFixedNumber() throws Exception {
		final StormOutputFieldsDeclarer declarer = new StormOutputFieldsDeclarer();
		declarer.declare(new Fields("dummy"));
		PowerMockito.whenNew(StormOutputFieldsDeclarer.class).withNoArguments().thenReturn(declarer);

		final StreamingRuntimeContext taskContext = mock(StreamingRuntimeContext.class);
		when(taskContext.getExecutionConfig()).thenReturn(new ExecutionConfig());

		final IRichSpout spout = mock(IRichSpout.class);
		final int numberOfCalls = this.r.nextInt(50);
		final StormFiniteSpoutWrapper<?> spoutWrapper = new StormFiniteSpoutWrapper<Object>(spout, numberOfCalls);
		spoutWrapper.setRuntimeContext(taskContext);

		spoutWrapper.run(mock(SourceContext.class));
		verify(spout, times(numberOfCalls)).nextTuple();
	}

	@Test
	public void testRunExecute() throws Exception {
		final int numberOfCalls = this.r.nextInt(50);

		final LinkedList<Tuple1<Integer>> expectedResult = new LinkedList<Tuple1<Integer>>();
		for (int i = numberOfCalls - 1; i >= 0; --i) {
			expectedResult.add(new Tuple1<Integer>(new Integer(i)));
		}

		final StreamingRuntimeContext taskContext = mock(StreamingRuntimeContext.class);
		when(taskContext.getExecutionConfig()).thenReturn(new ExecutionConfig());

		final IRichSpout spout = new FiniteTestSpout(numberOfCalls);
		final StormFiniteSpoutWrapper<Tuple1<Integer>> spoutWrapper = new StormFiniteSpoutWrapper<Tuple1<Integer>>(
				spout);
		spoutWrapper.setRuntimeContext(taskContext);

		final TestContext collector = new TestContext();
		spoutWrapper.run(collector);

		Assert.assertEquals(expectedResult, collector.result);
	}

	@Test
	public void testCancel() throws Exception {
		final int numberOfCalls = 5 + this.r.nextInt(5);

		final LinkedList<Tuple1<Integer>> expectedResult = new LinkedList<Tuple1<Integer>>();
		expectedResult.add(new Tuple1<Integer>(new Integer(numberOfCalls - 1)));

		StreamingRuntimeContext taskContext = mock(StreamingRuntimeContext.class);
		when(taskContext.getExecutionConfig()).thenReturn(new ExecutionConfig());

		final IRichSpout spout = new FiniteTestSpout(numberOfCalls);
		final StormFiniteSpoutWrapper<Tuple1<Integer>> spoutWrapper = new StormFiniteSpoutWrapper<Tuple1<Integer>>(
				spout);
		spoutWrapper.setRuntimeContext(taskContext);

		spoutWrapper.cancel();
		final TestContext collector = new TestContext();
		spoutWrapper.run(collector);

		Assert.assertEquals(expectedResult, collector.result);
	}

}
