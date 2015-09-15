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

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.stormcompatibility.util.AbstractTest;
import org.apache.flink.stormcompatibility.util.FiniteTestSpout;
import org.apache.flink.stormcompatibility.util.StormConfig;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.streaming.runtime.tasks.StreamingRuntimeContext;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.LinkedList;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(StormWrapperSetupHelper.class)
public class StormSpoutWrapperTest extends AbstractTest {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testRunPrepare() throws Exception {
		final StormConfig stormConfig = new StormConfig();
		final Configuration flinkConfig = new Configuration();

		final ExecutionConfig taskConfig = mock(ExecutionConfig.class);
		when(taskConfig.getGlobalJobParameters()).thenReturn(null).thenReturn(stormConfig)
				.thenReturn(flinkConfig);

		final StreamingRuntimeContext taskContext = mock(StreamingRuntimeContext.class);
		when(taskContext.getExecutionConfig()).thenReturn(taskConfig);

		final IRichSpout spout = mock(IRichSpout.class);
		final StormSpoutWrapper spoutWrapper = new StormSpoutWrapper(spout);
		spoutWrapper.setRuntimeContext(taskContext);
		spoutWrapper.isRunning = false;

		// test without configuration
		spoutWrapper.run(mock(SourceContext.class));
		verify(spout).open(any(Map.class), any(TopologyContext.class),
				any(SpoutOutputCollector.class));

		// test with StormConfig
		spoutWrapper.run(mock(SourceContext.class));
		verify(spout).open(same(stormConfig), any(TopologyContext.class),
				any(SpoutOutputCollector.class));

		// test with Configuration
		spoutWrapper.run(mock(SourceContext.class));
		verify(spout, times(3)).open(eq(flinkConfig.toMap()), any(TopologyContext.class),
				any(SpoutOutputCollector.class));
	}

	@Test
	public void testRunExecuteCancelInfinite() throws Exception {
		final int numberOfCalls = 5 + this.r.nextInt(5);

		final StreamingRuntimeContext taskContext = mock(StreamingRuntimeContext.class);
		when(taskContext.getExecutionConfig()).thenReturn(new ExecutionConfig());
		when(taskContext.getTaskStubParameters()).thenReturn(new Configuration());
		when(taskContext.getTaskName()).thenReturn("name");

		final IRichSpout spout = new FiniteTestSpout(numberOfCalls);


		final StormSpoutWrapper<Tuple1<Integer>> spoutWrapper = new StormSpoutWrapper<Tuple1<Integer>>(spout);
		spoutWrapper.setRuntimeContext(taskContext);

		spoutWrapper.cancel();
		final TestContext collector = new TestContext();
		spoutWrapper.run(collector);

		Assert.assertEquals(new LinkedList<Tuple1<Integer>>(), collector.result);
	}

	@Test
	public void testClose() throws Exception {
		final IRichSpout spout = mock(IRichSpout.class);
		final StormSpoutWrapper<Tuple1<Integer>> spoutWrapper = new StormSpoutWrapper<Tuple1<Integer>>(spout);

		spoutWrapper.close();

		verify(spout).close();
	}

}
