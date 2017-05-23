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

package org.apache.flink.storm.wrappers;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.storm.util.AbstractTest;
import org.apache.flink.storm.util.FiniteSpout;
import org.apache.flink.storm.util.FiniteTestSpout;
import org.apache.flink.storm.util.StormConfig;
import org.apache.flink.storm.util.TestDummySpout;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.tuple.Fields;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the SpoutWrapper.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(WrapperSetupHelper.class)
@PowerMockIgnore({"javax.management.*", "com.sun.jndi.*", "org.apache.log4j.*"})
public class SpoutWrapperTest extends AbstractTest {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testRunPrepare() throws Exception {
		final StormConfig stormConfig = new StormConfig();
		stormConfig.put(this.r.nextInt(), this.r.nextInt());
		final Configuration flinkConfig = new Configuration();
		flinkConfig.setInteger("testKey", this.r.nextInt());

		final ExecutionConfig taskConfig = mock(ExecutionConfig.class);
		when(taskConfig.getGlobalJobParameters()).thenReturn(null).thenReturn(stormConfig)
				.thenReturn(flinkConfig);

		final StreamingRuntimeContext taskContext = mock(StreamingRuntimeContext.class);
		when(taskContext.getExecutionConfig()).thenReturn(taskConfig);
		when(taskContext.getTaskName()).thenReturn("name");

		final IRichSpout spout = mock(IRichSpout.class);
		SpoutWrapper spoutWrapper = new SpoutWrapper(spout);
		spoutWrapper.setRuntimeContext(taskContext);
		spoutWrapper.cancel();

		// test without configuration
		spoutWrapper.run(mock(SourceContext.class));
		verify(spout).open(any(Map.class), any(TopologyContext.class),
				any(SpoutOutputCollector.class));

		// test with StormConfig
		spoutWrapper.run(mock(SourceContext.class));
		verify(spout).open(eq(stormConfig), any(TopologyContext.class),
				any(SpoutOutputCollector.class));

		// test with Configuration
		final TestDummySpout testSpout = new TestDummySpout();
		spoutWrapper = new SpoutWrapper(testSpout);
		spoutWrapper.setRuntimeContext(taskContext);
		spoutWrapper.cancel();

		spoutWrapper.run(mock(SourceContext.class));
		for (Entry<String, String> entry : flinkConfig.toMap().entrySet()) {
			Assert.assertEquals(entry.getValue(), testSpout.config.get(entry.getKey()));
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testRunExecuteFixedNumber() throws Exception {
		final SetupOutputFieldsDeclarer declarer = new SetupOutputFieldsDeclarer();
		declarer.declare(new Fields("dummy"));
		PowerMockito.whenNew(SetupOutputFieldsDeclarer.class).withNoArguments()
				.thenReturn(declarer);

		final StreamingRuntimeContext taskContext = mock(StreamingRuntimeContext.class);
		when(taskContext.getExecutionConfig()).thenReturn(mock(ExecutionConfig.class));
		when(taskContext.getTaskName()).thenReturn("name");

		final IRichSpout spout = mock(IRichSpout.class);
		final int numberOfCalls = this.r.nextInt(50);
		final SpoutWrapper<?> spoutWrapper = new SpoutWrapper<Object>(spout,
				numberOfCalls);
		spoutWrapper.setRuntimeContext(taskContext);

		spoutWrapper.run(mock(SourceContext.class));
		verify(spout, times(numberOfCalls)).nextTuple();
	}

	@Test
	public void testRunExecuteFinite() throws Exception {
		final int numberOfCalls = this.r.nextInt(50);

		final LinkedList<Tuple1<Integer>> expectedResult = new LinkedList<Tuple1<Integer>>();
		for (int i = numberOfCalls - 1; i >= 0; --i) {
			expectedResult.add(new Tuple1<Integer>(new Integer(i)));
		}

		final StreamingRuntimeContext taskContext = mock(StreamingRuntimeContext.class);
		when(taskContext.getExecutionConfig()).thenReturn(mock(ExecutionConfig.class));
		when(taskContext.getTaskName()).thenReturn("name");

		final FiniteTestSpout spout = new FiniteTestSpout(numberOfCalls);
		final SpoutWrapper<Tuple1<Integer>> spoutWrapper = new SpoutWrapper<Tuple1<Integer>>(
				spout, -1);
		spoutWrapper.setRuntimeContext(taskContext);

		final TestContext collector = new TestContext();
		spoutWrapper.run(collector);

		Assert.assertEquals(expectedResult, collector.result);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void runAndExecuteFiniteSpout() throws Exception {
		final FiniteSpout stormSpout = mock(FiniteSpout.class);
		when(stormSpout.reachedEnd()).thenReturn(false, false, false, true, false, false, true);

		final StreamingRuntimeContext taskContext = mock(StreamingRuntimeContext.class);
		when(taskContext.getExecutionConfig()).thenReturn(mock(ExecutionConfig.class));
		when(taskContext.getTaskName()).thenReturn("name");

		final SpoutWrapper<?> wrapper = new SpoutWrapper<Object>(stormSpout);
		wrapper.setRuntimeContext(taskContext);

		wrapper.run(mock(SourceContext.class));
		verify(stormSpout, times(3)).nextTuple();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void runAndExecuteFiniteSpout2() throws Exception {
		final FiniteSpout stormSpout = mock(FiniteSpout.class);
		when(stormSpout.reachedEnd()).thenReturn(true, false, true, false, true, false, true);

		final StreamingRuntimeContext taskContext = mock(StreamingRuntimeContext.class);
		when(taskContext.getExecutionConfig()).thenReturn(mock(ExecutionConfig.class));
		when(taskContext.getTaskName()).thenReturn("name");

		final SpoutWrapper<?> wrapper = new SpoutWrapper<Object>(stormSpout);
		wrapper.setRuntimeContext(taskContext);

		wrapper.run(mock(SourceContext.class));
		verify(stormSpout, never()).nextTuple();
	}

	@Test
	public void testCancel() throws Exception {
		final int numberOfCalls = 5 + this.r.nextInt(5);

		final StreamingRuntimeContext taskContext = mock(StreamingRuntimeContext.class);
		when(taskContext.getExecutionConfig()).thenReturn(mock(ExecutionConfig.class));
		when(taskContext.getTaskName()).thenReturn("name");

		final IRichSpout spout = new FiniteTestSpout(numberOfCalls);

		final SpoutWrapper<Tuple1<Integer>> spoutWrapper = new SpoutWrapper<Tuple1<Integer>>(spout);
		spoutWrapper.setRuntimeContext(taskContext);

		spoutWrapper.cancel();
		final TestContext collector = new TestContext();
		spoutWrapper.run(collector);

		Assert.assertEquals(new LinkedList<Tuple1<Integer>>(), collector.result);
	}

	@Test
	public void testClose() throws Exception {
		final IRichSpout spout = mock(IRichSpout.class);
		final SpoutWrapper<Tuple1<Integer>> spoutWrapper = new SpoutWrapper<Tuple1<Integer>>(spout);

		spoutWrapper.close();

		verify(spout).close();
	}

}
