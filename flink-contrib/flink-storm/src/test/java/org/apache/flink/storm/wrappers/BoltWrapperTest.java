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
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.storm.util.AbstractTest;
import org.apache.flink.storm.util.SplitStreamType;
import org.apache.flink.storm.util.StormConfig;
import org.apache.flink.storm.util.TestDummyBolt;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.util.MockStreamConfig;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.MessageId;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the BoltWrapper.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({StreamElementSerializer.class, WrapperSetupHelper.class, StreamRecord.class})
@PowerMockIgnore({"javax.management.*", "com.sun.jndi.*", "org.apache.log4j.*"})
public class BoltWrapperTest extends AbstractTest {

	@Test(expected = IllegalArgumentException.class)
	public void testWrapperRawType() throws Exception {
		final SetupOutputFieldsDeclarer declarer = new SetupOutputFieldsDeclarer();
		declarer.declare(new Fields("dummy1", "dummy2"));
		PowerMockito.whenNew(SetupOutputFieldsDeclarer.class).withNoArguments().thenReturn(declarer);

		new BoltWrapper<Object, Object>(mock(IRichBolt.class), new String[] { Utils.DEFAULT_STREAM_ID });
	}

	@Test(expected = IllegalArgumentException.class)
	public void testWrapperToManyAttributes1() throws Exception {
		final SetupOutputFieldsDeclarer declarer = new SetupOutputFieldsDeclarer();
		final String[] schema = new String[26];
		for (int i = 0; i < schema.length; ++i) {
			schema[i] = "a" + i;
		}
		declarer.declare(new Fields(schema));
		PowerMockito.whenNew(SetupOutputFieldsDeclarer.class).withNoArguments().thenReturn(declarer);

		new BoltWrapper<Object, Object>(mock(IRichBolt.class));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testWrapperToManyAttributes2() throws Exception {
		final SetupOutputFieldsDeclarer declarer = new SetupOutputFieldsDeclarer();
		final String[] schema = new String[26];
		for (int i = 0; i < schema.length; ++i) {
			schema[i] = "a" + i;
		}
		declarer.declare(new Fields(schema));
		PowerMockito.whenNew(SetupOutputFieldsDeclarer.class).withNoArguments().thenReturn(declarer);

		new BoltWrapper<Object, Object>(mock(IRichBolt.class), new String[] {});
	}

	@Test
	public void testWrapper() throws Exception {
		for (int i = -1; i < 26; ++i) {
			this.testWrapper(i);
		}
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	private void testWrapper(final int numberOfAttributes) throws Exception {
		assert ((-1 <= numberOfAttributes) && (numberOfAttributes <= 25));
		Tuple flinkTuple = null;
		String rawTuple = null;

		if (numberOfAttributes == -1) {
			rawTuple = "test";
		} else {
			flinkTuple = Tuple.getTupleClass(numberOfAttributes).newInstance();
		}

		final String[] schema;
		if (numberOfAttributes == -1) {
			schema = new String[1];
		} else {
			schema = new String[numberOfAttributes];
		}
		for (int i = 0; i < schema.length; ++i) {
			schema[i] = "a" + i;
		}

		final StreamRecord record = mock(StreamRecord.class);
		if (numberOfAttributes == -1) {
			when(record.getValue()).thenReturn(rawTuple);
		} else {
			when(record.getValue()).thenReturn(flinkTuple);
		}

		final StreamingRuntimeContext taskContext = mock(StreamingRuntimeContext.class);
		when(taskContext.getExecutionConfig()).thenReturn(mock(ExecutionConfig.class));
		when(taskContext.getTaskName()).thenReturn("name");
		when(taskContext.getMetricGroup()).thenReturn(new UnregisteredMetricsGroup());

		final IRichBolt bolt = mock(IRichBolt.class);

		final SetupOutputFieldsDeclarer declarer = new SetupOutputFieldsDeclarer();
		declarer.declare(new Fields(schema));
		PowerMockito.whenNew(SetupOutputFieldsDeclarer.class).withNoArguments().thenReturn(declarer);

		final BoltWrapper wrapper = new BoltWrapper(bolt, (Fields) null);
		wrapper.setup(createMockStreamTask(), new MockStreamConfig(), mock(Output.class));
		wrapper.open();

		wrapper.processElement(record);
		if (numberOfAttributes == -1) {
			verify(bolt).execute(
					eq(new StormTuple<String>(rawTuple, null, -1, null, null, MessageId
							.makeUnanchored())));
		} else {
			verify(bolt).execute(
					eq(new StormTuple<Tuple>(flinkTuple, null, -1, null, null, MessageId
							.makeUnanchored())));
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testMultipleOutputStreams() throws Exception {
		final boolean rawOutType1 = super.r.nextBoolean();
		final boolean rawOutType2 = super.r.nextBoolean();

		final StreamRecord record = mock(StreamRecord.class);
		when(record.getValue()).thenReturn(2).thenReturn(3);

		final Output output = mock(Output.class);

		final TestBolt bolt = new TestBolt();
		final HashSet<String> raw = new HashSet<String>();
		if (rawOutType1) {
			raw.add("stream1");
		}
		if (rawOutType2) {
			raw.add("stream2");
		}

		final BoltWrapper wrapper = new BoltWrapper(bolt, null, raw);
		wrapper.setup(createMockStreamTask(), new MockStreamConfig(), output);
		wrapper.open();

		final SplitStreamType splitRecord = new SplitStreamType<Integer>();
		if (rawOutType1) {
			splitRecord.streamId = "stream1";
			splitRecord.value = 2;
		} else {
			splitRecord.streamId = "stream1";
			splitRecord.value = new Tuple1<Integer>(2);
		}
		wrapper.processElement(record);
		verify(output).collect(new StreamRecord<SplitStreamType>(splitRecord));

		if (rawOutType2) {
			splitRecord.streamId = "stream2";
			splitRecord.value = 3;
		} else {
			splitRecord.streamId = "stream2";
			splitRecord.value = new Tuple1<Integer>(3);
		}
		wrapper.processElement(record);
		verify(output, times(2)).collect(new StreamRecord<SplitStreamType>(splitRecord));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testOpen() throws Exception {

		// utility mocks
		final StormConfig stormConfig = new StormConfig();
		final Configuration flinkConfig = new Configuration();

		final ExecutionConfig taskConfig = mock(ExecutionConfig.class);
		when(taskConfig.getGlobalJobParameters()).thenReturn(null).thenReturn(stormConfig)
		.thenReturn(flinkConfig);

		final StreamingRuntimeContext taskContext = mock(StreamingRuntimeContext.class);
		when(taskContext.getExecutionConfig()).thenReturn(taskConfig);
		when(taskContext.getTaskName()).thenReturn("name");
		when(taskContext.getMetricGroup()).thenReturn(new UnregisteredMetricsGroup());

		final SetupOutputFieldsDeclarer declarer = new SetupOutputFieldsDeclarer();
		declarer.declare(new Fields("dummy"));
		PowerMockito.whenNew(SetupOutputFieldsDeclarer.class).withNoArguments().thenReturn(declarer);

		// (1) open with no configuration
		{
			ExecutionConfig execConfig = mock(ExecutionConfig.class);
			when(execConfig.getGlobalJobParameters()).thenReturn(null);

			final IRichBolt bolt = mock(IRichBolt.class);
			BoltWrapper<Object, Object> wrapper = new BoltWrapper<Object, Object>(bolt);
			wrapper.setup(createMockStreamTask(execConfig), new MockStreamConfig(), mock(Output.class));

			wrapper.open();
			verify(bolt).prepare(any(Map.class), any(TopologyContext.class), any(OutputCollector.class));
		}

		// (2) open with a storm specific configuration
		{
			ExecutionConfig execConfig = mock(ExecutionConfig.class);
			when(execConfig.getGlobalJobParameters()).thenReturn(stormConfig);

			final IRichBolt bolt = mock(IRichBolt.class);
			BoltWrapper<Object, Object> wrapper = new BoltWrapper<Object, Object>(bolt);
			wrapper.setup(createMockStreamTask(execConfig), new MockStreamConfig(), mock(Output.class));

			wrapper.open();
			verify(bolt).prepare(same(stormConfig), any(TopologyContext.class), any(OutputCollector.class));
		}

		// (3) open with a flink config
		{
			final Configuration cfg = new Configuration();
			cfg.setString("foo", "bar");
			cfg.setInteger("the end (the int)", Integer.MAX_VALUE);

			ExecutionConfig execConfig = mock(ExecutionConfig.class);
			when(execConfig.getGlobalJobParameters()).thenReturn(new UnmodifiableConfiguration(cfg));

			TestDummyBolt testBolt = new TestDummyBolt();
			BoltWrapper<Object, Object> wrapper = new BoltWrapper<Object, Object>(testBolt);
			wrapper.setup(createMockStreamTask(execConfig), new MockStreamConfig(), mock(Output.class));

			wrapper.open();
			for (Entry<String, String> entry : cfg.toMap().entrySet()) {
				Assert.assertEquals(entry.getValue(), testBolt.config.get(entry.getKey()));
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testOpenSink() throws Exception {
		final StormConfig stormConfig = new StormConfig();
		final Configuration flinkConfig = new Configuration();

		final ExecutionConfig taskConfig = mock(ExecutionConfig.class);
		when(taskConfig.getGlobalJobParameters()).thenReturn(null).thenReturn(stormConfig)
		.thenReturn(flinkConfig);

		final StreamingRuntimeContext taskContext = mock(StreamingRuntimeContext.class);
		when(taskContext.getExecutionConfig()).thenReturn(taskConfig);
		when(taskContext.getTaskName()).thenReturn("name");
		when(taskContext.getMetricGroup()).thenReturn(new UnregisteredMetricsGroup());

		final IRichBolt bolt = mock(IRichBolt.class);
		BoltWrapper<Object, Object> wrapper = new BoltWrapper<Object, Object>(bolt);

		wrapper.setup(createMockStreamTask(), new MockStreamConfig(), mock(Output.class));
		wrapper.open();

		verify(bolt).prepare(any(Map.class), any(TopologyContext.class), isNotNull(OutputCollector.class));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testClose() throws Exception {
		final IRichBolt bolt = mock(IRichBolt.class);

		final SetupOutputFieldsDeclarer declarer = new SetupOutputFieldsDeclarer();
		declarer.declare(new Fields("dummy"));
		PowerMockito.whenNew(SetupOutputFieldsDeclarer.class).withNoArguments().thenReturn(declarer);

		final BoltWrapper<Object, Object> wrapper = new BoltWrapper<Object, Object>(bolt);

		wrapper.setup(createMockStreamTask(), new MockStreamConfig(), mock(Output.class));

		wrapper.close();
		wrapper.dispose();

		verify(bolt).cleanup();
	}

	private static final class TestBolt implements IRichBolt {
		private static final long serialVersionUID = 7278692872260138758L;
		private transient OutputCollector collector;

		@SuppressWarnings("rawtypes")
		@Override
		public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
			this.collector = collector;
		}

		int counter = 0;
		@Override
		public void execute(org.apache.storm.tuple.Tuple input) {
			if (++counter % 2 == 1) {
				this.collector.emit("stream1", new Values(input.getInteger(0)));
			} else {
				this.collector.emit("stream2", new Values(input.getInteger(0)));
			}
		}

		@Override
		public void cleanup() {}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declareStream("stream1", new Fields("a1"));
			declarer.declareStream("stream2", new Fields("a2"));
		}

		@Override
		public Map<String, Object> getComponentConfiguration() {
			return null;
		}
	}

	public static StreamTask<?, ?> createMockStreamTask() {
		return createMockStreamTask(new ExecutionConfig());
	}

	public static StreamTask<?, ?> createMockStreamTask(ExecutionConfig execConfig) {
		Environment env = mock(Environment.class);
		when(env.getTaskInfo()).thenReturn(new TaskInfo("Mock Task", 1, 0, 1, 0));
		when(env.getUserClassLoader()).thenReturn(BoltWrapperTest.class.getClassLoader());
		when(env.getMetricGroup()).thenReturn(UnregisteredMetricGroups.createUnregisteredTaskMetricGroup());
		when(env.getTaskManagerInfo()).thenReturn(new TestingTaskManagerRuntimeInfo());

		final CloseableRegistry closeableRegistry = new CloseableRegistry();
		StreamTask<?, ?> mockTask = mock(StreamTask.class);
		when(mockTask.getCheckpointLock()).thenReturn(new Object());
		when(mockTask.getConfiguration()).thenReturn(new MockStreamConfig());
		when(mockTask.getEnvironment()).thenReturn(env);
		when(mockTask.getExecutionConfig()).thenReturn(execConfig);
		when(mockTask.getCancelables()).thenReturn(closeableRegistry);

		return mockTask;
	}
}
