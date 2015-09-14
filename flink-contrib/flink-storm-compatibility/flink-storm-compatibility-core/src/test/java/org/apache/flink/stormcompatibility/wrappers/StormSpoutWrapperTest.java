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
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.state.StateHandleProvider;
import org.apache.flink.runtime.taskmanager.RuntimeEnvironment;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.stormcompatibility.util.AbstractTest;
import org.apache.flink.stormcompatibility.util.StormConfig;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.runtime.tasks.StreamingRuntimeContext;
import org.apache.flink.util.InstantiationUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(PowerMockRunner.class)
@PrepareForTest(StormWrapperSetupHelper.class)
public class StormSpoutWrapperTest extends AbstractTest {

	@Test
	public void testRunExecuteCancelInfinite() throws Exception {
		final int numberOfCalls = 5 + this.r.nextInt(5);

		final IRichSpout spout = new FiniteTestSpout(numberOfCalls);
		final StormSpoutWrapper<Tuple1<Integer>> spoutWrapper = new StormSpoutWrapper<Tuple1<Integer>>(spout);
		spoutWrapper.setRuntimeContext(mock(StreamingRuntimeContext.class));

		spoutWrapper.cancel();
		final TestContext collector = new TestContext();
		spoutWrapper.run(collector);

		Assert.assertEquals(new LinkedList<Tuple1<Integer>>(), collector.result);
	}

	@Test
	public void testOpenWithStormConf() throws Exception {
		final IRichSpout spout = mock(IRichSpout.class);
		final StormSpoutWrapper<Tuple1<Integer>> spoutWrapper = new StormSpoutWrapper<Tuple1<Integer>>(spout);

		Map stormConf = new HashMap();
		stormConf.put("path", "/home/user/file.txt");
		stormConf.put(1, 1024);
		byte[] bytes = InstantiationUtil.serializeObject(stormConf);
		Configuration jobConfiguration = new Configuration();
		jobConfiguration.setBytes(StormConfig.STORM_DEFAULT_CONFIG, bytes);
		jobConfiguration.setInteger("port", 5566);
		Environment env = new RuntimeEnvironment(new JobID(), new JobVertexID(), new ExecutionAttemptID(),
				new String(), new String(), 1, 2, jobConfiguration, mock(Configuration.class), mock(ClassLoader.class),
				mock(MemoryManager.class), mock(IOManager.class), mock(BroadcastVariableManager.class),
				mock(AccumulatorRegistry.class), mock(InputSplitProvider.class), mock(Map.class),
				new ResultPartitionWriter[1], new InputGate[1], mock(ActorGateway.class),
				mock(TaskManagerRuntimeInfo.class));
		StreamingRuntimeContext runtimeContext = new StreamingRuntimeContext(env, new ExecutionConfig(),
				mock(KeySelector.class),
				mock(StateHandleProvider.class), mock(Map.class));

		spoutWrapper.setRuntimeContext(runtimeContext);
		spoutWrapper.open(mock(Configuration.class));
		final SourceFunction.SourceContext ctx = mock(SourceFunction.SourceContext.class);
		spoutWrapper.cancel();
		spoutWrapper.run(ctx);

		verify(spout).open(eq(stormConf), any(TopologyContext.class), any(SpoutOutputCollector.class));
	}

	@Test
	public void testOpenWithJobConf() throws Exception {
		final IRichSpout spout = mock(IRichSpout.class);
		final StormSpoutWrapper<Tuple1<Integer>> spoutWrapper = new StormSpoutWrapper<Tuple1<Integer>>(spout);

		ExecutionConfig executionConfig = new ExecutionConfig();
		Configuration jobParameters = new Configuration();
		jobParameters.setString("path", "/home/user/file.txt");
		executionConfig.setGlobalJobParameters(jobParameters);
		byte[] bytes = InstantiationUtil.serializeObject(executionConfig);
		Configuration jobConfiguration = new Configuration();
		jobConfiguration.setBytes(ExecutionConfig.CONFIG_KEY, bytes);
		Environment env = new RuntimeEnvironment(new JobID(), new JobVertexID(), new ExecutionAttemptID(),
				new String(), new String(), 1, 2, jobConfiguration, mock(Configuration.class), mock(ClassLoader.class),
				mock(MemoryManager.class), mock(IOManager.class), mock(BroadcastVariableManager.class),
				mock(AccumulatorRegistry.class), mock(InputSplitProvider.class), mock(Map.class),
				new ResultPartitionWriter[1], new InputGate[1], mock(ActorGateway.class),
				mock(TaskManagerRuntimeInfo.class));
		StreamingRuntimeContext runtimeContext = new StreamingRuntimeContext(env, new ExecutionConfig(),
				mock(KeySelector.class),
				mock(StateHandleProvider.class), mock(Map.class));

		spoutWrapper.setRuntimeContext(runtimeContext);
		spoutWrapper.open(mock(Configuration.class));
		final SourceFunction.SourceContext ctx = mock(SourceFunction.SourceContext.class);
		spoutWrapper.cancel();
		spoutWrapper.run(ctx);

		verify(spout).open(eq(jobParameters.toMap()), any(TopologyContext.class), any(SpoutOutputCollector.class));
	}

	@Test
	public void testClose() throws Exception {
		final IRichSpout spout = mock(IRichSpout.class);
		final StormSpoutWrapper<Tuple1<Integer>> spoutWrapper = new StormSpoutWrapper<Tuple1<Integer>>(spout);

		spoutWrapper.close();

		verify(spout).close();
	}

}
