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

import java.util.HashMap;
import java.util.Map;

import backtype.storm.topology.IComponent;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.functions.KeySelector;
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
import org.apache.flink.streaming.runtime.tasks.StreamingRuntimeContext;
import org.apache.flink.util.InstantiationUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.collect.Sets;

import static org.mockito.Mockito.mock;

@RunWith(PowerMockRunner.class)
@PrepareForTest(StormWrapperSetupHelper.class)
public class StormWrapperSetupHelperTest extends AbstractTest {

	@Test
	public void testEmptyDeclarerBolt() {
		IComponent boltOrSpout;

		if (this.r.nextBoolean()) {
			boltOrSpout = mock(IRichSpout.class);
		} else {
			boltOrSpout = mock(IRichBolt.class);
		}

		Assert.assertEquals(new HashMap<String, Integer>(),
				StormWrapperSetupHelper.getNumberOfAttributes(boltOrSpout, null));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testRawType() throws Exception {
		IComponent boltOrSpout;

		if (this.r.nextBoolean()) {
			boltOrSpout = mock(IRichSpout.class);
		} else {
			boltOrSpout = mock(IRichBolt.class);
		}

		final StormOutputFieldsDeclarer declarer = new StormOutputFieldsDeclarer();
		declarer.declare(new Fields("dummy1", "dummy2"));
		PowerMockito.whenNew(StormOutputFieldsDeclarer.class).withNoArguments().thenReturn(declarer);

		StormWrapperSetupHelper.getNumberOfAttributes(boltOrSpout,
				Sets.newHashSet(new String[] { Utils.DEFAULT_STREAM_ID }));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testToManyAttributes() throws Exception {
		IComponent boltOrSpout;

		if (this.r.nextBoolean()) {
			boltOrSpout = mock(IRichSpout.class);
		} else {
			boltOrSpout = mock(IRichBolt.class);
		}

		final StormOutputFieldsDeclarer declarer = new StormOutputFieldsDeclarer();
		final String[] schema = new String[26];
		for (int i = 0; i < schema.length; ++i) {
			schema[i] = "a" + i;
		}
		declarer.declare(new Fields(schema));
		PowerMockito.whenNew(StormOutputFieldsDeclarer.class).withNoArguments().thenReturn(declarer);

		StormWrapperSetupHelper.getNumberOfAttributes(boltOrSpout, null);
	}

	@Test
	public void testTupleTypes() throws Exception {
		for (int i = -1; i < 26; ++i) {
			this.testTupleTypes(i);
		}
	}

	private void testTupleTypes(final int numberOfAttributes) throws Exception {
		String[] schema;
		if (numberOfAttributes == -1) {
			schema = new String[1];
		} else {
			schema = new String[numberOfAttributes];
		}
		for (int i = 0; i < schema.length; ++i) {
			schema[i] = "a" + i;
		}

		IComponent boltOrSpout;
		if (this.r.nextBoolean()) {
			boltOrSpout = mock(IRichSpout.class);
		} else {
			boltOrSpout = mock(IRichBolt.class);
		}

		final StormOutputFieldsDeclarer declarer = new StormOutputFieldsDeclarer();
		declarer.declare(new Fields(schema));
		PowerMockito.whenNew(StormOutputFieldsDeclarer.class).withNoArguments().thenReturn(declarer);

		HashMap<String, Integer> attributes = new HashMap<String, Integer>();
		attributes.put(Utils.DEFAULT_STREAM_ID, numberOfAttributes);

		Assert.assertEquals(attributes, StormWrapperSetupHelper.getNumberOfAttributes(
				boltOrSpout,
				numberOfAttributes == -1 ? Sets
						.newHashSet(new String[] { Utils.DEFAULT_STREAM_ID }) : null));
	}

	@Test
	public void testGetConfTopologiesMode() throws Exception {
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
		StreamingRuntimeContext ctx = new StreamingRuntimeContext(env, new ExecutionConfig(),
				mock(KeySelector.class),
				mock(StateHandleProvider.class), mock(Map.class));

		Assert.assertEquals(stormConf, StormWrapperSetupHelper.getStormConfFromContext(ctx));
	}

	@Test
	public void testGetConfEmbeddedMode() throws Exception {
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
		StreamingRuntimeContext ctx = new StreamingRuntimeContext(env, new ExecutionConfig(),
				mock(KeySelector.class),
				mock(StateHandleProvider.class), mock(Map.class));

		Assert.assertEquals(jobParameters.toMap(), StormWrapperSetupHelper.getStormConfFromContext(ctx));
	}
}
