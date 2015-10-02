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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.StormTopology;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IComponent;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import org.apache.flink.storm.api.TestTopologyBuilder;
import org.apache.flink.storm.util.AbstractTest;
import org.apache.flink.storm.util.TestDummyBolt;
import org.apache.flink.storm.util.TestDummySpout;
import org.apache.flink.storm.util.TestSink;
import org.apache.flink.storm.wrappers.SetupOutputFieldsDeclarer;
import org.apache.flink.storm.wrappers.WrapperSetupHelper;
import org.apache.flink.streaming.runtime.tasks.StreamingRuntimeContext;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.collect.Sets;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@PowerMockIgnore("javax.*")
@RunWith(PowerMockRunner.class)
@PrepareForTest(WrapperSetupHelper.class)
public class WrapperSetupHelperTest extends AbstractTest {

	@Test
	public void testEmptyDeclarerBolt() {
		IComponent boltOrSpout;

		if (this.r.nextBoolean()) {
			boltOrSpout = mock(IRichSpout.class);
		} else {
			boltOrSpout = mock(IRichBolt.class);
		}

		Assert.assertEquals(new HashMap<String, Integer>(),
				WrapperSetupHelper.getNumberOfAttributes(boltOrSpout, null));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testRawType() throws Exception {
		IComponent boltOrSpout;

		if (this.r.nextBoolean()) {
			boltOrSpout = mock(IRichSpout.class);
		} else {
			boltOrSpout = mock(IRichBolt.class);
		}

		final SetupOutputFieldsDeclarer declarer = new SetupOutputFieldsDeclarer();
		declarer.declare(new Fields("dummy1", "dummy2"));
		PowerMockito.whenNew(SetupOutputFieldsDeclarer.class).withNoArguments().thenReturn(declarer);

		WrapperSetupHelper.getNumberOfAttributes(boltOrSpout,
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

		final SetupOutputFieldsDeclarer declarer = new SetupOutputFieldsDeclarer();
		final String[] schema = new String[26];
		for (int i = 0; i < schema.length; ++i) {
			schema[i] = "a" + i;
		}
		declarer.declare(new Fields(schema));
		PowerMockito.whenNew(SetupOutputFieldsDeclarer.class).withNoArguments().thenReturn(declarer);

		WrapperSetupHelper.getNumberOfAttributes(boltOrSpout, null);
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

		final SetupOutputFieldsDeclarer declarer = new SetupOutputFieldsDeclarer();
		declarer.declare(new Fields(schema));
		PowerMockito.whenNew(SetupOutputFieldsDeclarer.class).withNoArguments().thenReturn(declarer);

		HashMap<String, Integer> attributes = new HashMap<String, Integer>();
		attributes.put(Utils.DEFAULT_STREAM_ID, numberOfAttributes);

		Assert.assertEquals(attributes, WrapperSetupHelper.getNumberOfAttributes(
				boltOrSpout,
				numberOfAttributes == -1 ? Sets
						.newHashSet(new String[] { Utils.DEFAULT_STREAM_ID }) : null));
	}

	@Test
	public void testCreateTopologyContext() {
		HashMap<String, Integer> dops = new HashMap<String, Integer>();
		dops.put("spout1", 1);
		dops.put("spout2", 3);
		dops.put("bolt1", 1);
		dops.put("bolt2", 2);
		dops.put("sink", 1);

		HashMap<String, Integer> taskCounter = new HashMap<String, Integer>();
		taskCounter.put("spout1", 0);
		taskCounter.put("spout2", 0);
		taskCounter.put("bolt1", 0);
		taskCounter.put("bolt2", 0);
		taskCounter.put("sink", 0);

		HashMap<String, IComponent> operators = new HashMap<String, IComponent>();
		operators.put("spout1", new TestDummySpout());
		operators.put("spout2", new TestDummySpout());
		operators.put("bolt1", new TestDummyBolt());
		operators.put("bolt2", new TestDummyBolt());
		operators.put("sink", new TestSink());

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout1", (IRichSpout) operators.get("spout1"), dops.get("spout1"));
		builder.setSpout("spout2", (IRichSpout) operators.get("spout2"), dops.get("spout2"));
		builder.setBolt("bolt1", (IRichBolt) operators.get("bolt1"), dops.get("bolt1")).shuffleGrouping("spout1");
		builder.setBolt("bolt2", (IRichBolt) operators.get("bolt2"), dops.get("bolt2")).allGrouping("spout2");
		builder.setBolt("sink", (IRichBolt) operators.get("sink"), dops.get("sink"))
		.fieldsGrouping("bolt1", TestDummyBolt.groupingStreamId, new Fields("id"))
		.shuffleGrouping("bolt1", TestDummyBolt.shuffleStreamId)
		.fieldsGrouping("bolt2", TestDummyBolt.groupingStreamId, new Fields("id"))
		.shuffleGrouping("bolt2", TestDummyBolt.shuffleStreamId);

		final int maxRetry = 3;
		int counter;
		for (counter = 0; counter < maxRetry; ++counter) {
			LocalCluster cluster = new LocalCluster();
			Config c = new Config();
			c.setNumAckers(0);
			cluster.submitTopology("test", c, builder.createTopology());
			Utils.sleep((counter + 1) * 5000);
			cluster.shutdown();

			if (TestSink.result.size() == 8) {
				break;
			}
		}
		Assert.assertTrue(counter < maxRetry);

		TestTopologyBuilder flinkBuilder = new TestTopologyBuilder();

		flinkBuilder.setSpout("spout1", (IRichSpout) operators.get("spout1"), dops.get("spout1"));
		flinkBuilder.setSpout("spout2", (IRichSpout) operators.get("spout2"), dops.get("spout2"));
		flinkBuilder.setBolt("bolt1", (IRichBolt) operators.get("bolt1"), dops.get("bolt1")).shuffleGrouping("spout1");
		flinkBuilder.setBolt("bolt2", (IRichBolt) operators.get("bolt2"), dops.get("bolt2")).allGrouping("spout2");
		flinkBuilder.setBolt("sink", (IRichBolt) operators.get("sink"), dops.get("sink"))
		.fieldsGrouping("bolt1", TestDummyBolt.groupingStreamId, new Fields("id"))
		.shuffleGrouping("bolt1", TestDummyBolt.shuffleStreamId)
		.fieldsGrouping("bolt2", TestDummyBolt.groupingStreamId, new Fields("id"))
		.shuffleGrouping("bolt2", TestDummyBolt.shuffleStreamId);

		flinkBuilder.createTopology();
		StormTopology stormTopology = flinkBuilder.getStormTopology();

		Set<Integer> taskIds = new HashSet<Integer>();

		for (TopologyContext expectedContext : TestSink.result) {
			final String thisComponentId = expectedContext.getThisComponentId();
			int index = taskCounter.get(thisComponentId);

			StreamingRuntimeContext context = mock(StreamingRuntimeContext.class);
			when(context.getTaskName()).thenReturn(thisComponentId);
			when(context.getNumberOfParallelSubtasks()).thenReturn(dops.get(thisComponentId));
			when(context.getIndexOfThisSubtask()).thenReturn(index);
			taskCounter.put(thisComponentId, ++index);

			Config stormConfig = new Config();
			stormConfig.put(WrapperSetupHelper.TOPOLOGY_NAME, "test");

			TopologyContext topologyContext = WrapperSetupHelper.createTopologyContext(
					context, operators.get(thisComponentId), stormTopology, stormConfig);

			ComponentCommon expcetedCommon = expectedContext.getComponentCommon(thisComponentId);
			ComponentCommon common = topologyContext.getComponentCommon(thisComponentId);

			Assert.assertNull(topologyContext.getCodeDir());
			Assert.assertNull(common.get_json_conf());
			Assert.assertNull(topologyContext.getExecutorData(null));
			Assert.assertNull(topologyContext.getPIDDir());
			Assert.assertNull(topologyContext.getResource(null));
			Assert.assertNull(topologyContext.getSharedExecutor());
			Assert.assertNull(expectedContext.getTaskData(null));
			Assert.assertNull(topologyContext.getThisWorkerPort());

			Assert.assertTrue(expectedContext.getStormId().startsWith(topologyContext.getStormId()));

			Assert.assertEquals(expcetedCommon.get_inputs(), common.get_inputs());
			Assert.assertEquals(expcetedCommon.get_parallelism_hint(), common.get_parallelism_hint());
			Assert.assertEquals(expcetedCommon.get_streams(), common.get_streams());
			Assert.assertEquals(expectedContext.getComponentIds(), topologyContext.getComponentIds());
			Assert.assertEquals(expectedContext.getComponentStreams(thisComponentId),
					topologyContext.getComponentStreams(thisComponentId));
			Assert.assertEquals(thisComponentId, topologyContext.getThisComponentId());
			Assert.assertEquals(expectedContext.getThisSources(), topologyContext.getThisSources());
			Assert.assertEquals(expectedContext.getThisStreams(), topologyContext.getThisStreams());
			Assert.assertEquals(expectedContext.getThisTargets(), topologyContext.getThisTargets());
			Assert.assertEquals(0, topologyContext.getThisWorkerTasks().size());

			for (int taskId : topologyContext.getComponentTasks(thisComponentId)) {
				Assert.assertEquals(thisComponentId, topologyContext.getComponentId(taskId));
			}

			for (String componentId : expectedContext.getComponentIds()) {
				Assert.assertEquals(expectedContext.getSources(componentId),
						topologyContext.getSources(componentId));
				Assert.assertEquals(expectedContext.getTargets(componentId),
						topologyContext.getTargets(componentId));

				for (String streamId : expectedContext.getComponentStreams(componentId)) {
					Assert.assertEquals(
							expectedContext.getComponentOutputFields(componentId, streamId).toList(),
							topologyContext.getComponentOutputFields(componentId, streamId).toList());
				}
			}

			for (String streamId : expectedContext.getThisStreams()) {
				Assert.assertEquals(expectedContext.getThisOutputFields(streamId).toList(),
						topologyContext.getThisOutputFields(streamId).toList());
			}

			HashMap<Integer, String> taskToComponents = new HashMap<Integer, String>();
			Set<Integer> allTaskIds = new HashSet<Integer>();
			for (String componentId : expectedContext.getComponentIds()) {
				List<Integer> possibleTasks = expectedContext.getComponentTasks(componentId);
				List<Integer> tasks = topologyContext.getComponentTasks(componentId);

				Iterator<Integer> p_it = possibleTasks.iterator();
				Iterator<Integer> t_it = tasks.iterator();
				while(p_it.hasNext()) {
					Assert.assertTrue(t_it.hasNext());
					Assert.assertNull(taskToComponents.put(p_it.next(), componentId));
					Assert.assertTrue(allTaskIds.add(t_it.next()));
				}
				Assert.assertFalse(t_it.hasNext());
			}

			Assert.assertEquals(taskToComponents, expectedContext.getTaskToComponent());
			Assert.assertTrue(taskIds.add(topologyContext.getThisTaskId()));

			try {
				topologyContext.getHooks();
				Assert.fail();
			} catch (UnsupportedOperationException e) { /* expected */ }

			try {
				topologyContext.getRegisteredMetricByName(null);
				Assert.fail();
			} catch (UnsupportedOperationException e) { /* expected */ }
		}
	}

}
