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

package org.apache.flink.storm.api;

import org.apache.flink.storm.util.TestDummyBolt;
import org.apache.flink.storm.util.TestDummySpout;
import org.apache.flink.storm.util.TestSink;

import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the FlinkTopology.
 */
public class FlinkTopologyTest {

	@Test
	public void testDefaultParallelism() {
		final TopologyBuilder builder = new TopologyBuilder();
		final FlinkTopology flinkTopology = FlinkTopology.createTopology(builder);
		Assert.assertEquals(1, flinkTopology.getExecutionEnvironment().getParallelism());
	}

	@Test(expected = RuntimeException.class)
	public void testUnknowSpout() {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new TestSpout());
		builder.setBolt("bolt", new TestBolt()).shuffleGrouping("unknown");

		FlinkTopology.createTopology(builder);
	}

	@Test(expected = RuntimeException.class)
	public void testUnknowBolt() {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new TestSpout());
		builder.setBolt("bolt1", new TestBolt()).shuffleGrouping("spout");
		builder.setBolt("bolt2", new TestBolt()).shuffleGrouping("unknown");

		FlinkTopology.createTopology(builder);
	}

	@Test(expected = RuntimeException.class)
	public void testUndeclaredStream() {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new TestSpout());
		builder.setBolt("bolt", new TestBolt()).shuffleGrouping("spout");

		FlinkTopology.createTopology(builder);
	}

	@Test
	public void testFieldsGroupingOnMultipleSpoutOutputStreams() {
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new TestDummySpout());
		builder.setBolt("sink", new TestSink()).fieldsGrouping("spout",
				TestDummySpout.SPOUT_STREAM_ID, new Fields("id"));

		FlinkTopology.createTopology(builder);
	}

	@Test
	public void testFieldsGroupingOnMultipleBoltOutputStreams() {
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new TestDummySpout());
		builder.setBolt("bolt", new TestDummyBolt()).shuffleGrouping("spout");
		builder.setBolt("sink", new TestSink()).fieldsGrouping("bolt",
				TestDummyBolt.GROUPING_STREAM_ID, new Fields("id"));

		FlinkTopology.createTopology(builder);
	}

}
