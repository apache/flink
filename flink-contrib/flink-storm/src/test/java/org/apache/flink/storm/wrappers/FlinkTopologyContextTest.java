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

import org.apache.flink.storm.util.AbstractTest;

import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StateSpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.metric.api.ICombiner;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.metric.api.IReducer;
import org.junit.Test;

import java.util.HashMap;

/**
 * FlinkTopologyContext.getSources(componentId) and FlinkTopologyContext.getTargets(componentId) are not tested here,
 * because those are tested in StormWrapperSetupHelperTest.
 */
public class FlinkTopologyContextTest extends AbstractTest {

	@Test(expected = UnsupportedOperationException.class)
	public void testAddTaskHook() {
		new FlinkTopologyContext(new StormTopology(new HashMap<String, SpoutSpec>(),
				new HashMap<String, Bolt>(), new HashMap<String, StateSpoutSpec>()), null, null,
				null, null, null, null, null, null, null, null, null, null, null, null, null)
		.addTaskHook(null);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testGetHooks() {
		new FlinkTopologyContext(new StormTopology(new HashMap<String, SpoutSpec>(),
				new HashMap<String, Bolt>(), new HashMap<String, StateSpoutSpec>()), null, null,
				null, null, null, null, null, null, null, null, null, null, null, null, null)
		.getHooks();
	}

	@SuppressWarnings("rawtypes")
	@Test(expected = UnsupportedOperationException.class)
	public void testRegisteredMetric1() {
		new FlinkTopologyContext(new StormTopology(new HashMap<String, SpoutSpec>(),
				new HashMap<String, Bolt>(), new HashMap<String, StateSpoutSpec>()), null, null,
				null, null, null, null, null, null, null, null, null, null, null, null, null)
		.registerMetric(null, (ICombiner) null, 0);
	}

	@SuppressWarnings("rawtypes")
	@Test(expected = UnsupportedOperationException.class)
	public void testRegisteredMetric2() {
		new FlinkTopologyContext(new StormTopology(new HashMap<String, SpoutSpec>(),
				new HashMap<String, Bolt>(), new HashMap<String, StateSpoutSpec>()), null, null,
				null, null, null, null, null, null, null, null, null, null, null, null, null)
		.registerMetric(null, (IReducer) null, 0);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testRegisteredMetric3() {
		new FlinkTopologyContext(new StormTopology(new HashMap<String, SpoutSpec>(),
				new HashMap<String, Bolt>(), new HashMap<String, StateSpoutSpec>()), null, null,
				null, null, null, null, null, null, null, null, null, null, null, null, null)
		.registerMetric(null, (IMetric) null, 0);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testGetRegisteredMetricByName() {
		new FlinkTopologyContext(new StormTopology(new HashMap<String, SpoutSpec>(),
				new HashMap<String, Bolt>(), new HashMap<String, StateSpoutSpec>()), null, null,
				null, null, null, null, null, null, null, null, null, null, null, null, null)
		.getRegisteredMetricByName(null);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testSetAllSubscribedState() {
		new FlinkTopologyContext(new StormTopology(new HashMap<String, SpoutSpec>(),
				new HashMap<String, Bolt>(), new HashMap<String, StateSpoutSpec>()), null, null,
				null, null, null, null, null, null, null, null, null, null, null, null, null)
		.setAllSubscribedState(null);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testSetSubscribedState1() {
		new FlinkTopologyContext(new StormTopology(new HashMap<String, SpoutSpec>(),
				new HashMap<String, Bolt>(), new HashMap<String, StateSpoutSpec>()), null, null,
				null, null, null, null, null, null, null, null, null, null, null, null, null)
		.setSubscribedState(null, null);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testSetSubscribedState2() {
		new FlinkTopologyContext(new StormTopology(new HashMap<String, SpoutSpec>(),
				new HashMap<String, Bolt>(), new HashMap<String, StateSpoutSpec>()), null, null,
				null, null, null, null, null, null, null, null, null, null, null, null, null)
		.setSubscribedState(null, null, null);
	}

}
