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
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.storm.util.AbstractTest;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import clojure.lang.Atom;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StateSpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.metric.api.CountMetric;
import org.apache.storm.metric.api.ICombiner;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.metric.api.IReducer;
import org.apache.storm.metric.api.MultiCountMetric;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * FlinkTopologyContext.getSources(componentId) and FlinkTopologyContext.getTargets(componentId) are not tested here,
 * because those are tested in StormWrapperSetupHelperTest.
 */
public class FlinkTopologyContextTest extends AbstractTest {

	@Test(expected = UnsupportedOperationException.class)
	public void testAddTaskHook() {
		new FlinkTopologyContext(null, new StormTopology(new HashMap<String, SpoutSpec>(),
				new HashMap<String, Bolt>(), new HashMap<String, StateSpoutSpec>()), null, null,
				null, null, null, null, null, null, null, null, null, null, null, null, null)
		.addTaskHook(null);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testGetHooks() {
		new FlinkTopologyContext(null, new StormTopology(new HashMap<String, SpoutSpec>(),
				new HashMap<String, Bolt>(), new HashMap<String, StateSpoutSpec>()), null, null,
				null, null, null, null, null, null, null, null, null, null, null, null, null)
		.getHooks();
	}

	@SuppressWarnings("rawtypes")
	@Test(expected = UnsupportedOperationException.class)
	public void testRegisteredMetric1() {
		new FlinkTopologyContext(null, new StormTopology(new HashMap<String, SpoutSpec>(),
				new HashMap<String, Bolt>(), new HashMap<String, StateSpoutSpec>()), null, null,
				null, null, null, null, null, null, null, null, null, null, null, null, null)
		.registerMetric(null, (ICombiner) null, 0);
	}

	@SuppressWarnings("rawtypes")
	@Test(expected = UnsupportedOperationException.class)
	public void testRegisteredMetric2() {
		new FlinkTopologyContext(null, new StormTopology(new HashMap<String, SpoutSpec>(),
				new HashMap<String, Bolt>(), new HashMap<String, StateSpoutSpec>()), null, null,
				null, null, null, null, null, null, null, null, null, null, null, null, null)
		.registerMetric(null, (IReducer) null, 0);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testRegisteredMetric3() {
		new FlinkTopologyContext(null, new StormTopology(new HashMap<String, SpoutSpec>(),
				new HashMap<String, Bolt>(), new HashMap<String, StateSpoutSpec>()), null, null,
				null, null, null, null, null, null, null, null, null, null, null, null, null)
		.registerMetric(null, (IMetric) null, 0);
	}

	private static class MockRuntimeContext extends StreamingRuntimeContext {
		MockMetricGroup mmg = new MockMetricGroup();;
		private MockRuntimeContext() {
			super(
				new MockStreamOperator(),
				new MockEnvironment(
					"mockTask",
					4 * MemoryManager.DEFAULT_PAGE_SIZE,
					null,
					16,
					new TestTaskStateManager()),
					Collections.<String, Accumulator<?, ?>>emptyMap());
		}

		@Override
		public MetricGroup getMetricGroup() {
			return mmg;
		}

		@Override
		public boolean isCheckpointingEnabled() {
			return false;
		}

		@Override
		public int getIndexOfThisSubtask() {
			return 0;
		}

		@Override
		public int getNumberOfParallelSubtasks() {
			return 0;
		}

		// ------------------------------------------------------------------------

		public static class MockStreamOperator extends AbstractStreamOperator<Integer> {
			private static final long serialVersionUID = -1153976702711944427L;

			@Override
			public ExecutionConfig getExecutionConfig() {
				return new ExecutionConfig();
			}
		}

		public static class MockMetricGroup implements MetricGroup {
			private Map<String, Counter> counters = new HashMap<String, Counter>();

			@Override
			public Counter counter(int name) {
				Counter c = new SimpleCounter();
				counters.put(String.valueOf(name), c);
				return c;
			}

			@Override
			public Counter counter(String name) {
				Counter c = new SimpleCounter();
				counters.put(name, c);
				return c;
			}

			@Override
			public <C extends Counter> C counter(int name, C counter) {
				return null;
			}

			@Override
			public <C extends Counter> C counter(String name, C counter) {
				return null;
			}

			@Override
			public <T, G extends Gauge<T>> G gauge(int name, G gauge) {
				return null;
			}

			@Override
			public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
				return null;
			}

			@Override
			public <H extends Histogram> H histogram(String name, H histogram) {
				return null;
			}

			@Override
			public <H extends Histogram> H histogram(int name, H histogram) {
				return null;
			}

			@Override
			public <M extends Meter> M meter(String name, M meter) {
				return null;
			}

			@Override
			public <M extends Meter> M meter(int name, M meter) {
				return null;
			}

			@Override
			public MetricGroup addGroup(int name) {
				return null;
			}

			@Override
			public MetricGroup addGroup(String name) {
				return null;
			}

			@Override
			public MetricGroup addGroup(String key, String value) {
				return null;
			}

			@Override
			public String[] getScopeComponents() {
				return null;
			}

			@Override
			public Map<String, String> getAllVariables() {
				return null;
			}

			@Override
			public String getMetricIdentifier(String metricName) {
				return null;
			}

			@Override
			public String getMetricIdentifier(String metricName, CharacterFilter filter) {
				return null;
			}

			public Counter getCounter(String name) {
				return counters.get(name);
			}

			public Map<String, Counter> getCounters() {
				return counters;
			}
		}
	}

	@Test
	public void testRegisteredMetric_count() {
		StreamingRuntimeContext streamContext = new MockRuntimeContext();
		CountMetric cm = new FlinkTopologyContext(streamContext, new StormTopology(new HashMap<String, SpoutSpec>(),
				new HashMap<String, Bolt>(), new HashMap<String, StateSpoutSpec>()), null, null,
				null, null, null, null, null, null, null, null, null, null, null, new HashMap<Integer, Map<Integer, Map<String, IMetric>>>(), new Atom(new Boolean(false)))
		.registerMetric("test-count", new CountMetric(), 1);

		Counter counter = ((org.apache.flink.storm.wrappers.FlinkTopologyContextTest.MockRuntimeContext.MockMetricGroup) streamContext.getMetricGroup()).getCounter("test-count");
		Assert.assertNotNull(counter);
		cm.incr();
		Assert.assertEquals(1, counter.getCount());
		cm.incrBy(2);
		Assert.assertEquals(3, counter.getCount());
	}

	@Test
	public void testRegisteredMetric_MultiCount() {
		StreamingRuntimeContext streamContext = new MockRuntimeContext();
		MultiCountMetric multiCount = new FlinkTopologyContext(streamContext, new StormTopology(new HashMap<String, SpoutSpec>(),
				new HashMap<String, Bolt>(), new HashMap<String, StateSpoutSpec>()), null, null,
				null, null, null, null, null, null, null, null, null, null, null, new HashMap<Integer, Map<Integer, Map<String, IMetric>>>(), new Atom(new Boolean(false)))
			.registerMetric("test-multi-count", new MultiCountMetric(), 1);
		CountMetric cm = multiCount.scope("count1");

		System.out.println(streamContext.getMetricGroup().getClass().getCanonicalName());
		org.apache.flink.storm.wrappers.FlinkTopologyContextTest.MockRuntimeContext.MockMetricGroup mmg = (org.apache.flink.storm.wrappers.FlinkTopologyContextTest.MockRuntimeContext.MockMetricGroup) streamContext.getMetricGroup();
		System.out.println(mmg.getCounters().size());
		Counter counter = mmg.getCounter("count1");
		Assert.assertNotNull(counter);
		cm.incr();
		Assert.assertEquals(1, counter.getCount());
		cm.incrBy(2);
		Assert.assertEquals(3, counter.getCount());
	}

	@Test
	public void testGetRegisteredMetricByName() {
		org.junit.Assert.assertNull(new FlinkTopologyContext(null, new StormTopology(new HashMap<String, SpoutSpec>(),
				new HashMap<String, Bolt>(), new HashMap<String, StateSpoutSpec>()), null, null,
				null, null, null, null, null, null, null, null, null, null, null, new HashMap<Integer, Map<Integer, Map<String, IMetric>>>(), new Atom(new Boolean(false)))
		.getRegisteredMetricByName(null));
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testSetAllSubscribedState() {
		new FlinkTopologyContext(null, new StormTopology(new HashMap<String, SpoutSpec>(),
				new HashMap<String, Bolt>(), new HashMap<String, StateSpoutSpec>()), null, null,
				null, null, null, null, null, null, null, null, null, null, null, null, null)
		.setAllSubscribedState(null);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testSetSubscribedState1() {
		new FlinkTopologyContext(null, new StormTopology(new HashMap<String, SpoutSpec>(),
				new HashMap<String, Bolt>(), new HashMap<String, StateSpoutSpec>()), null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)
		.setSubscribedState(null, null);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testSetSubscribedState2() {
		new FlinkTopologyContext(null, new StormTopology(new HashMap<String, SpoutSpec>(),
				new HashMap<String, Bolt>(), new HashMap<String, StateSpoutSpec>()), null, null,
				null, null, null, null, null, null, null, null, null, null, null, null, null)
		.setSubscribedState(null, null, null);
	}

}
