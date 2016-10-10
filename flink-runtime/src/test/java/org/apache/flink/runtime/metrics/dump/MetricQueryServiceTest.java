/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.runtime.metrics.dump;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.testkit.TestActorRef;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.util.TestingHistogram;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MetricQueryServiceTest extends TestLogger {
	@Test
	public void testCreateDump() throws Exception {

		ActorSystem s = AkkaUtils.createLocalActorSystem(new Configuration());
		ActorRef serviceActor = MetricQueryService.startMetricQueryService(s, null);
		TestActorRef testActorRef = TestActorRef.create(s, Props.create(TestActor.class));
		TestActor testActor = (TestActor) testActorRef.underlyingActor();

		final Counter c = new SimpleCounter();
		final Gauge<String> g = new Gauge<String>() {
			@Override
			public String getValue() {
				return "Hello";
			}
		};
		final Histogram h = new TestingHistogram();
		final Meter m = new Meter() {

			@Override
			public void markEvent() {
			}

			@Override
			public void markEvent(long n) {
			}

			@Override
			public double getRate() {
				return 5;
			}

			@Override
			public long getCount() {
				return 10;
			}
		};

		MetricRegistry registry = new MetricRegistry(MetricRegistryConfiguration.defaultMetricRegistryConfiguration());
		final TaskManagerMetricGroup tm = new TaskManagerMetricGroup(registry, "host", "id");

		MetricQueryService.notifyOfAddedMetric(serviceActor, c, "counter", tm);
		MetricQueryService.notifyOfAddedMetric(serviceActor, g, "gauge", tm);
		MetricQueryService.notifyOfAddedMetric(serviceActor, h, "histogram", tm);
		MetricQueryService.notifyOfAddedMetric(serviceActor, m, "meter", tm);

		// these metrics will be removed *after* the first query
		MetricQueryService.notifyOfRemovedMetric(serviceActor, c);
		MetricQueryService.notifyOfRemovedMetric(serviceActor, g);
		MetricQueryService.notifyOfRemovedMetric(serviceActor, h);
		MetricQueryService.notifyOfRemovedMetric(serviceActor, m);

		serviceActor.tell(MetricQueryService.getCreateDump(), testActorRef);
		synchronized (testActor.lock) {
			if (testActor.message == null) {
				testActor.lock.wait();
			}
		}

		byte[] dump = (byte[]) testActor.message;
		testActor.message = null;
		assertTrue(dump.length > 0);

		serviceActor.tell(MetricQueryService.getCreateDump(), testActorRef);
		synchronized (testActor.lock) {
			if (testActor.message == null) {
				testActor.lock.wait();
			}
		}

		byte[] emptyDump = (byte[]) testActor.message;
		testActor.message = null;
		assertEquals(16, emptyDump.length);
		for (int x = 0; x < 16; x++) {
			assertEquals(0, emptyDump[x]);
		}

		s.shutdown();
	}

	private static class TestActor extends UntypedActor {
		public Object message;
		public Object lock = new Object();

		@Override
		public void onReceive(Object message) throws Exception {
			synchronized (lock) {
				this.message = message;
				lock.notifyAll();
			}
		}
	}
}
