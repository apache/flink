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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.util.TestHistogram;
import org.apache.flink.metrics.util.TestMeter;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.util.TestLogger;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.testkit.TestActorRef;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link MetricQueryService}.
 */
public class MetricQueryServiceTest extends TestLogger {
	@Test
	public void testCreateDump() throws Exception {
		ActorSystem s = AkkaUtils.createLocalActorSystem(new Configuration());
		try {
			ActorRef serviceActor = MetricQueryService.startMetricQueryService(s, null, Long.MAX_VALUE);
			TestActorRef testActorRef = TestActorRef.create(s, Props.create(TestActor.class));
			TestActor testActor = (TestActor) testActorRef.underlyingActor();

			final Counter c = new SimpleCounter();
			final Gauge<String> g = () -> "Hello";
			final Histogram h = new TestHistogram();
			final Meter m = new TestMeter();

			final TaskManagerMetricGroup tm = UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup();

			MetricQueryService.notifyOfAddedMetric(serviceActor, c, "counter", tm);
			MetricQueryService.notifyOfAddedMetric(serviceActor, g, "gauge", tm);
			MetricQueryService.notifyOfAddedMetric(serviceActor, h, "histogram", tm);
			MetricQueryService.notifyOfAddedMetric(serviceActor, m, "meter", tm);
			serviceActor.tell(MetricQueryService.getCreateDump(), testActorRef);

			testActor.waitForResult();

			MetricDumpSerialization.MetricSerializationResult dump = testActor.getSerializationResult();

			assertTrue(dump.serializedCounters.length > 0);
			assertTrue(dump.serializedGauges.length > 0);
			assertTrue(dump.serializedHistograms.length > 0);
			assertTrue(dump.serializedMeters.length > 0);

			MetricQueryService.notifyOfRemovedMetric(serviceActor, c);
			MetricQueryService.notifyOfRemovedMetric(serviceActor, g);
			MetricQueryService.notifyOfRemovedMetric(serviceActor, h);
			MetricQueryService.notifyOfRemovedMetric(serviceActor, m);

			serviceActor.tell(MetricQueryService.getCreateDump(), testActorRef);

			testActor.waitForResult();

			MetricDumpSerialization.MetricSerializationResult emptyDump = testActor.getSerializationResult();

			assertEquals(0, emptyDump.serializedCounters.length);
			assertEquals(0, emptyDump.serializedGauges.length);
			assertEquals(0, emptyDump.serializedHistograms.length);
			assertEquals(0, emptyDump.serializedMeters.length);
		} finally {
			s.terminate();
		}
	}

	@Test
	public void testHandleOversizedMetricMessage() throws Exception {
		ActorSystem s = AkkaUtils.createLocalActorSystem(new Configuration());
		try {
			final long sizeLimit = 200L;
			ActorRef serviceActor = MetricQueryService.startMetricQueryService(s, null, sizeLimit);
			TestActorRef testActorRef = TestActorRef.create(s, Props.create(TestActor.class));
			TestActor testActor = (TestActor) testActorRef.underlyingActor();

			final TaskManagerMetricGroup tm = UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup();

			final String gaugeValue = "Hello";
			final long requiredGaugesToExceedLimit = sizeLimit / gaugeValue.length() + 1;
			List<Tuple2<String, Gauge<String>>> gauges = LongStream.range(0, requiredGaugesToExceedLimit)
				.mapToObj(x -> Tuple2.of("gauge" + x, (Gauge<String>) () -> "Hello" + x))
				.collect(Collectors.toList());
			gauges.forEach(gauge -> MetricQueryService.notifyOfAddedMetric(serviceActor, gauge.f1, gauge.f0, tm));

			MetricQueryService.notifyOfAddedMetric(serviceActor, new SimpleCounter(), "counter", tm);
			MetricQueryService.notifyOfAddedMetric(serviceActor, new TestHistogram(), "histogram", tm);
			MetricQueryService.notifyOfAddedMetric(serviceActor, new TestMeter(), "meter", tm);

			serviceActor.tell(MetricQueryService.getCreateDump(), testActorRef);
			testActor.waitForResult();

			MetricDumpSerialization.MetricSerializationResult dump = testActor.getSerializationResult();

			assertTrue(dump.serializedCounters.length > 0);
			assertEquals(1, dump.numCounters);
			assertTrue(dump.serializedMeters.length > 0);
			assertEquals(1, dump.numMeters);

			// gauges exceeded the size limit and will be excluded
			assertEquals(0, dump.serializedGauges.length);
			assertEquals(0, dump.numGauges);

			assertTrue(dump.serializedHistograms.length > 0);
			assertEquals(1, dump.numHistograms);

			// unregister all but one gauge to ensure gauges are reported again if the remaining fit
			for (int x = 1; x < gauges.size(); x++) {
				MetricQueryService.notifyOfRemovedMetric(serviceActor, gauges.get(x).f1);
			}

			serviceActor.tell(MetricQueryService.getCreateDump(), testActorRef);
			testActor.waitForResult();

			MetricDumpSerialization.MetricSerializationResult recoveredDump = testActor.getSerializationResult();

			assertTrue(recoveredDump.serializedCounters.length > 0);
			assertEquals(1, recoveredDump.numCounters);
			assertTrue(recoveredDump.serializedMeters.length > 0);
			assertEquals(1, recoveredDump.numMeters);
			assertTrue(recoveredDump.serializedGauges.length > 0);
			assertEquals(1, recoveredDump.numGauges);
			assertTrue(recoveredDump.serializedHistograms.length > 0);
			assertEquals(1, recoveredDump.numHistograms);
		} finally {
			s.terminate();
		}
	}

	private static class TestActor extends UntypedActor {
		private Object message;
		private final Object lock = new Object();

		@Override
		public void onReceive(Object message) throws Exception {
			synchronized (lock) {
				this.message = message;
				lock.notifyAll();
			}
		}

		void waitForResult() throws InterruptedException {
			synchronized (lock) {
				if (message == null) {
					lock.wait();
				}
			}
		}

		MetricDumpSerialization.MetricSerializationResult getSerializationResult() {
			final MetricDumpSerialization.MetricSerializationResult result = (MetricDumpSerialization.MetricSerializationResult) message;
			message = null;
			return result;
		}
	}
}
