/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.util.CollectorOutput;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link StreamSource} awareness of source idleness.
 */
@RunWith(Parameterized.class)
public class StreamSourceContextIdleDetectionTests {

	/** The tests in this class will be parameterized with these enumerations.*/
	private enum TestMethod {

		// test idleness detection using the {@link SourceFunction.SourceContext#collect(Object)} method
		COLLECT,

		// test idleness detection using the {@link SourceFunction.SourceContext#collectWithTimestamp(Object, long)} method
		COLLECT_WITH_TIMESTAMP,

		// test idleness detection using the {@link SourceFunction.SourceContext#emitWatermark(Watermark)} method
		EMIT_WATERMARK
	}

	private TestMethod testMethod;

	public StreamSourceContextIdleDetectionTests(TestMethod testMethod) {
		this.testMethod = testMethod;
	}

	/**
	 * Test scenario (idleTimeout = 100):
	 * (1) Start from 0 as initial time.
	 * (2) As soon as time reaches 100, status should have been toggled to IDLE.
	 * (3) After some arbitrary time (until 300), the status should remain IDLE.
	 * (4) Emit a record at 310. Status should become ACTIVE. This should fire a idleness detection at 410.
	 * (5) Emit another record at 320 (which is before the next check). This should make the idleness check pass.
	 * (6) Advance time to 410 and trigger idleness detection.
	 *     The status should still be ACTIVE due to step (5). Another idleness detection should be fired at 510.
	 * (7) Advance time to 510 and trigger idleness detection. Since no records were collected in-between the two
	 *     idleness detections, status should have been toggle back to IDLE.
	 *
	 * <p>Inline comments will refer to the corresponding tested steps in the scenario.
	 */
	@Test
	public void testManualWatermarkContext() throws Exception {
		long idleTimeout = 100;

		long initialTime = 0;
		TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
		processingTimeService.setCurrentTime(initialTime);

		final List<StreamElement> output = new ArrayList<>();

		MockStreamStatusMaintainer mockStreamStatusMaintainer = new MockStreamStatusMaintainer();

		SourceFunction.SourceContext<String> context = StreamSourceContexts.getSourceContext(
			TimeCharacteristic.EventTime,
			processingTimeService,
			new Object(),
			mockStreamStatusMaintainer,
			new CollectorOutput<String>(output),
			0,
			idleTimeout);

		// -------------------------- begin test scenario --------------------------

		// corresponds to step (2) of scenario (please see method-level Javadoc comment)
		processingTimeService.setCurrentTime(initialTime + idleTimeout);
		assertTrue(mockStreamStatusMaintainer.getStreamStatus().isIdle());

		// corresponds to step (3) of scenario (please see method-level Javadoc comment)
		processingTimeService.setCurrentTime(initialTime + 2 * idleTimeout);
		processingTimeService.setCurrentTime(initialTime + 3 * idleTimeout);
		assertTrue(mockStreamStatusMaintainer.getStreamStatus().isIdle());

		// corresponds to step (4) of scenario (please see method-level Javadoc comment)
		processingTimeService.setCurrentTime(initialTime + 3 * idleTimeout + idleTimeout / 10);
		switch (testMethod) {
			case COLLECT:
				context.collect("msg");
				break;
			case COLLECT_WITH_TIMESTAMP:
				context.collectWithTimestamp("msg", processingTimeService.getCurrentProcessingTime());
				break;
			case EMIT_WATERMARK:
				context.emitWatermark(new Watermark(processingTimeService.getCurrentProcessingTime()));
				break;
		}
		assertTrue(mockStreamStatusMaintainer.getStreamStatus().isActive());

		// corresponds to step (5) of scenario (please see method-level Javadoc comment)
		processingTimeService.setCurrentTime(initialTime + 3 * idleTimeout + 2 * idleTimeout / 10);
		switch (testMethod) {
			case COLLECT:
				context.collect("msg");
				break;
			case COLLECT_WITH_TIMESTAMP:
				context.collectWithTimestamp("msg", processingTimeService.getCurrentProcessingTime());
				break;
			case EMIT_WATERMARK:
				context.emitWatermark(new Watermark(processingTimeService.getCurrentProcessingTime()));
				break;
		}
		assertTrue(mockStreamStatusMaintainer.getStreamStatus().isActive());

		// corresponds to step (6) of scenario (please see method-level Javadoc comment)
		processingTimeService.setCurrentTime(initialTime + 4 * idleTimeout + idleTimeout / 10);
		assertTrue(mockStreamStatusMaintainer.getStreamStatus().isActive());

		// corresponds to step (7) of scenario (please see method-level Javadoc comment)
		processingTimeService.setCurrentTime(initialTime + 5 * idleTimeout + idleTimeout / 10);
		assertTrue(mockStreamStatusMaintainer.getStreamStatus().isIdle());
	}

	/**
	 * Test scenario (idleTimeout = 100, watermarkInterval = 40):
	 * (1) Start from 20 as initial time.
	 * (2) As soon as time reaches 120, status should have been toggled to IDLE.
	 * (3) After some arbitrary time (until 320), the status should remain IDLE, and no watermarks should have been emitted.
	 * (4) Emit a record at 330. Status should become ACTIVE. This should schedule a idleness detection to be fired at 430.
	 * (5) Emit another record at 350 (which is before the next check). This should make the idleness check pass.
	 * (6) Advance time to 430 and trigger idleness detection. The status should still be ACTIVE due to step (5).
	 *     This should schedule a idleness detection to be fired at 530.
	 * (7) Advance time to 460, in which a watermark emission task should be fired. Idleness detection
	 *     should have been "piggy-backed" in the task, allowing the status to be toggled to IDLE before the next
	 *     actual idle detection task at 530.
	 *
	 * <p>Inline comments will refer to the corresponding tested steps in the scenario.
	 */
	@Test
	public void testAutomaticWatermarkContext() throws Exception {
		long watermarkInterval = 40;
		long idleTimeout = 100;
		long initialTime = 20;

		TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
		processingTimeService.setCurrentTime(initialTime);

		MockStreamStatusMaintainer mockStreamStatusMaintainer = new MockStreamStatusMaintainer();

		final List<StreamElement> output = new ArrayList<>();
		final List<StreamElement> expectedOutput = new ArrayList<>();

		SourceFunction.SourceContext<String> context = StreamSourceContexts.getSourceContext(
			TimeCharacteristic.IngestionTime,
			processingTimeService,
			new Object(),
			mockStreamStatusMaintainer,
			new CollectorOutput<String>(output),
			watermarkInterval,
			idleTimeout);

		// -------------------------- begin test scenario --------------------------

		// corresponds to step (2) of scenario (please see method-level Javadoc comment)
		processingTimeService.setCurrentTime(initialTime + watermarkInterval);
		expectedOutput.add(new Watermark(processingTimeService.getCurrentProcessingTime() - (processingTimeService.getCurrentProcessingTime() % watermarkInterval)));
		processingTimeService.setCurrentTime(initialTime + 2 * watermarkInterval);
		expectedOutput.add(new Watermark(processingTimeService.getCurrentProcessingTime() - (processingTimeService.getCurrentProcessingTime() % watermarkInterval)));
		processingTimeService.setCurrentTime(initialTime + idleTimeout);
		assertTrue(mockStreamStatusMaintainer.getStreamStatus().isIdle());
		assertEquals(expectedOutput, output);

		// corresponds to step (3) of scenario (please see method-level Javadoc comment)
		processingTimeService.setCurrentTime(initialTime + 3 * watermarkInterval);
		processingTimeService.setCurrentTime(initialTime + 4 * watermarkInterval);
		processingTimeService.setCurrentTime(initialTime + 2 * idleTimeout);
		processingTimeService.setCurrentTime(initialTime + 6 * watermarkInterval);
		processingTimeService.setCurrentTime(initialTime + 7 * watermarkInterval);
		processingTimeService.setCurrentTime(initialTime + 3 * idleTimeout);
		assertTrue(mockStreamStatusMaintainer.getStreamStatus().isIdle());
		assertEquals(expectedOutput, output);

		// corresponds to step (4) of scenario (please see method-level Javadoc comment)
		processingTimeService.setCurrentTime(initialTime + 3 * idleTimeout + idleTimeout / 10);
		switch (testMethod) {
			case COLLECT:
				context.collect("msg");
				expectedOutput.add(new StreamRecord<>("msg", processingTimeService.getCurrentProcessingTime()));
				expectedOutput.add(new Watermark(processingTimeService.getCurrentProcessingTime() - (processingTimeService.getCurrentProcessingTime() % watermarkInterval)));
				assertTrue(mockStreamStatusMaintainer.getStreamStatus().isActive());
				assertEquals(expectedOutput, output);
				break;
			case COLLECT_WITH_TIMESTAMP:
				context.collectWithTimestamp("msg", processingTimeService.getCurrentProcessingTime());
				expectedOutput.add(new StreamRecord<>("msg", processingTimeService.getCurrentProcessingTime()));
				expectedOutput.add(new Watermark(processingTimeService.getCurrentProcessingTime() - (processingTimeService.getCurrentProcessingTime() % watermarkInterval)));
				assertTrue(mockStreamStatusMaintainer.getStreamStatus().isActive());
				assertEquals(expectedOutput, output);
				break;
			case EMIT_WATERMARK:
				// for emitWatermark, since the watermark will be blocked,
				// it should not make the status become active;
				// from here on, the status should remain idle for the emitWatermark variant test
				context.emitWatermark(new Watermark(processingTimeService.getCurrentProcessingTime()));
				assertTrue(mockStreamStatusMaintainer.getStreamStatus().isIdle());
				assertEquals(expectedOutput, output);
		}

		// corresponds to step (5) of scenario (please see method-level Javadoc comment)
		processingTimeService.setCurrentTime(initialTime + 8 * watermarkInterval);
		processingTimeService.setCurrentTime(initialTime + 3 * idleTimeout + 3 * idleTimeout / 10);
		switch (testMethod) {
			case COLLECT:
				context.collect("msg");
				expectedOutput.add(new StreamRecord<>("msg", processingTimeService.getCurrentProcessingTime()));
				assertTrue(mockStreamStatusMaintainer.getStreamStatus().isActive());
				assertEquals(expectedOutput, output);
				break;
			case COLLECT_WITH_TIMESTAMP:
				context.collectWithTimestamp("msg", processingTimeService.getCurrentProcessingTime());
				expectedOutput.add(new StreamRecord<>("msg", processingTimeService.getCurrentProcessingTime()));
				assertTrue(mockStreamStatusMaintainer.getStreamStatus().isActive());
				assertEquals(expectedOutput, output);
				break;
			case EMIT_WATERMARK:
				context.emitWatermark(new Watermark(processingTimeService.getCurrentProcessingTime()));
				assertTrue(mockStreamStatusMaintainer.getStreamStatus().isIdle());
				assertEquals(expectedOutput, output);
		}

		processingTimeService.setCurrentTime(initialTime + 9 * watermarkInterval);
		switch (testMethod) {
			case COLLECT:
			case COLLECT_WITH_TIMESTAMP:
				expectedOutput.add(new Watermark(processingTimeService.getCurrentProcessingTime() - (processingTimeService.getCurrentProcessingTime() % watermarkInterval)));
				assertTrue(mockStreamStatusMaintainer.getStreamStatus().isActive());
				assertEquals(expectedOutput, output);
				break;
			case EMIT_WATERMARK:
				assertTrue(mockStreamStatusMaintainer.getStreamStatus().isIdle());
				assertEquals(expectedOutput, output);
		}

		processingTimeService.setCurrentTime(initialTime + 10 * watermarkInterval);
		switch (testMethod) {
			case COLLECT:
			case COLLECT_WITH_TIMESTAMP:
				expectedOutput.add(new Watermark(processingTimeService.getCurrentProcessingTime() - (processingTimeService.getCurrentProcessingTime() % watermarkInterval)));
				assertTrue(mockStreamStatusMaintainer.getStreamStatus().isActive());
				assertEquals(expectedOutput, output);
				break;
			case EMIT_WATERMARK:
				assertTrue(mockStreamStatusMaintainer.getStreamStatus().isIdle());
				assertEquals(expectedOutput, output);
		}

		// corresponds to step (6) of scenario (please see method-level Javadoc comment)
		processingTimeService.setCurrentTime(initialTime + 4 * idleTimeout + idleTimeout / 10);
		switch (testMethod) {
			case COLLECT:
			case COLLECT_WITH_TIMESTAMP:
				assertTrue(mockStreamStatusMaintainer.getStreamStatus().isActive());
				assertEquals(expectedOutput, output);
				break;
			case EMIT_WATERMARK:
				assertTrue(mockStreamStatusMaintainer.getStreamStatus().isIdle());
				assertEquals(expectedOutput, output);
		}

		// corresponds to step (7) of scenario (please see method-level Javadoc comment)
		processingTimeService.setCurrentTime(initialTime + 11 * watermarkInterval);
		assertTrue(mockStreamStatusMaintainer.getStreamStatus().isIdle());
		assertEquals(expectedOutput, output);
	}

	private static class MockStreamStatusMaintainer implements StreamStatusMaintainer {
		StreamStatus currentStreamStatus = StreamStatus.ACTIVE;

		@Override
		public void toggleStreamStatus(StreamStatus streamStatus) {
			if (!currentStreamStatus.equals(streamStatus)) {
				currentStreamStatus = streamStatus;
			}
		}

		@Override
		public StreamStatus getStreamStatus() {
			return currentStreamStatus;
		}
	}

	@Parameterized.Parameters(name = "TestMethod = {0}")
	@SuppressWarnings("unchecked")
	public static Collection<TestMethod[]> timeCharacteristic(){
		return Arrays.asList(
			new TestMethod[]{TestMethod.COLLECT},
			new TestMethod[]{TestMethod.COLLECT_WITH_TIMESTAMP},
			new TestMethod[]{TestMethod.EMIT_WATERMARK});
	}
}
