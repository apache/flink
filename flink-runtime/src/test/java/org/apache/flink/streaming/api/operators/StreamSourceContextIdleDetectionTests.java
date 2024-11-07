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

import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.CollectorOutput;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link StreamSource} awareness of source idleness. */
@ExtendWith(ParameterizedTestExtension.class)
class StreamSourceContextIdleDetectionTests {

    /** The tests in this class will be parameterized with these enumerations. */
    private enum TestMethod {

        // test idleness detection using the {@link SourceFunction.SourceContext#collect(Object)}
        // method
        COLLECT,

        // test idleness detection using the {@link
        // SourceFunction.SourceContext#collectWithTimestamp(Object, long)} method
        COLLECT_WITH_TIMESTAMP,

        // test idleness detection using the {@link
        // SourceFunction.SourceContext#emitWatermark(Watermark)} method
        EMIT_WATERMARK
    }

    private TestMethod testMethod;

    public StreamSourceContextIdleDetectionTests(TestMethod testMethod) {
        this.testMethod = testMethod;
    }

    /**
     * Test scenario (idleTimeout = 100): (1) Start from 0 as initial time. (2) As soon as time
     * reaches 100, status should have been toggled to IDLE. (3) After some arbitrary time (until
     * 300), the status should remain IDLE. (4) Emit a record at 310. Status should become ACTIVE.
     * This should fire a idleness detection at 410. (5) Emit another record at 320 (which is before
     * the next check). This should make the idleness check pass. (6) Advance time to 410 and
     * trigger idleness detection. The status should still be ACTIVE due to step (5). Another
     * idleness detection should be fired at 510. (7) Advance time to 510 and trigger idleness
     * detection. Since no records were collected in-between the two idleness detections, status
     * should have been toggle back to IDLE.
     *
     * <p>Inline comments will refer to the corresponding tested steps in the scenario.
     */
    @TestTemplate
    void testManualWatermarkContext() throws Exception {
        long idleTimeout = 100;

        long initialTime = 0;
        TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
        processingTimeService.setCurrentTime(initialTime);

        final List<StreamElement> output = new ArrayList<>();
        final List<StreamElement> expectedOutput = new ArrayList<>();

        SourceFunction.SourceContext<String> context =
                StreamSourceContexts.getSourceContext(
                        processingTimeService,
                        new Object(),
                        new CollectorOutput<>(output),
                        0,
                        idleTimeout,
                        true);

        // -------------------------- begin test scenario --------------------------

        // corresponds to step (2) of scenario (please see method-level Javadoc comment)
        processingTimeService.setCurrentTime(initialTime + idleTimeout);
        expectedOutput.add(WatermarkStatus.IDLE);
        assertThat(output).isEqualTo(expectedOutput);

        // corresponds to step (3) of scenario (please see method-level Javadoc comment)
        processingTimeService.setCurrentTime(initialTime + 2 * idleTimeout);
        processingTimeService.setCurrentTime(initialTime + 3 * idleTimeout);
        assertThat(output).isEqualTo(expectedOutput);

        // corresponds to step (4) of scenario (please see method-level Javadoc comment)
        expectedOutput.add(WatermarkStatus.ACTIVE);
        emitStreamElement(
                initialTime + 3 * idleTimeout + idleTimeout / 10,
                expectedOutput,
                processingTimeService,
                context);
        assertThat(output).isEqualTo(expectedOutput);

        // corresponds to step (5) of scenario (please see method-level Javadoc comment)
        emitStreamElement(
                initialTime + 3 * idleTimeout + 2 * idleTimeout / 10,
                expectedOutput,
                processingTimeService,
                context);
        assertThat(output).isEqualTo(expectedOutput);

        // corresponds to step (6) of scenario (please see method-level Javadoc comment)
        processingTimeService.setCurrentTime(initialTime + 4 * idleTimeout + idleTimeout / 10);
        assertThat(output).isEqualTo(expectedOutput);

        // corresponds to step (7) of scenario (please see method-level Javadoc comment)
        processingTimeService.setCurrentTime(initialTime + 5 * idleTimeout + idleTimeout / 10);
        expectedOutput.add(WatermarkStatus.IDLE);
        assertThat(output).isEqualTo(expectedOutput);
    }

    private void emitStreamElement(
            long currentTime,
            List<StreamElement> expectedOutput,
            TestProcessingTimeService processingTimeService,
            SourceFunction.SourceContext<String> context)
            throws Exception {
        processingTimeService.setCurrentTime(currentTime);
        switch (testMethod) {
            case COLLECT:
                expectedOutput.add(new StreamRecord<>("msg"));
                context.collect("msg");
                break;
            case COLLECT_WITH_TIMESTAMP:
                long recordTime = processingTimeService.getCurrentProcessingTime();
                expectedOutput.add(new StreamRecord<>("msg", recordTime));
                context.collectWithTimestamp("msg", recordTime);
                break;
            case EMIT_WATERMARK:
                long watermarkTime = processingTimeService.getCurrentProcessingTime();
                expectedOutput.add(new Watermark(watermarkTime));
                context.emitWatermark(new Watermark(watermarkTime));
                break;
        }
    }

    @Parameters(name = "TestMethod = {0}")
    @SuppressWarnings("unchecked")
    private static Collection<TestMethod[]> timeCharacteristic() {
        return Arrays.asList(
                new TestMethod[] {TestMethod.COLLECT},
                new TestMethod[] {TestMethod.COLLECT_WITH_TIMESTAMP},
                new TestMethod[] {TestMethod.EMIT_WATERMARK});
    }
}
