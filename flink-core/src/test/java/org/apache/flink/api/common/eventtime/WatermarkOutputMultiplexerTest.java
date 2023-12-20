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

package org.apache.flink.api.common.eventtime;

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.apache.flink.api.common.eventtime.WatermarkMatchers.watermark;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for the {@link WatermarkOutputMultiplexer}. */
class WatermarkOutputMultiplexerTest {

    @Test
    void singleImmediateWatermark() {
        TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
        WatermarkOutputMultiplexer multiplexer =
                new WatermarkOutputMultiplexer(underlyingWatermarkOutput);

        WatermarkOutput watermarkOutput = createImmediateOutput(multiplexer);

        watermarkOutput.emitWatermark(new Watermark(0));

        MatcherAssert.assertThat(underlyingWatermarkOutput.lastWatermark(), is(watermark(0)));
        MatcherAssert.assertThat(underlyingWatermarkOutput.isIdle(), is(false));
    }

    @Test
    void singleImmediateIdleness() {
        TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
        WatermarkOutputMultiplexer multiplexer =
                new WatermarkOutputMultiplexer(underlyingWatermarkOutput);

        WatermarkOutput watermarkOutput = createImmediateOutput(multiplexer);

        watermarkOutput.markIdle();

        MatcherAssert.assertThat(underlyingWatermarkOutput.lastWatermark(), nullValue());
        MatcherAssert.assertThat(underlyingWatermarkOutput.isIdle(), is(true));
    }

    @Test
    void singleImmediateWatermarkAfterIdleness() {
        TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
        WatermarkOutputMultiplexer multiplexer =
                new WatermarkOutputMultiplexer(underlyingWatermarkOutput);

        WatermarkOutput watermarkOutput = createImmediateOutput(multiplexer);

        watermarkOutput.markIdle();
        MatcherAssert.assertThat(underlyingWatermarkOutput.isIdle(), is(true));

        watermarkOutput.emitWatermark(new Watermark(0));

        MatcherAssert.assertThat(underlyingWatermarkOutput.lastWatermark(), is(watermark(0)));
        MatcherAssert.assertThat(underlyingWatermarkOutput.isIdle(), is(false));
    }

    @Test
    void multipleImmediateWatermark() {
        TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
        WatermarkOutputMultiplexer multiplexer =
                new WatermarkOutputMultiplexer(underlyingWatermarkOutput);

        WatermarkOutput watermarkOutput1 = createImmediateOutput(multiplexer);
        WatermarkOutput watermarkOutput2 = createImmediateOutput(multiplexer);
        WatermarkOutput watermarkOutput3 = createImmediateOutput(multiplexer);

        watermarkOutput1.emitWatermark(new Watermark(2));
        watermarkOutput2.emitWatermark(new Watermark(5));
        watermarkOutput3.markIdle();

        MatcherAssert.assertThat(underlyingWatermarkOutput.lastWatermark(), is(watermark(2)));
        MatcherAssert.assertThat(underlyingWatermarkOutput.isIdle(), is(false));
    }

    @Test
    void whenImmediateOutputBecomesIdleWatermarkAdvances() {
        TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
        WatermarkOutputMultiplexer multiplexer =
                new WatermarkOutputMultiplexer(underlyingWatermarkOutput);

        WatermarkOutput watermarkOutput1 = createImmediateOutput(multiplexer);
        WatermarkOutput watermarkOutput2 = createImmediateOutput(multiplexer);

        watermarkOutput1.emitWatermark(new Watermark(2));
        watermarkOutput2.emitWatermark(new Watermark(5));

        MatcherAssert.assertThat(underlyingWatermarkOutput.lastWatermark(), is(watermark(2)));

        watermarkOutput1.markIdle();

        MatcherAssert.assertThat(underlyingWatermarkOutput.lastWatermark(), is(watermark(5)));
    }

    @Test
    void combinedWatermarkDoesNotRegressWhenIdleOutputRegresses() {
        TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
        WatermarkOutputMultiplexer multiplexer =
                new WatermarkOutputMultiplexer(underlyingWatermarkOutput);

        WatermarkOutput watermarkOutput1 = createImmediateOutput(multiplexer);
        WatermarkOutput watermarkOutput2 = createImmediateOutput(multiplexer);

        watermarkOutput1.emitWatermark(new Watermark(2));
        watermarkOutput2.emitWatermark(new Watermark(5));
        watermarkOutput1.markIdle();

        MatcherAssert.assertThat(underlyingWatermarkOutput.lastWatermark(), is(watermark(5)));

        watermarkOutput1.emitWatermark(new Watermark(3));

        MatcherAssert.assertThat(underlyingWatermarkOutput.lastWatermark(), is(watermark(5)));
    }

    /**
     * This test makes sure that we don't output any update if there are zero outputs. Due to how
     * aggregation of deferred updates in the KafkaConsumer works we had a bug there that caused a
     * Long.MAX_VALUE watermark to be emitted in case of zero partitions.
     *
     * <p>Additionally it verifies that the combined output is not IDLE during the initial phase
     * when there are no splits assigned and the combined watermark is at its initial value.
     */
    @Test
    void noCombinedDeferredUpdateWhenWeHaveZeroOutputs() {
        TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
        WatermarkOutputMultiplexer multiplexer =
                new WatermarkOutputMultiplexer(underlyingWatermarkOutput);

        multiplexer.onPeriodicEmit();

        MatcherAssert.assertThat(underlyingWatermarkOutput.lastWatermark(), is(nullValue()));
        MatcherAssert.assertThat(underlyingWatermarkOutput.isIdle(), is(false));
    }

    @Test
    void deferredOutputDoesNotImmediatelyAdvanceWatermark() {
        TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
        WatermarkOutputMultiplexer multiplexer =
                new WatermarkOutputMultiplexer(underlyingWatermarkOutput);

        WatermarkOutput watermarkOutput1 = createDeferredOutput(multiplexer);
        WatermarkOutput watermarkOutput2 = createDeferredOutput(multiplexer);

        watermarkOutput1.emitWatermark(new Watermark(0));
        watermarkOutput2.emitWatermark(new Watermark(1));

        MatcherAssert.assertThat(underlyingWatermarkOutput.lastWatermark(), nullValue());

        multiplexer.onPeriodicEmit();

        MatcherAssert.assertThat(underlyingWatermarkOutput.lastWatermark(), is(watermark(0)));
    }

    @Test
    void singleDeferredWatermark() {
        TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
        WatermarkOutputMultiplexer multiplexer =
                new WatermarkOutputMultiplexer(underlyingWatermarkOutput);

        WatermarkOutput watermarkOutput = createDeferredOutput(multiplexer);

        watermarkOutput.emitWatermark(new Watermark(0));
        multiplexer.onPeriodicEmit();

        MatcherAssert.assertThat(underlyingWatermarkOutput.lastWatermark(), is(watermark(0)));
        MatcherAssert.assertThat(underlyingWatermarkOutput.isIdle(), is(false));
    }

    @Test
    void singleDeferredIdleness() {
        TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
        WatermarkOutputMultiplexer multiplexer =
                new WatermarkOutputMultiplexer(underlyingWatermarkOutput);

        WatermarkOutput watermarkOutput = createDeferredOutput(multiplexer);

        watermarkOutput.markIdle();
        multiplexer.onPeriodicEmit();

        MatcherAssert.assertThat(underlyingWatermarkOutput.lastWatermark(), nullValue());
        MatcherAssert.assertThat(underlyingWatermarkOutput.isIdle(), is(true));
    }

    @Test
    void singleDeferredWatermarkAfterIdleness() {
        TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
        WatermarkOutputMultiplexer multiplexer =
                new WatermarkOutputMultiplexer(underlyingWatermarkOutput);

        WatermarkOutput watermarkOutput = createDeferredOutput(multiplexer);

        watermarkOutput.markIdle();
        multiplexer.onPeriodicEmit();

        MatcherAssert.assertThat(underlyingWatermarkOutput.isIdle(), is(true));

        watermarkOutput.emitWatermark(new Watermark(0));
        multiplexer.onPeriodicEmit();

        MatcherAssert.assertThat(underlyingWatermarkOutput.lastWatermark(), is(watermark(0)));
        MatcherAssert.assertThat(underlyingWatermarkOutput.isIdle(), is(false));
    }

    @Test
    void multipleDeferredWatermark() {
        TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
        WatermarkOutputMultiplexer multiplexer =
                new WatermarkOutputMultiplexer(underlyingWatermarkOutput);

        WatermarkOutput watermarkOutput1 = createDeferredOutput(multiplexer);
        WatermarkOutput watermarkOutput2 = createDeferredOutput(multiplexer);
        WatermarkOutput watermarkOutput3 = createDeferredOutput(multiplexer);

        watermarkOutput1.emitWatermark(new Watermark(2));
        watermarkOutput2.emitWatermark(new Watermark(5));
        watermarkOutput3.markIdle();

        multiplexer.onPeriodicEmit();

        MatcherAssert.assertThat(underlyingWatermarkOutput.lastWatermark(), is(watermark(2)));
        MatcherAssert.assertThat(underlyingWatermarkOutput.isIdle(), is(false));
    }

    @Test
    void immediateUpdatesTakeDeferredUpdatesIntoAccount() {
        TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
        WatermarkOutputMultiplexer multiplexer =
                new WatermarkOutputMultiplexer(underlyingWatermarkOutput);

        WatermarkOutput immediateOutput = createImmediateOutput(multiplexer);
        WatermarkOutput deferredOutput = createDeferredOutput(multiplexer);

        deferredOutput.emitWatermark(new Watermark(5));

        MatcherAssert.assertThat(underlyingWatermarkOutput.lastWatermark(), is(nullValue()));

        immediateOutput.emitWatermark(new Watermark(2));

        MatcherAssert.assertThat(underlyingWatermarkOutput.lastWatermark(), is(watermark(2)));
    }

    @Test
    void immediateUpdateOnSameOutputAsDeferredUpdateDoesNotRegress() {
        TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
        WatermarkOutputMultiplexer multiplexer =
                new WatermarkOutputMultiplexer(underlyingWatermarkOutput);

        final String id = "test-id";
        multiplexer.registerNewOutput(id, watermark -> {});
        WatermarkOutput immediateOutput = multiplexer.getImmediateOutput(id);
        WatermarkOutput deferredOutput = multiplexer.getDeferredOutput(id);

        deferredOutput.emitWatermark(new Watermark(5));
        multiplexer.onPeriodicEmit();

        MatcherAssert.assertThat(underlyingWatermarkOutput.lastWatermark(), is(watermark(5)));

        immediateOutput.emitWatermark(new Watermark(2));

        MatcherAssert.assertThat(underlyingWatermarkOutput.lastWatermark(), is(watermark(5)));

        multiplexer.onPeriodicEmit();
        MatcherAssert.assertThat(underlyingWatermarkOutput.lastWatermark(), is(watermark(5)));
    }

    @Test
    void lowerImmediateUpdateOnSameOutputDoesNotEmitCombinedUpdate() {
        TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
        WatermarkOutputMultiplexer multiplexer =
                new WatermarkOutputMultiplexer(underlyingWatermarkOutput);

        final String id = "1234-test";
        multiplexer.registerNewOutput(id, watermark -> {});
        WatermarkOutput immediateOutput = multiplexer.getImmediateOutput(id);
        WatermarkOutput deferredOutput = multiplexer.getDeferredOutput(id);

        deferredOutput.emitWatermark(new Watermark(5));
        immediateOutput.emitWatermark(new Watermark(2));

        MatcherAssert.assertThat(underlyingWatermarkOutput.lastWatermark(), is(nullValue()));
    }

    @Test
    void testRemoveUnblocksWatermarks() {
        final TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
        final WatermarkOutputMultiplexer multiplexer =
                new WatermarkOutputMultiplexer(underlyingWatermarkOutput);
        final long lowTimestamp = 156765L;
        final long highTimestamp = lowTimestamp + 10;

        multiplexer.registerNewOutput("lower", watermark -> {});
        multiplexer.registerNewOutput("higher", watermark -> {});
        multiplexer.getImmediateOutput("lower").emitWatermark(new Watermark(lowTimestamp));

        multiplexer.unregisterOutput("lower");
        multiplexer.getImmediateOutput("higher").emitWatermark(new Watermark(highTimestamp));

        assertEquals(highTimestamp, underlyingWatermarkOutput.lastWatermark().getTimestamp());
    }

    @Test
    void testRemoveOfLowestDoesNotImmediatelyAdvanceWatermark() {
        final TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
        final WatermarkOutputMultiplexer multiplexer =
                new WatermarkOutputMultiplexer(underlyingWatermarkOutput);
        final long lowTimestamp = -4343L;
        final long highTimestamp = lowTimestamp + 10;

        multiplexer.registerNewOutput("lower", watermark -> {});
        multiplexer.registerNewOutput("higher", watermark -> {});
        multiplexer.getImmediateOutput("lower").emitWatermark(new Watermark(lowTimestamp));
        multiplexer.getImmediateOutput("higher").emitWatermark(new Watermark(highTimestamp));

        multiplexer.unregisterOutput("lower");

        assertEquals(lowTimestamp, underlyingWatermarkOutput.lastWatermark().getTimestamp());
    }

    @Test
    void testRemoveOfHighestDoesNotRetractWatermark() {
        final TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
        final WatermarkOutputMultiplexer multiplexer =
                new WatermarkOutputMultiplexer(underlyingWatermarkOutput);
        final long lowTimestamp = 1L;
        final long highTimestamp = 2L;

        multiplexer.registerNewOutput("higher", watermark -> {});
        multiplexer.getImmediateOutput("higher").emitWatermark(new Watermark(highTimestamp));
        multiplexer.unregisterOutput("higher");

        multiplexer.registerNewOutput("lower", watermark -> {});
        multiplexer.getImmediateOutput("lower").emitWatermark(new Watermark(lowTimestamp));

        assertEquals(highTimestamp, underlyingWatermarkOutput.lastWatermark().getTimestamp());
    }

    @Test
    void testRemoveRegisteredReturnValue() {
        final TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
        final WatermarkOutputMultiplexer multiplexer =
                new WatermarkOutputMultiplexer(underlyingWatermarkOutput);
        multiplexer.registerNewOutput("does-exist", watermark -> {});

        final boolean unregistered = multiplexer.unregisterOutput("does-exist");

        assertThat(unregistered).isTrue();
    }

    @Test
    void testRemoveNotRegisteredReturnValue() {
        final TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
        final WatermarkOutputMultiplexer multiplexer =
                new WatermarkOutputMultiplexer(underlyingWatermarkOutput);

        final boolean unregistered = multiplexer.unregisterOutput("does-not-exist");

        assertThat(unregistered).isFalse();
    }

    @Test
    void testNotEmittingIdleAfterAllSplitsRemoved() {
        final TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
        final WatermarkOutputMultiplexer multiplexer =
                new WatermarkOutputMultiplexer(underlyingWatermarkOutput);

        Watermark emittedWatermark = new Watermark(1);
        final String id = UUID.randomUUID().toString();
        multiplexer.registerNewOutput(id, watermark -> {});
        WatermarkOutput immediateOutput = multiplexer.getImmediateOutput(id);
        immediateOutput.emitWatermark(emittedWatermark);
        multiplexer.unregisterOutput(id);

        multiplexer.onPeriodicEmit();
        MatcherAssert.assertThat(
                underlyingWatermarkOutput.lastWatermark(), equalTo(emittedWatermark));
        MatcherAssert.assertThat(underlyingWatermarkOutput.isIdle(), equalTo(false));
    }

    /**
     * Convenience method so we don't have to go through the output ID dance when we only want an
     * immediate output for a given output ID.
     */
    private static WatermarkOutput createImmediateOutput(WatermarkOutputMultiplexer multiplexer) {
        final String id = UUID.randomUUID().toString();
        multiplexer.registerNewOutput(id, watermark -> {});
        return multiplexer.getImmediateOutput(id);
    }

    /**
     * Convenience method so we don't have to go through the output ID dance when we only want an
     * deferred output for a given output ID.
     */
    private static WatermarkOutput createDeferredOutput(WatermarkOutputMultiplexer multiplexer) {
        final String id = UUID.randomUUID().toString();
        multiplexer.registerNewOutput(id, watermark -> {});
        return multiplexer.getDeferredOutput(id);
    }

    private static TestingWatermarkOutput createTestingWatermarkOutput() {
        return new TestingWatermarkOutput();
    }
}
