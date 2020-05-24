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

import org.junit.Test;

import java.util.UUID;

import static org.apache.flink.api.common.eventtime.WatermarkMatchers.watermark;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link WatermarkOutputMultiplexer}.
 */
public class WatermarkOutputMultiplexerTest {

	@Test
	public void singleImmediateWatermark() {
		TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
		WatermarkOutputMultiplexer multiplexer =
				new WatermarkOutputMultiplexer(underlyingWatermarkOutput);

		WatermarkOutput watermarkOutput = createImmediateOutput(multiplexer);

		watermarkOutput.emitWatermark(new Watermark(0));

		assertThat(underlyingWatermarkOutput.lastWatermark(), is(watermark(0)));
		assertThat(underlyingWatermarkOutput.isIdle(), is(false));
	}

	@Test
	public void singleImmediateIdleness() {
		TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
		WatermarkOutputMultiplexer multiplexer =
				new WatermarkOutputMultiplexer(underlyingWatermarkOutput);

		WatermarkOutput watermarkOutput = createImmediateOutput(multiplexer);

		watermarkOutput.markIdle();

		assertThat(underlyingWatermarkOutput.lastWatermark(), nullValue());
		assertThat(underlyingWatermarkOutput.isIdle(), is(true));
	}

	@Test
	public void singleImmediateWatermarkAfterIdleness() {
		TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
		WatermarkOutputMultiplexer multiplexer =
				new WatermarkOutputMultiplexer(underlyingWatermarkOutput);

		WatermarkOutput watermarkOutput = createImmediateOutput(multiplexer);

		watermarkOutput.markIdle();
		assertThat(underlyingWatermarkOutput.isIdle(), is(true));

		watermarkOutput.emitWatermark(new Watermark(0));

		assertThat(underlyingWatermarkOutput.lastWatermark(), is(watermark(0)));
		assertThat(underlyingWatermarkOutput.isIdle(), is(false));
	}

	@Test
	public void multipleImmediateWatermark() {
		TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
		WatermarkOutputMultiplexer multiplexer =
				new WatermarkOutputMultiplexer(underlyingWatermarkOutput);

		WatermarkOutput watermarkOutput1 = createImmediateOutput(multiplexer);
		WatermarkOutput watermarkOutput2 = createImmediateOutput(multiplexer);
		WatermarkOutput watermarkOutput3 = createImmediateOutput(multiplexer);

		watermarkOutput1.emitWatermark(new Watermark(2));
		watermarkOutput2.emitWatermark(new Watermark(5));
		watermarkOutput3.markIdle();

		assertThat(underlyingWatermarkOutput.lastWatermark(), is(watermark(2)));
		assertThat(underlyingWatermarkOutput.isIdle(), is(false));
	}

	@Test
	public void whenImmediateOutputBecomesIdleWatermarkAdvances() {
		TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
		WatermarkOutputMultiplexer multiplexer =
				new WatermarkOutputMultiplexer(underlyingWatermarkOutput);

		WatermarkOutput watermarkOutput1 = createImmediateOutput(multiplexer);
		WatermarkOutput watermarkOutput2 = createImmediateOutput(multiplexer);

		watermarkOutput1.emitWatermark(new Watermark(2));
		watermarkOutput2.emitWatermark(new Watermark(5));

		assertThat(underlyingWatermarkOutput.lastWatermark(), is(watermark(2)));

		watermarkOutput1.markIdle();

		assertThat(underlyingWatermarkOutput.lastWatermark(), is(watermark(5)));
	}

	@Test
	public void combinedWatermarkDoesNotRegressWhenIdleOutputRegresses() {
		TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
		WatermarkOutputMultiplexer multiplexer =
				new WatermarkOutputMultiplexer(underlyingWatermarkOutput);

		WatermarkOutput watermarkOutput1 = createImmediateOutput(multiplexer);
		WatermarkOutput watermarkOutput2 = createImmediateOutput(multiplexer);

		watermarkOutput1.emitWatermark(new Watermark(2));
		watermarkOutput2.emitWatermark(new Watermark(5));
		watermarkOutput1.markIdle();

		assertThat(underlyingWatermarkOutput.lastWatermark(), is(watermark(5)));

		watermarkOutput1.emitWatermark(new Watermark(3));

		assertThat(underlyingWatermarkOutput.lastWatermark(), is(watermark(5)));
	}

	/**
	 * This test makes sure that we don't output any update if there are zero outputs. Due to how
	 * aggregation of deferred updates in the KafkaConsumer works we had a bug there that caused a
	 * Long.MAX_VALUE watermark to be emitted in case of zero partitions.
	 */
	@Test
	public void noCombinedDeferredUpdateWhenWeHaveZeroOutputs() {
		TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
		WatermarkOutputMultiplexer multiplexer =
				new WatermarkOutputMultiplexer(underlyingWatermarkOutput);

		multiplexer.onPeriodicEmit();

		assertThat(underlyingWatermarkOutput.lastWatermark(), is(nullValue()));
	}

	@Test
	public void deferredOutputDoesNotImmediatelyAdvanceWatermark() {
		TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
		WatermarkOutputMultiplexer multiplexer =
				new WatermarkOutputMultiplexer(underlyingWatermarkOutput);

		WatermarkOutput watermarkOutput1 = createDeferredOutput(multiplexer);
		WatermarkOutput watermarkOutput2 = createDeferredOutput(multiplexer);

		watermarkOutput1.emitWatermark(new Watermark(0));
		watermarkOutput2.emitWatermark(new Watermark(1));

		assertThat(underlyingWatermarkOutput.lastWatermark(), nullValue());

		multiplexer.onPeriodicEmit();

		assertThat(underlyingWatermarkOutput.lastWatermark(), is(watermark(0)));
	}

	@Test
	public void singleDeferredWatermark() {
		TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
		WatermarkOutputMultiplexer multiplexer =
				new WatermarkOutputMultiplexer(underlyingWatermarkOutput);

		WatermarkOutput watermarkOutput = createDeferredOutput(multiplexer);

		watermarkOutput.emitWatermark(new Watermark(0));
		multiplexer.onPeriodicEmit();

		assertThat(underlyingWatermarkOutput.lastWatermark(), is(watermark(0)));
		assertThat(underlyingWatermarkOutput.isIdle(), is(false));
	}

	@Test
	public void singleDeferredIdleness() {
		TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
		WatermarkOutputMultiplexer multiplexer =
				new WatermarkOutputMultiplexer(underlyingWatermarkOutput);

		WatermarkOutput watermarkOutput = createDeferredOutput(multiplexer);

		watermarkOutput.markIdle();
		multiplexer.onPeriodicEmit();

		assertThat(underlyingWatermarkOutput.lastWatermark(), nullValue());
		assertThat(underlyingWatermarkOutput.isIdle(), is(true));
	}

	@Test
	public void singleDeferredWatermarkAfterIdleness() {
		TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
		WatermarkOutputMultiplexer multiplexer =
				new WatermarkOutputMultiplexer(underlyingWatermarkOutput);

		WatermarkOutput watermarkOutput = createDeferredOutput(multiplexer);

		watermarkOutput.markIdle();
		multiplexer.onPeriodicEmit();

		assertThat(underlyingWatermarkOutput.isIdle(), is(true));

		watermarkOutput.emitWatermark(new Watermark(0));
		multiplexer.onPeriodicEmit();

		assertThat(underlyingWatermarkOutput.lastWatermark(), is(watermark(0)));
		assertThat(underlyingWatermarkOutput.isIdle(), is(false));
	}

	@Test
	public void multipleDeferredWatermark() {
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

		assertThat(underlyingWatermarkOutput.lastWatermark(), is(watermark(2)));
		assertThat(underlyingWatermarkOutput.isIdle(), is(false));
	}

	@Test
	public void immediateUpdatesTakeDeferredUpdatesIntoAccount() {
		TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
		WatermarkOutputMultiplexer multiplexer =
				new WatermarkOutputMultiplexer(underlyingWatermarkOutput);

		WatermarkOutput immediateOutput = createImmediateOutput(multiplexer);
		WatermarkOutput deferredOutput = createDeferredOutput(multiplexer);

		deferredOutput.emitWatermark(new Watermark(5));

		assertThat(underlyingWatermarkOutput.lastWatermark(), is(nullValue()));

		immediateOutput.emitWatermark(new Watermark(2));

		assertThat(underlyingWatermarkOutput.lastWatermark(), is(watermark(2)));
	}

	@Test
	public void immediateUpdateOnSameOutputAsDeferredUpdateDoesNotRegress() {
		TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
		WatermarkOutputMultiplexer multiplexer =
				new WatermarkOutputMultiplexer(underlyingWatermarkOutput);

		final String id = "test-id";
		multiplexer.registerNewOutput(id);
		WatermarkOutput immediateOutput = multiplexer.getImmediateOutput(id);
		WatermarkOutput deferredOutput = multiplexer.getDeferredOutput(id);

		deferredOutput.emitWatermark(new Watermark(5));
		multiplexer.onPeriodicEmit();

		assertThat(underlyingWatermarkOutput.lastWatermark(), is(watermark(5)));

		immediateOutput.emitWatermark(new Watermark(2));

		assertThat(underlyingWatermarkOutput.lastWatermark(), is(watermark(5)));

		multiplexer.onPeriodicEmit();
		assertThat(underlyingWatermarkOutput.lastWatermark(), is(watermark(5)));
	}

	@Test
	public void lowerImmediateUpdateOnSameOutputDoesNotEmitCombinedUpdate() {
		TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
		WatermarkOutputMultiplexer multiplexer =
				new WatermarkOutputMultiplexer(underlyingWatermarkOutput);

		final String id = "1234-test";
		multiplexer.registerNewOutput(id);
		WatermarkOutput immediateOutput = multiplexer.getImmediateOutput(id);
		WatermarkOutput deferredOutput = multiplexer.getDeferredOutput(id);

		deferredOutput.emitWatermark(new Watermark(5));
		immediateOutput.emitWatermark(new Watermark(2));

		assertThat(underlyingWatermarkOutput.lastWatermark(), is(nullValue()));
	}

	@Test
	public void testRemoveUnblocksWatermarks() {
		final TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
		final WatermarkOutputMultiplexer multiplexer = new WatermarkOutputMultiplexer(underlyingWatermarkOutput);
		final long lowTimestamp = 156765L;
		final long highTimestamp = lowTimestamp + 10;

		multiplexer.registerNewOutput("lower");
		multiplexer.registerNewOutput("higher");
		multiplexer.getImmediateOutput("lower").emitWatermark(new Watermark(lowTimestamp));

		multiplexer.unregisterOutput("lower");
		multiplexer.getImmediateOutput("higher").emitWatermark(new Watermark(highTimestamp));

		assertEquals(highTimestamp, underlyingWatermarkOutput.lastWatermark().getTimestamp());
	}

	@Test
	public void testRemoveOfLowestDoesNotImmediatelyAdvanceWatermark() {
		final TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
		final WatermarkOutputMultiplexer multiplexer = new WatermarkOutputMultiplexer(underlyingWatermarkOutput);
		final long lowTimestamp = -4343L;
		final long highTimestamp = lowTimestamp + 10;

		multiplexer.registerNewOutput("lower");
		multiplexer.registerNewOutput("higher");
		multiplexer.getImmediateOutput("lower").emitWatermark(new Watermark(lowTimestamp));
		multiplexer.getImmediateOutput("higher").emitWatermark(new Watermark(highTimestamp));

		multiplexer.unregisterOutput("lower");

		assertEquals(lowTimestamp, underlyingWatermarkOutput.lastWatermark().getTimestamp());
	}

	@Test
	public void testRemoveOfHighestDoesNotRetractWatermark() {
		final TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
		final WatermarkOutputMultiplexer multiplexer = new WatermarkOutputMultiplexer(underlyingWatermarkOutput);
		final long lowTimestamp = 1L;
		final long highTimestamp = 2L;

		multiplexer.registerNewOutput("higher");
		multiplexer.getImmediateOutput("higher").emitWatermark(new Watermark(highTimestamp));
		multiplexer.unregisterOutput("higher");

		multiplexer.registerNewOutput("lower");
		multiplexer.getImmediateOutput("lower").emitWatermark(new Watermark(lowTimestamp));

		assertEquals(highTimestamp, underlyingWatermarkOutput.lastWatermark().getTimestamp());
	}

	@Test
	public void testRemoveRegisteredReturnValue() {
		final TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
		final WatermarkOutputMultiplexer multiplexer = new WatermarkOutputMultiplexer(underlyingWatermarkOutput);
		multiplexer.registerNewOutput("does-exist");

		final boolean unregistered = multiplexer.unregisterOutput("does-exist");

		assertTrue(unregistered);
	}

	@Test
	public void testRemoveNotRegisteredReturnValue() {
		final TestingWatermarkOutput underlyingWatermarkOutput = createTestingWatermarkOutput();
		final WatermarkOutputMultiplexer multiplexer = new WatermarkOutputMultiplexer(underlyingWatermarkOutput);

		final boolean unregistered = multiplexer.unregisterOutput("does-not-exist");

		assertFalse(unregistered);
	}

	/**
	 * Convenience method so we don't have to go through the output ID dance when we only want an
	 * immediate output for a given output ID.
	 */
	private static WatermarkOutput createImmediateOutput(WatermarkOutputMultiplexer multiplexer) {
		final String id = UUID.randomUUID().toString();
		multiplexer.registerNewOutput(id);
		return multiplexer.getImmediateOutput(id);
	}

	/**
	 * Convenience method so we don't have to go through the output ID dance when we only want an
	 * deferred output for a given output ID.
	 */
	private static WatermarkOutput createDeferredOutput(WatermarkOutputMultiplexer multiplexer) {
		final String id = UUID.randomUUID().toString();
		multiplexer.registerNewOutput(id);
		return multiplexer.getDeferredOutput(id);
	}

	private static TestingWatermarkOutput createTestingWatermarkOutput() {
		return new TestingWatermarkOutput();
	}
}
