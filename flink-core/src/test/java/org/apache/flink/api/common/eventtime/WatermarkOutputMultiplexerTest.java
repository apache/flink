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

import static org.apache.flink.api.common.eventtime.WatermarkMatchers.watermark;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

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

		int outputId = multiplexer.registerNewOutput();
		WatermarkOutput immediateOutput = multiplexer.getImmediateOutput(outputId);
		WatermarkOutput deferredOutput = multiplexer.getDeferredOutput(outputId);

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

		int outputId = multiplexer.registerNewOutput();
		WatermarkOutput immediateOutput = multiplexer.getImmediateOutput(outputId);
		WatermarkOutput deferredOutput = multiplexer.getDeferredOutput(outputId);

		deferredOutput.emitWatermark(new Watermark(5));
		immediateOutput.emitWatermark(new Watermark(2));

		assertThat(underlyingWatermarkOutput.lastWatermark(), is(nullValue()));
	}

	/**
	 * Convenience method so we don't have to go through the output ID dance when we only want an
	 * immediate output for a given output ID.
	 */
	private static WatermarkOutput createImmediateOutput(WatermarkOutputMultiplexer multiplexer) {
		int outputId = multiplexer.registerNewOutput();
		return multiplexer.getImmediateOutput(outputId);
	}

	/**
	 * Convenience method so we don't have to go through the output ID dance when we only want an
	 * deferred output for a given output ID.
	 */
	private static WatermarkOutput createDeferredOutput(WatermarkOutputMultiplexer multiplexer) {
		int outputId = multiplexer.registerNewOutput();
		return multiplexer.getDeferredOutput(outputId);
	}

	private static TestingWatermarkOutput createTestingWatermarkOutput() {
		return new TestingWatermarkOutput();
	}
}
