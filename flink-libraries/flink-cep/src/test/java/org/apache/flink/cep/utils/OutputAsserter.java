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

package org.apache.flink.cep.utils;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import java.util.Queue;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Asserter for output from {@link OneInputStreamOperatorTestHarness}.
 */
public class OutputAsserter {

	private final Queue<?> output;

	private OutputAsserter(Queue<?> output) {
		this.output = output;
	}

	public static OutputAsserter assertOutput(Queue<?> output) {
		return new OutputAsserter(output);
	}

	private AssertionError fail(Object record) {
		return new AssertionError("Received unexpected element: " + record);
	}

	public <T> OutputAsserter nextElementEquals(T expected) {
		final Object record = output.poll();
		final Object actual;
		if (record instanceof StreamRecord) {
			// This is in case we assert side output
			actual = ((StreamRecord) record).getValue();
		} else {
			// This is in case we assert the main output
			actual = record;
		}
		assertThat(actual, is(expected));
		return this;
	}

	public void hasNoMoreElements() {
		assertTrue(output.isEmpty());
	}

	public OutputAsserter watermarkEquals(long timestamp) {
		Object record = output.poll();
		if (record instanceof Watermark) {
			Watermark watermark = (Watermark) record;
			assertThat(watermark.getTimestamp(), is(timestamp));
		} else {
			throw fail(record);
		}
		return this;
	}
}
