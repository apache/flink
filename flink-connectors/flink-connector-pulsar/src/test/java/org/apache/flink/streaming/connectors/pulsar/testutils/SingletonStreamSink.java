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

package org.apache.flink.streaming.connectors.pulsar.testutils;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test results collector.
 */
public class SingletonStreamSink {
	public static final List<String> SINKED_RESULTS = new ArrayList<>();

	public static void clear() {
		SINKED_RESULTS.clear();
	}

	/**
	 * Collector sink for string.
	 */
	public static final class StringSink<T> extends RichSinkFunction<T> {
		@Override
		public void invoke(T value, Context context) throws Exception {
			synchronized (SINKED_RESULTS) {
				SINKED_RESULTS.add(value.toString());
			}
		}
	}

	public static void compareWithList(List<String> expected) {
		expected.sort(null);
		SingletonStreamSink.SINKED_RESULTS.sort(null);
		assertEquals(expected, SINKED_RESULTS);
	}
}
