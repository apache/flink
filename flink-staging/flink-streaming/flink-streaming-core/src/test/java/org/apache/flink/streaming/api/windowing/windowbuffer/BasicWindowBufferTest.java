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

package org.apache.flink.streaming.api.windowing.windowbuffer;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.Test;

public class BasicWindowBufferTest {

	@Test
	public void testEmitWindow() throws Exception {

		TestOutput<StreamWindow<Integer>> collector = new TestOutput<StreamWindow<Integer>>();
		List<StreamWindow<Integer>> collected = collector.getCollected();

		WindowBuffer<Integer> wb = new BasicWindowBuffer<Integer>();

		wb.store(2);
		wb.store(10);

		wb.emitWindow(collector);

		assertEquals(1, collected.size());
		assertEquals(StreamWindow.fromElements(2, 10), collected.get(0));

		wb.store(4);
		wb.evict(2);

		wb.emitWindow(collector);

		assertEquals(2, collected.size());
		assertEquals(StreamWindow.fromElements(4), collected.get(1));

		wb.evict(1);

		wb.emitWindow(collector);
		assertEquals(2, collected.size());
	}

	public static class TestOutput<T> implements Output<StreamRecord<T>> {

		private final List<T> collected = new ArrayList<T>();

		@Override
		public void collect(StreamRecord<T> record) {
			collected.add(record.getValue());
		}

		@Override
		public void close() {
		}

		public List<T> getCollected() {
			return collected;
		}

		@Override
		public void emitWatermark(Watermark mark) {

		}
	}

}
