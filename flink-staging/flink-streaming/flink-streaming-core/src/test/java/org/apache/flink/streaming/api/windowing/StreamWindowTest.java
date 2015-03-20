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

package org.apache.flink.streaming.api.windowing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.streaming.api.windowing.StreamWindowSerializer;
import org.junit.Test;

public class StreamWindowTest {

	@Test
	public void creationTest() {

		StreamWindow<Integer> window1 = new StreamWindow<Integer>();
		assertTrue(window1.isEmpty());
		assertTrue(window1.windowID != 0);

		window1.add(10);
		assertEquals(1, window1.size());

		StreamWindow<Integer> window2 = new StreamWindow<Integer>(window1);

		assertTrue(window1.windowID == window2.windowID);
		assertEquals(1, window2.size());

		StreamWindow<Integer> window3 = new StreamWindow<Integer>(100);
		assertEquals(100, window3.windowID);

		StreamWindow<Integer> window4 = new StreamWindow<Integer>();
		assertFalse(window4.windowID == window1.windowID);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void mergeTest() throws IOException {
		StreamWindow<Integer> window1 = new StreamWindow<Integer>().setNumberOfParts(3);
		StreamWindow<Integer> window2 = new StreamWindow<Integer>(window1.windowID, 3);
		StreamWindow<Integer> window3 = new StreamWindow<Integer>(window1.windowID, 3);

		window1.add(1);
		window2.add(2);
		window3.add(3);

		Set<Integer> values = new HashSet<Integer>();
		values.add(1);
		values.add(2);
		values.add(3);

		StreamWindow<Integer> merged = StreamWindow.merge(window1, window2, window3);

		assertEquals(3, merged.size());
		assertEquals(window1.windowID, merged.windowID);
		assertEquals(values, new HashSet<Integer>(merged));

		try {
			StreamWindow.merge(window1, new StreamWindow<Integer>());
			fail();
		} catch (RuntimeException e) {
			// good
		}

		List<StreamWindow<Integer>> wList = StreamWindow.split(merged,3);

		StreamWindow<Integer> merged2 = StreamWindow.merge(wList);

		assertEquals(3, merged2.size());
		assertEquals(window1.windowID, merged2.windowID);
		assertEquals(values, new HashSet<Integer>(merged2));

	}

	@Test
	public void serializerTest() throws IOException {

		StreamWindow<Integer> streamWindow = new StreamWindow<Integer>();
		streamWindow.add(1);
		streamWindow.add(2);
		streamWindow.add(3);

		TypeSerializer<StreamWindow<Integer>> ts = new StreamWindowSerializer<Integer>(
				BasicTypeInfo.INT_TYPE_INFO, null);

		TestOutputView ow = new TestOutputView();

		ts.serialize(streamWindow, ow);

		TestInputView iw = ow.getInputView();

		assertEquals(streamWindow, ts.deserialize(iw));

	}

	@Test
	public void partitionTest() {
		StreamWindow<Integer> streamWindow = new StreamWindow<Integer>();
		streamWindow.add(1);
		streamWindow.add(2);
		streamWindow.add(3);
		streamWindow.add(4);
		streamWindow.add(5);
		streamWindow.add(6);

		List<StreamWindow<Integer>> split = StreamWindow.split(streamWindow,2);
		assertEquals(2, split.size());
		assertEquals(StreamWindow.fromElements(1, 2, 3), split.get(0));
		assertEquals(StreamWindow.fromElements(4, 5, 6), split.get(1));

		List<StreamWindow<Integer>> split2 = StreamWindow.split(streamWindow,6);
		assertEquals(6, split2.size());
		assertEquals(StreamWindow.fromElements(1), split2.get(0));
		assertEquals(StreamWindow.fromElements(2), split2.get(1));
		assertEquals(StreamWindow.fromElements(3), split2.get(2));
		assertEquals(StreamWindow.fromElements(4), split2.get(3));
		assertEquals(StreamWindow.fromElements(5), split2.get(4));
		assertEquals(StreamWindow.fromElements(6), split2.get(5));

		List<StreamWindow<Integer>> split3 = StreamWindow.split(streamWindow,10);
		assertEquals(6, split3.size());
		assertEquals(StreamWindow.fromElements(1), split3.get(0));
		assertEquals(StreamWindow.fromElements(2), split3.get(1));
		assertEquals(StreamWindow.fromElements(3), split3.get(2));
		assertEquals(StreamWindow.fromElements(4), split3.get(3));
		assertEquals(StreamWindow.fromElements(5), split3.get(4));
		assertEquals(StreamWindow.fromElements(6), split3.get(5));

	}

	private class TestOutputView extends DataOutputStream implements DataOutputView {

		public TestOutputView() {
			super(new ByteArrayOutputStream(4096));
		}

		public TestInputView getInputView() {
			ByteArrayOutputStream baos = (ByteArrayOutputStream) out;
			return new TestInputView(baos.toByteArray());
		}

		@Override
		public void skipBytesToWrite(int numBytes) throws IOException {
			for (int i = 0; i < numBytes; i++) {
				write(0);
			}
		}

		@Override
		public void write(DataInputView source, int numBytes) throws IOException {
			byte[] buffer = new byte[numBytes];
			source.readFully(buffer);
			write(buffer);
		}
	}

	private class TestInputView extends DataInputStream implements DataInputView {

		public TestInputView(byte[] data) {
			super(new ByteArrayInputStream(data));
		}

		@Override
		public void skipBytesToRead(int numBytes) throws IOException {
			while (numBytes > 0) {
				int skipped = skipBytes(numBytes);
				numBytes -= skipped;
			}
		}
	}
}
