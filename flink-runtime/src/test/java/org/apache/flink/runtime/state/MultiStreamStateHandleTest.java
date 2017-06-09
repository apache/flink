/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class MultiStreamStateHandleTest {

	private static final int TEST_DATA_LENGTH = 123;
	private Random random;
	private byte[] testData;
	private List<StreamStateHandle> streamStateHandles;

	@Before
	public void setup() {
		random = new Random(0x42);
		testData = new byte[TEST_DATA_LENGTH];
		for (int i = 0; i < testData.length; ++i) {
			testData[i] = (byte) i;
		}

		int idx = 0;
		streamStateHandles = new ArrayList<>();
		while (idx < testData.length) {
			int len = random.nextInt(5);
			byte[] sub = Arrays.copyOfRange(testData, idx, idx + len);
			streamStateHandles.add(new ByteStreamStateHandle(String.valueOf(idx), sub));
			idx += len;
		}
	}

	@Test
	public void testMetaData() throws IOException {
		MultiStreamStateHandle multiStreamStateHandle = new MultiStreamStateHandle(streamStateHandles);
		assertEquals(TEST_DATA_LENGTH, multiStreamStateHandle.getStateSize());
	}

	@Test
	public void testLinearRead() throws IOException {
		MultiStreamStateHandle multiStreamStateHandle = new MultiStreamStateHandle(streamStateHandles);
		try (FSDataInputStream in = multiStreamStateHandle.openInputStream()) {

			for (int i = 0; i < TEST_DATA_LENGTH; ++i) {
				assertEquals(i, in.getPos());
				assertEquals(testData[i], in.read());
			}

			assertEquals(-1, in.read());
			assertEquals(TEST_DATA_LENGTH, in.getPos());
			assertEquals(-1, in.read());
			assertEquals(TEST_DATA_LENGTH, in.getPos());
		}
	}

	@Test
	public void testRandomRead() throws IOException {

		MultiStreamStateHandle multiStreamStateHandle = new MultiStreamStateHandle(streamStateHandles);

		try (FSDataInputStream in = multiStreamStateHandle.openInputStream()) {

			for (int i = 0; i < 1000; ++i) {
				int pos = random.nextInt(TEST_DATA_LENGTH);
				int readLen = random.nextInt(TEST_DATA_LENGTH);
				in.seek(pos);
				while (--readLen > 0 && pos < TEST_DATA_LENGTH) {
					assertEquals(pos, in.getPos());
					assertEquals(testData[pos++], in.read());
				}
			}

			in.seek(TEST_DATA_LENGTH);
			assertEquals(TEST_DATA_LENGTH, in.getPos());
			assertEquals(-1, in.read());

			try {
				in.seek(TEST_DATA_LENGTH + 1);
				fail();
			} catch (Exception ignored) {

			}
		}
	}

	@Test
	public void testEmptyList() throws IOException {

		MultiStreamStateHandle multiStreamStateHandle =
				new MultiStreamStateHandle(Collections.<StreamStateHandle>emptyList());

		try (FSDataInputStream in = multiStreamStateHandle.openInputStream()) {

			assertEquals(0, in.getPos());
			in.seek(0);
			assertEquals(0, in.getPos());
			assertEquals(-1, in.read());

			try {
				in.seek(1);
				fail();
			} catch (Exception ignored) {

			}
		}
	}
}