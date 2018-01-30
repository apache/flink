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

package org.apache.flink.core.memory;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;

/**
 * Unit tests for the {@link InputStreamViewWrapper}.
 */
public class InputStreamViewWrapperTest {

	private static final byte[] TEST_BYTE_ARRAY = {50, 51, 103, 125, 68, 43};

	@Test
	public void testRead() throws Exception {
		DataInputView wrappedInputView = new DataInputViewStreamWrapper(new ByteArrayInputStream(TEST_BYTE_ARRAY));
		InputStream in = new InputStreamViewWrapper(wrappedInputView);

		for (int i = 0; i < TEST_BYTE_ARRAY.length; i++) {
			Assert.assertEquals((int) TEST_BYTE_ARRAY[i], in.read());
		}
	}

	@Test
	public void testReadBytes() throws Exception {
		DataInputView wrappedInputView = new DataInputViewStreamWrapper(new ByteArrayInputStream(TEST_BYTE_ARRAY));
		InputStream in = new InputStreamViewWrapper(wrappedInputView);

		byte[] buffer = new byte[TEST_BYTE_ARRAY.length];

		Assert.assertEquals(TEST_BYTE_ARRAY.length, in.read(buffer));
		Assert.assertTrue(Arrays.equals(TEST_BYTE_ARRAY, buffer));
	}

	@Test
	public void testReadBytesToOffset() throws Exception {
		DataInputView wrappedInputView = new DataInputViewStreamWrapper(new ByteArrayInputStream(TEST_BYTE_ARRAY));
		InputStream in = new InputStreamViewWrapper(wrappedInputView);

		byte[] buffer = new byte[TEST_BYTE_ARRAY.length + 3];

		Assert.assertEquals(TEST_BYTE_ARRAY.length, in.read(buffer, 3, TEST_BYTE_ARRAY.length));

		for (int i = 0; i < 3; i++) {
			Assert.assertEquals(0, buffer[i]);
		}

		for (int i = 0; i < TEST_BYTE_ARRAY.length; i++) {
			Assert.assertEquals((int) TEST_BYTE_ARRAY[i], buffer[i + 3]);
		}
	}

	@Test
	public void testSkip() throws Exception {
		DataInputView wrappedInputView = new DataInputViewStreamWrapper(new ByteArrayInputStream(TEST_BYTE_ARRAY));
		InputStream in = new InputStreamViewWrapper(wrappedInputView);

		Assert.assertEquals(3, in.skip(3));
		Assert.assertEquals(TEST_BYTE_ARRAY.length - 3, in.skip(3000));
		Assert.assertEquals(0, in.skip(1000));
	}
}
