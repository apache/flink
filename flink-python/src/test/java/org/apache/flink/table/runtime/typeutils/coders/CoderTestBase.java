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

package org.apache.flink.table.runtime.typeutils.coders;

import org.apache.flink.testutils.CustomEqualityMatcher;
import org.apache.flink.testutils.DeeplyEqualsChecker;

import org.apache.beam.sdk.coders.Coder;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Abstract test base for coders.
 */
public abstract class CoderTestBase<T> {

	private final DeeplyEqualsChecker checker;

	protected CoderTestBase() {
		this.checker = new DeeplyEqualsChecker();
	}

	protected CoderTestBase(DeeplyEqualsChecker checker) {
		this.checker = checker;
	}

	protected abstract Coder<T> createCoder();

	protected abstract T[] getTestData();

	// --------------------------------------------------------------------------------------------

	@Test
	public void testEncodeDecode() {
		try {
			Coder<T> coder = getCoder();
			T[] testData = getData();

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			for (T value : testData) {
				coder.encode(value, baos);
			}

			ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
			int num = 0;
			while (bais.available() > 0) {
				T deserialized = coder.decode(bais);

				deepEquals("Decoded value if wrong.", testData[num], deserialized);
				num++;
			}

			assertEquals("Wrong number of elements decoded.", testData.length, num);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Exception in test: " + e.getMessage());
		}
	}

	// --------------------------------------------------------------------------------------------

	private void deepEquals(String message, T should, T is) {
		assertThat(message, is, CustomEqualityMatcher.deeplyEquals(should).withChecker(checker));
	}

	private Coder<T> getCoder() {
		Coder<T> coder = createCoder();
		if (coder == null) {
			throw new RuntimeException("Test case corrupt. Returns null as coder.");
		}
		return coder;
	}

	private T[] getData() {
		T[] data = getTestData();
		if (data == null) {
			throw new RuntimeException("Test case corrupt. Returns null as test data.");
		}
		return data;
	}
}
