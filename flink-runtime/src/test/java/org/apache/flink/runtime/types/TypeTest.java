/**
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


package org.apache.flink.runtime.types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.types.FileRecord;
import org.apache.flink.runtime.types.IntegerRecord;
import org.junit.Test;

/**
 * This class contains test which check the correct serialization/deserialization of Nephele's built-in data types.
 * 
 */
public class TypeTest {

	/**
	 * Tests the serialization/deserialization of the {@link FileRecord} class.
	 */
	@Test
	public void testFileRecord() {

		final FileRecord orig = new FileRecord("Test Filename");
		final byte[] data = new byte[128];

		orig.append(data, 0, data.length);
		orig.append(data, 0, data.length);

		assertEquals(orig.getDataBuffer().length, 2 * data.length);

		try {
			final FileRecord copy = (FileRecord) CommonTestUtils.createCopy(orig);

			assertEquals(orig.getFileName(), copy.getFileName());
			assertEquals(orig, copy);
			assertEquals(orig.hashCode(), copy.hashCode());

		} catch (IOException ioe) {
			fail(ioe.getMessage());
		}

	}

	/**
	 * Tests the serialization/deserialization of the {@link IntegerRecord} class.
	 */
	@Test
	public void testIntegerRecord() {

		final IntegerRecord orig = new IntegerRecord(12);

		try {

			final IntegerRecord copy = (IntegerRecord) CommonTestUtils.createCopy(orig);

			assertEquals(orig.getValue(), copy.getValue());
			assertEquals(orig, copy);
			assertEquals(orig.hashCode(), copy.hashCode());

		} catch (IOException ioe) {
			fail(ioe.getMessage());
		}

	}

	
}
