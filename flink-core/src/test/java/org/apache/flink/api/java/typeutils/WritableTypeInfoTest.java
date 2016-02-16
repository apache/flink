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

package org.apache.flink.api.java.typeutils;

import org.apache.flink.util.TestLogger;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.junit.Assert.*;

public class WritableTypeInfoTest extends TestLogger {

	public static class TestClass implements Writable {
		@Override
		public void write(DataOutput dataOutput) throws IOException {

		}

		@Override
		public void readFields(DataInput dataInput) throws IOException {

		}
	}

	public static class AlternateClass implements Writable {
		@Override
		public void write(DataOutput dataOutput) throws IOException {

		}

		@Override
		public void readFields(DataInput dataInput) throws IOException {

		}
	}


	@Test
	public void testWritableTypeInfoEquality() {
		WritableTypeInfo<TestClass> tpeInfo1 = new WritableTypeInfo<>(TestClass.class);
		WritableTypeInfo<TestClass> tpeInfo2 = new WritableTypeInfo<>(TestClass.class);

		assertEquals(tpeInfo1, tpeInfo2);
		assertEquals(tpeInfo1.hashCode(), tpeInfo2.hashCode());
	}

	@Test
	public void testWritableTypeInfoInequality() {
		WritableTypeInfo<TestClass> tpeInfo1 = new WritableTypeInfo<>(TestClass.class);
		WritableTypeInfo<AlternateClass> tpeInfo2 = new WritableTypeInfo<>(AlternateClass.class);

		assertNotEquals(tpeInfo1, tpeInfo2);
	}
}
