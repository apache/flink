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

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Record;
import org.apache.flink.types.Value;
import org.apache.flink.util.TestLogger;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class ValueTypeInfoTest extends TestLogger {

	public static class TestClass implements Value {
		private static final long serialVersionUID = -492760806806568285L;

		@Override
		public void write(DataOutputView out) throws IOException {

		}

		@Override
		public void read(DataInputView in) throws IOException {

		}
	}

	public static class AlternativeClass implements Value {

		private static final long serialVersionUID = -163437084575260172L;

		@Override
		public void write(DataOutputView out) throws IOException {

		}

		@Override
		public void read(DataInputView in) throws IOException {

		}
	}

	@Test
	public void testValueTypeInfoEquality() {
		ValueTypeInfo<TestClass> tpeInfo1 = new ValueTypeInfo<>(TestClass.class);
		ValueTypeInfo<TestClass> tpeInfo2 = new ValueTypeInfo<>(TestClass.class);

		assertEquals(tpeInfo1, tpeInfo2);
		assertEquals(tpeInfo1.hashCode(), tpeInfo2.hashCode());
	}

	@Test
	public void testValueTyepInfoInequality() {
		ValueTypeInfo<TestClass> tpeInfo1 = new ValueTypeInfo<>(TestClass.class);
		ValueTypeInfo<AlternativeClass> tpeInfo2 = new ValueTypeInfo<>(AlternativeClass.class);

		assertNotEquals(tpeInfo1, tpeInfo2);
	}

	@Test
	public void testValueTypeEqualsWithNull() throws Exception {
		ValueTypeInfo<Record> tpeInfo = new ValueTypeInfo<>(Record.class);

		Assert.assertFalse(tpeInfo.equals(null));
	}
}
