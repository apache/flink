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

import static org.junit.Assert.*;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.InstantiationUtil;
import org.junit.Test;

import java.io.IOException;

public class PojoTypeInfoTest {

	@Test
	public void testPojoTypeInfoEquality() {
		try {
			TypeInformation<TestPojo> info1 = TypeExtractor.getForClass(TestPojo.class);
			TypeInformation<TestPojo> info2 = TypeExtractor.getForClass(TestPojo.class);
			
			assertTrue(info1 instanceof PojoTypeInfo);
			assertTrue(info2 instanceof PojoTypeInfo);
			
			assertTrue(info1.equals(info2));
			assertTrue(info1.hashCode() == info2.hashCode());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testPojoTypeInfoInequality() {
		try {
			TypeInformation<TestPojo> info1 = TypeExtractor.getForClass(TestPojo.class);
			TypeInformation<AlternatePojo> info2 = TypeExtractor.getForClass(AlternatePojo.class);

			assertTrue(info1 instanceof PojoTypeInfo);
			assertTrue(info2 instanceof PojoTypeInfo);

			assertFalse(info1.equals(info2));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testSerializabilityOfPojoTypeInfo() throws IOException, ClassNotFoundException {
		PojoTypeInfo<TestPojo> pojoTypeInfo = (PojoTypeInfo<TestPojo>)TypeExtractor.getForClass(TestPojo.class);

		byte[] serializedPojoTypeInfo = InstantiationUtil.serializeObject(pojoTypeInfo);
		PojoTypeInfo<TestPojo> deserializedPojoTypeInfo = (PojoTypeInfo<TestPojo>)InstantiationUtil.deserializeObject(
			serializedPojoTypeInfo,
			getClass().getClassLoader());

		assertEquals(pojoTypeInfo, deserializedPojoTypeInfo);
	}

	@Test
	public void testPrimitivePojo() {
		TypeInformation<PrimitivePojo> info1 = TypeExtractor.getForClass(PrimitivePojo.class);

		assertTrue(info1 instanceof PojoTypeInfo);
	}

	@Test
	public void testUnderscorePojo() {
		TypeInformation<UnderscorePojo> info1 = TypeExtractor.getForClass(UnderscorePojo.class);

		assertTrue(info1 instanceof PojoTypeInfo);
	}

	public static final class TestPojo {
		
		public int someInt;

		private String aString;
		
		public Double[] doubleArray;
		
		
		public void setaString(String aString) {
			this.aString = aString;
		}
		
		public String getaString() {
			return aString;
		}
	}

	public static final class AlternatePojo {

		public int someInt;

		private String aString;

		public Double[] doubleArray;


		public void setaString(String aString) {
			this.aString = aString;
		}

		public String getaString() {
			return aString;
		}
	}

	public static final class PrimitivePojo {

		private int someInt;

		public void setSomeInt(Integer someInt) {
			this.someInt = someInt;
		}

		public Integer getSomeInt() {
			return this.someInt;
		}
	}

	public static final class UnderscorePojo {

		private int some_int;

		public void setSomeInt(int some_int) {
			this.some_int = some_int;
		}

		public Integer getSomeInt() {
			return this.some_int;
		}
	}
}
