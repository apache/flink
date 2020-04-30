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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;

import org.junit.Test;

import java.util.Objects;
import java.util.Random;

/**
 * Testing the serialization of classes which are subclasses of a class that implements an interface.
 */
public class SubclassFromInterfaceSerializerTest extends SerializerTestBase<SubclassFromInterfaceSerializerTest.TestUserInterface> {
	private TypeInformation<TestUserInterface> type = TypeExtractor.getForClass(TestUserInterface.class);

	@Override
	protected TypeSerializer<TestUserInterface> createSerializer() {
		// only register one of the two child classes
		ExecutionConfig conf = new ExecutionConfig();
		conf.registerPojoType(TestUserClass2.class);
		TypeSerializer<TestUserInterface> serializer = type.createSerializer(conf);
		assert(serializer instanceof KryoSerializer);
		return serializer;
	}

	@Override
	protected int getLength() {
		return -1;
	}

	@Override
	protected Class<TestUserInterface> getTypeClass() {
		return TestUserInterface.class;
	}

	@Override
	protected TestUserInterface[] getTestData() {
		Random rnd = new Random(874597969123412341L);

		return new TestUserInterface[]{
				new TestUserClass1(rnd.nextInt(), "foo", rnd.nextLong()),
				new TestUserClass2(rnd.nextInt(), "bar", rnd.nextFloat())
		};

	}

	@Override
	@Test
	public void testInstantiate() {
		// don't do anything, since the PojoSerializer with subclass will return null
	}

	public interface TestUserInterface {}

	// User code class for testing the serializer
	public static class TestUserClassBase implements TestUserInterface {
		public int dumm1;
		public String dumm2;


		public TestUserClassBase() {
		}

		public TestUserClassBase(int dumm1, String dumm2) {
			this.dumm1 = dumm1;
			this.dumm2 = dumm2;
		}

		@Override
		public int hashCode() {
			return Objects.hash(dumm1, dumm2);
		}

		@Override
		public boolean equals(Object other) {
			if (!(other instanceof TestUserClassBase)) {
				return false;
			}
			TestUserClassBase otherTUC = (TestUserClassBase) other;
			if (dumm1 != otherTUC.dumm1) {
				return false;
			}
			if (!dumm2.equals(otherTUC.dumm2)) {
				return false;
			}
			return true;
		}
	}

	public static class TestUserClass1 extends TestUserClassBase {
		public long dumm3;

		public TestUserClass1() {
		}

		public TestUserClass1(int dumm1, String dumm2, long dumm3) {
			super(dumm1, dumm2);
			this.dumm3 = dumm3;
		}

		@Override
		public boolean equals(Object other) {
			if (!(other instanceof TestUserClass1)) {
				return false;
			}
			TestUserClass1 otherTUC = (TestUserClass1) other;
			if (dumm1 != otherTUC.dumm1) {
				return false;
			}
			if (!dumm2.equals(otherTUC.dumm2)) {
				return false;
			}
			if (dumm3 != otherTUC.dumm3) {
				return false;
			}
			return true;
		}
	}

	public static class TestUserClass2 extends TestUserClassBase {
		public float dumm4;

		public TestUserClass2() {
		}

		public TestUserClass2(int dumm1, String dumm2, float dumm4) {
			super(dumm1, dumm2);
			this.dumm4 = dumm4;
		}

		@Override
		public boolean equals(Object other) {
			if (!(other instanceof TestUserClass2)) {
				return false;
			}
			TestUserClass2 otherTUC = (TestUserClass2) other;
			if (dumm1 != otherTUC.dumm1) {
				return false;
			}
			if (!dumm2.equals(otherTUC.dumm2)) {
				return false;
			}
			if (dumm4 != otherTUC.dumm4) {
				return false;
			}
			return true;
		}
	}
}
