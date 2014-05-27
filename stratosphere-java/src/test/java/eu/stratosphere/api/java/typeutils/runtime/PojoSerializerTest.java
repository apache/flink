/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.java.typeutils.runtime;

import java.util.Random;

import com.google.common.base.Objects;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import eu.stratosphere.api.common.typeutils.SerializerTestBase;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.java.typeutils.TypeExtractor;
import eu.stratosphere.types.TypeInformation;

/**
 * A test for the {@link eu.stratosphere.api.java.typeutils.runtime.PojoSerializer}.
 */
public class PojoSerializerTest extends SerializerTestBase<PojoSerializerTest.TestUserClass> {
	private TypeInformation<TestUserClass> type = TypeExtractor.getForClass(TestUserClass.class);

	@Override
	protected TypeSerializer<TestUserClass> createSerializer() {
		TypeSerializer<TestUserClass> serializer = type.createSerializer();
		assert(serializer instanceof PojoSerializer);
		return serializer;
	}
	
	@Override
	protected int getLength() {
		return -1;
	}
	
	@Override
	protected Class<TestUserClass> getTypeClass() {
		return TestUserClass.class;
	}
	
	@Override
	protected TestUserClass[] getTestData() {
		Random rnd = new Random(874597969123412341L);

		return new TestUserClass[] {
			new TestUserClass(rnd.nextInt(), "foo", rnd.nextDouble(), new int[] {1,2,3},
					new NestedTestUserClass(rnd.nextInt(), "foo@boo", rnd.nextDouble(), new int[] {10, 11, 12})),
			new TestUserClass(rnd.nextInt(), "bar", rnd.nextDouble(), new int[] {4,5,6},
					new NestedTestUserClass(rnd.nextInt(), "bar@bas", rnd.nextDouble(), new int[] {20, 21, 22}))
		};

	}

	// User code class for testing the serializer
	public static class TestUserClass {
		private int dumm1;
		protected String dumm2;
		public double dumm3;
		private int[] dumm4;

		private NestedTestUserClass nestedClass;

		public TestUserClass() {}

		public TestUserClass(int dumm1, String dumm2, double dumm3, int[] dumm4, NestedTestUserClass nestedClass) {
			this.dumm1 = dumm1;
			this.dumm2 = dumm2;
			this.dumm3 = dumm3;
			this.dumm4 = dumm4;
			this.nestedClass = nestedClass;
		}

		@Override
		public int hashCode() {
			return Objects.hashCode(dumm1, dumm2, dumm3, dumm4, nestedClass);
		}

		@Override
		public boolean equals(Object other) {
			if (!(other instanceof TestUserClass)) {
				return false;
			}
			TestUserClass otherTUC = (TestUserClass) other;
			if (dumm1 != otherTUC.dumm1) {
				return false;
			}
			if (!dumm2.equals(otherTUC.dumm2)) {
				return false;
			}
			if (dumm3 != otherTUC.dumm3) {
				return false;
			}
			if (dumm4.length != otherTUC.dumm4.length) {
				return false;
			}
			for (int i = 0; i < dumm4.length; i++) {
				if (dumm4[i] != otherTUC.dumm4[i]) {
					return false;
				}
			}
			if (!nestedClass.equals(otherTUC.nestedClass)) {
				return false;
			}
			return true;
		}
	}

	public static class NestedTestUserClass {
		private int dumm1;
		protected String dumm2;
		public double dumm3;
		private int[] dumm4;

		public NestedTestUserClass() {}

		public NestedTestUserClass(int dumm1, String dumm2, double dumm3, int[] dumm4) {
			this.dumm1 = dumm1;
			this.dumm2 = dumm2;
			this.dumm3 = dumm3;
			this.dumm4 = dumm4;
		}

		@Override
		public int hashCode() {
			return Objects.hashCode(dumm1, dumm2, dumm3, dumm4);
		}

		@Override
		public boolean equals(Object other) {
			if (!(other instanceof NestedTestUserClass)) {
				return false;
			}
			NestedTestUserClass otherTUC = (NestedTestUserClass) other;
			if (dumm1 != otherTUC.dumm1) {
				return false;
			}
			if (!dumm2.equals(otherTUC.dumm2)) {
				return false;
			}
			if (dumm3 != otherTUC.dumm3) {
				return false;
			}
			if (dumm4.length != otherTUC.dumm4.length) {
				return false;
			}
			for (int i = 0; i < dumm4.length; i++) {
				if (dumm4[i] != otherTUC.dumm4[i]) {
					return false;
				}
			}
			return true;
		}
	}
}	
