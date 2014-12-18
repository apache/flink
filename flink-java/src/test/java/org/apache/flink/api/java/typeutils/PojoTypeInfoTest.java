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
import org.junit.Test;

public class PojoTypeInfoTest {

	@Test
	public void testEquals() {
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
}
