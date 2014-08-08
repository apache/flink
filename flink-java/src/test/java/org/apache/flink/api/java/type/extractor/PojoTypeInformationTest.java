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


package org.apache.flink.api.java.type.extractor;

import static org.junit.Assert.assertTrue;

import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.types.TypeInformation;
import org.junit.Ignore;
import org.junit.Test;

@SuppressWarnings("unused")
public class PojoTypeInformationTest {

	static class SimplePojo {
		String str;
		Boolean Bl;
		boolean bl;
		Byte Bt;
		byte bt;
		Short Shrt;
		short shrt;
		Integer Intgr;
		int intgr;
		Long Lng;
		long lng;
		Float Flt;
		float flt;
		Double Dbl;
		double dbl;
		Character Ch;
		char ch;
		int[] primIntArray;
		Integer[] intWrapperArray;
	}

	@Ignore
	@Test
	public void testSimplePojoTypeExtraction() {
		TypeInformation<SimplePojo> type = TypeExtractor.getForClass(SimplePojo.class);
		assertTrue("Extracted type is not a Pojo type but should be.", type instanceof PojoTypeInfo);
	}

	static class NestedPojoInner {
		private String field;
	}

	static class NestedPojoOuter {
		private Integer intField;
		NestedPojoInner inner;
	}

	@Ignore
	@Test
	public void testNestedPojoTypeExtraction() {
		TypeInformation<NestedPojoOuter> type = TypeExtractor.getForClass(NestedPojoOuter.class);
		assertTrue("Extracted type is not a Pojo type but should be.", type instanceof PojoTypeInfo);
	}

	static class Recursive1Pojo {
		private Integer intField;
		Recursive2Pojo rec;
	}

	static class Recursive2Pojo {
		private String strField;
		Recursive1Pojo rec;
	}

	@Ignore
	@Test
	public void testRecursivePojoTypeExtraction() {
		// This one tests whether a recursive pojo is detected using the set of visited
		// types in the type extractor. The recursive field will be handled using the generic serializer.
		TypeInformation<Recursive1Pojo> type = TypeExtractor.getForClass(Recursive1Pojo.class);
		assertTrue("Extracted type is not a Pojo type but should be.", type instanceof PojoTypeInfo);
	}

	@Ignore
	@Test
	public void testRecursivePojoObjectTypeExtraction() {
		TypeInformation<Recursive1Pojo> type = TypeExtractor.getForObject(new Recursive1Pojo());
		assertTrue("Extracted type is not a Pojo type but should be.", type instanceof PojoTypeInfo);
	}
}
