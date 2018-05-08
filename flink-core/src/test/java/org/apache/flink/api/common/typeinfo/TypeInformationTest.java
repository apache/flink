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

package org.apache.flink.api.common.typeinfo;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link TypeInformation} class.
 */
public class TypeInformationTest {

	@Test
	public void testOfClass() {
		assertEquals(BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(String.class));
	}

	@Test
	public void testOfGenericClassForFlink() {
		try {
			TypeInformation.of(Tuple3.class);
			fail("should fail with an exception");
		}
		catch (FlinkRuntimeException e) {
			// check that the error message mentions the TypeHint
			assertNotEquals(-1, e.getMessage().indexOf("TypeHint"));
		}
	}

	@Test
	public void testOfGenericClassForGenericType() {
		assertEquals(new GenericTypeInfo<>(List.class), TypeInformation.of(List.class));
	}

	@Test
	public void testOfTypeHint() {
		assertEquals(BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(String.class));
		assertEquals(BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint<String>(){}));

		TypeInformation<Tuple3<String, Double, Boolean>> tupleInfo =
				new TupleTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO, BasicTypeInfo.BOOLEAN_TYPE_INFO);

		assertEquals(tupleInfo, TypeInformation.of(new TypeHint<Tuple3<String, Double, Boolean>>(){}));
	}
}
