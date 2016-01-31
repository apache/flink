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

import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import static org.junit.Assert.*;

public class MissingTypeInfoTest extends TestLogger {
	static final String functionName = "foobar";
	static final InvalidTypesException testException = new InvalidTypesException("Test exception.");

	@Test
	public void testMissingTypeInfoEquality() {
		MissingTypeInfo tpeInfo1 = new MissingTypeInfo(functionName, testException);
		MissingTypeInfo tpeInfo2 = new MissingTypeInfo(functionName, testException);

		assertEquals(tpeInfo1, tpeInfo2);
		assertEquals(tpeInfo1.hashCode(), tpeInfo2.hashCode());
	}

	@Test
	public void testMissingTypeInfoInequality() {
		MissingTypeInfo tpeInfo1 = new MissingTypeInfo(functionName, testException);
		MissingTypeInfo tpeInfo2 = new MissingTypeInfo("alt" + functionName, testException);

		assertNotEquals(tpeInfo1, tpeInfo2);
	}
}
