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

package org.apache.flink.table.types.extraction;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;

import org.junit.Test;

import java.lang.reflect.Method;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link ExtractionUtils}.
 */
public class ExtractionUtilsTest {

	@Test
	public void testArbitraryArg() {
		List<Method> methods = ExtractionUtils.collectMethods(ArbitraryArgFunc.class, "eval");
		assertTrue(methods.stream().anyMatch(m -> ExtractionUtils.isInvokable(m, String[].class, int.class)));
	}

	// --------------------------------------------------------------------------------------------
	// Test functions
	// --------------------------------------------------------------------------------------------

	/**
	 * A function that accepts arbitrary parameters.
	 */
	public static class ArbitraryArgFunc extends ScalarFunction {
		public int eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object... objects) {
			return 0;
		}
	}
}
