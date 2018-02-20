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

package org.apache.flink.runtime.webmonitor.handlers;

import org.apache.flink.runtime.rest.messages.ConversionException;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link JarIdPathParameter}.
 */
public class JarIdPathParameterTest extends TestLogger {

	private JarIdPathParameter jarIdPathParameter = new JarIdPathParameter();

	@Test(expected = ConversionException.class)
	public void testJarIdWithParentDir() throws Exception {
		jarIdPathParameter.convertFromString("../../test.jar");
	}

	@Test
	public void testConvertFromString() throws Exception {
		final String expectedJarId = "test.jar";
		final String jarId = jarIdPathParameter.convertFromString(expectedJarId);
		assertEquals(expectedJarId, jarId);
	}

	@Test
	public void testConvertToString() throws Exception {
		final String expected = "test.jar";
		final String toString = jarIdPathParameter.convertToString(expected);
		assertEquals(expected, toString);
	}

}
