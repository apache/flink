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

package org.apache.flink.runtime.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Base64 Library switch from org.apache.commons.codec.binary.Base64 to java.util.Base64.
 * Test cases provided to make sure Base64 content compatible.
 */
public class Base64CompatibleTest
{
	@Test
	public void testCommonsBase64ToJavaUtilBase64() throws Exception
	{
		String inputString = "Hello, this is apache flink.";
		String encodeString =
			org.apache.commons.codec.binary.Base64.encodeBase64URLSafeString(
				inputString.getBytes("UTF-8")
			);

		String decodeString =
			new String(
				java.util.Base64.getUrlDecoder().decode(encodeString),
				"UTF-8"
			);

		assertEquals(inputString, decodeString);
	}


	/**
	 * encodeBase64URLSafeString with inputString will have char '_' in decodeString.
	 * must use RFC4648_URLSAFE to decode.
	 * @throws Exception
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testCommonsBase64ToJavaUtilBase64UsingWrongDecoder1() throws Exception
	{
		String inputString = "special base64 char <_> 00?";
		String encodeString =
			org.apache.commons.codec.binary.Base64.encodeBase64URLSafeString(
				inputString.getBytes("UTF-8")
			);

		assertTrue(encodeString.contains("_"));

		String decodeString =
			new String(
				java.util.Base64.getDecoder().decode(encodeString),
				"UTF-8"
			);

	}

	/**
	 * encodeBase64URLSafeString with inputString will have char '-' in decodeString.
	 * must use RFC4648_URLSAFE to decode.
	 * @throws Exception
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testCommonsBase64ToJavaUtilBase64UsingWrongDecoder2() throws Exception
	{
		String inputString = "special base64 char <-> 00>";
		String encodeString =
			org.apache.commons.codec.binary.Base64.encodeBase64URLSafeString(
				inputString.getBytes("UTF-8")
			);

		assertTrue(encodeString.contains("-"));

		String decodeString =
			new String(
				java.util.Base64.getDecoder().decode(encodeString),
				"UTF-8"
			);

	}
}
