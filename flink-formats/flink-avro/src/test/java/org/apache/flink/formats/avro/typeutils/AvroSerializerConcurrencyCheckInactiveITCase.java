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

package org.apache.flink.formats.avro.typeutils;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * A test that validates that the concurrency checks in the Avro Serializer
 * are not hard coded to active.
 *
 * <p>The debug initialization in the AvroSerializer happens together with class
 * initialization (that makes it peak efficient), which is why this test needs to
 * run in a fresh JVM fork, and the JVM fork of this test should not be reused.
 *
 * <p><b>Important:</b> If you see this test fail and the initial settings are still
 * correct, check the assumptions above (on fresh JVM fork).
 */
public class AvroSerializerConcurrencyCheckInactiveITCase {

	// this sets the debug initialization back to its default, even if
	// by default tests modify it (implicitly via assertion loading)
	static {
		AvroSerializerDebugInitHelper.setToDebug = AvroSerializerDebugInitHelper.INITIAL_SETTING;
	}

	/**
	 * This test checks that concurrent access is not detected by default, meaning that
	 * the thread concurrency checks are off by default.
	 */
	@Test
	public void testWithNoConcurrencyCheck() throws Exception {
		boolean assertionError;
		try {
			new AvroSerializerConcurrencyTest().testConcurrentUseOfSerializer();
			assertionError = false;
		}
		catch (AssertionError e) {
			assertionError = true;
		}

		assertTrue("testConcurrentUseOfSerializer() should have failed if " +
				"concurrency checks are off by default", assertionError);
	}
}
