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

package org.apache.flink.runtime.state.ttl;

/** Test suite for per element methods of {@link TtlMapState}. */
public class TtlMapStatePerElementTest extends TtlMapStateTestBase<String, String> {
	private static final int TEST_KEY = 1;
	private static final String TEST_VAL1 = "test value1";
	private static final String TEST_VAL2 = "test value2";
	private static final String TEST_VAL3 = "test value3";

	@Override
	void initTestValues() {
		updater = v -> ttlState.put(TEST_KEY, v);
		getter = () -> ttlState.get(TEST_KEY);
		originalGetter = () -> ttlState.original.get(TEST_KEY);

		updateEmpty = TEST_VAL1;
		updateUnexpired = TEST_VAL2;
		updateExpired = TEST_VAL3;

		getUpdateEmpty = TEST_VAL1;
		getUnexpired = TEST_VAL2;
		getUpdateExpired = TEST_VAL3;
	}
}
