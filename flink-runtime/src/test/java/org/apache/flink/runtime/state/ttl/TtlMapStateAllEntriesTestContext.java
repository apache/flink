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

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/** Test suite for collection methods of {@link TtlMapState}. */
class TtlMapStateAllEntriesTestContext extends
	TtlMapStateTestContext<Map<Integer, String>, Set<Map.Entry<Integer, String>>> {

	@Override
	void initTestValues() {
		emptyValue = Collections.emptySet();

		updateEmpty = mapOf(Tuple2.of(3, "3"), Tuple2.of(5, "5"), Tuple2.of(10, "10"));
		updateUnexpired = mapOf(Tuple2.of(12, "12"), Tuple2.of(7, "7"));
		updateExpired = mapOf(Tuple2.of(15, "15"), Tuple2.of(4, "4"));

		getUpdateEmpty = updateEmpty.entrySet();
		getUnexpired = updateUnexpired.entrySet();
		getUpdateExpired = updateExpired.entrySet();
	}

	@SafeVarargs
	private static <UK, UV> Map<UK, UV> mapOf(Tuple2<UK, UV> ... entries) {
		return Arrays.stream(entries).collect(Collectors.toMap(t -> t.f0, t -> t.f1));
	}

	@Override
	void update(Map<Integer, String> map) throws Exception {
		ttlState.putAll(map);
	}

	@Override
	Set<Map.Entry<Integer, String>> get() throws Exception {
		return StreamSupport.stream(ttlState.entries().spliterator(), false).collect(Collectors.toSet());
	}

	@Override
	Object getOriginal() throws Exception {
		return ttlState.original.entries() == null ? Collections.emptySet() : ttlState.original.entries();
	}
}
